// Package conformance is a conformance suite for xDS libraries: give a library desired config, and check a real Envoy
// gets there safely.
//
// It tests the layer that serves config to Envoy (ordering, ACKs and NACKs, subscriptions, reconnects), not how a
// control plane decides what config to serve. For that, write your own scenarios with package xdstest.
package conformance

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	ondemandv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/on_demand/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcmv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	xdstest "github.com/JonathanO/envoy-xds-testing-go"
	"github.com/JonathanO/envoy-xds-testing-go/match"
)

// ControlPlane is the library under test.
type ControlPlane interface {
	// Register registers the library's ADS service, serving SotW or delta, whichever the test uses. It may be called
	// again with a new server after a reconnect.
	Register(s *grpc.Server)
	// SetConfig makes these resources the whole desired config for every Envoy, replacing what was there before:
	// Clusters, ClusterLoadAssignments, Listeners, RouteConfigurations, ScopedRouteConfigurations and VirtualHosts (for
	// VHDS, named <route config>/<host>).
	SetConfig(resources []proto.Message)
}

// IdleReporter is optional for a ControlPlane. Without it the suite waits for every response to be answered and a
// quiet period.
type IdleReporter interface {
	// Idle reports whether the control plane has nothing in flight or waiting to be sent on its latest stream.
	Idle() bool
}

// Resyncer is optional for a ControlPlane.
type Resyncer interface {
	// Resync makes Envoy drop its stream and sync from scratch, e.g. to get a fixed config to an Envoy that NACKed.
	Resync()
}

// Protocol is the ADS flavour Envoy uses.
type Protocol string

const (
	Sotw  Protocol = "sotw"
	Delta Protocol = "delta"
)

// EnvoyImages are the Envoy versions conformance means: the oldest Envoy release upstream still supports, and the newest.
var EnvoyImages = []string{"envoyproxy/envoy:v1.36-latest", "envoyproxy/envoy:v1.40-latest"}

// Options configure a run.
type Options struct {
	// New returns a fresh control plane for each test.
	New func(t *testing.T) ControlPlane
	// Protocols the library supports, by default both. Conformance results should say if it's not both.
	Protocols []Protocol
	// EnvoyImages to run on, tagged v1.NN..., by default EnvoyImages. Conformance results should say if it's not those.
	EnvoyImages []string
	// Skip records known deviations, with a reason. A key is a test's path, or part of it: any of protocol (sotw,
	// delta), Envoy version (v1.36), mode (normal, rate-limited) and scenario (ChangesWhileDisconnected), joined by "/".
	// "sotw/ChangesWhileDisconnected" skips that scenario on SotW everywhere.
	Skip map[string]string
	// Serial runs one test at a time, e.g. if the control planes New returns share state. Otherwise tests run in
	// parallel, each with its own Envoy, as many at once as go test's -parallel flag allows (by default GOMAXPROCS).
	Serial bool
}

// variant is one place in the matrix.
type variant struct {
	protocol    Protocol
	image       string
	rateLimited bool // rate_limit_settings on Envoy's requests: it queues them, and delta sends ACKs ahead of queued subscriptions
	noCDS       bool // no cds_config in Envoy's bootstrap: it subscribes to no clusters, only asking for them on demand
}

const (
	listenerPort        = 10000
	dynamicListenerPort = 10001        // the listener AddsAndRemovesListener adds
	routes              = "all_routes" // Envoy's static listener asks for this
	onDemandFilter      = "envoy.filters.http.on_demand"
)

var (
	//go:embed envoy-sotw.yaml
	sotwBootstrap []byte
	//go:embed envoy-delta.yaml
	deltaBootstrap []byte
)

// Run runs every scenario as subtests of t: on each protocol, on each of the Envoy images, normally and with Envoy's requests
// rate limited, e.g. t/sotw/v1.36/rate-limited/StartsEmpty. Each test's control plane listens on a free port, so tests
// run in parallel, unless Options.Serial.
func Run(t *testing.T, o Options) {
	tests := []struct {
		name          string
		run           func(h *harness)
		protocols     []Protocol // the protocols Envoy supports it on, if not both
		noCDS         bool       // Envoy subscribes to no clusters, only asking for them on demand
		minEnvoyMinor int        // the oldest Envoy minor version that supports it, if not all do
		why           string     // why older ones don't
	}{
		{name: "StartsEmpty", run: startsEmpty},
		{name: "AddsAndRemovesServices", run: addsAndRemovesServices},
		{name: "ChangesEndpoints", run: changesEndpoints},
		{name: "MovesRouteToNewCluster", run: movesRouteToNewCluster},
		{name: "ContainsRejectedConfig", run: containsRejectedConfig},
		{name: "Reconnects", run: reconnects},
		{name: "ChangesWhileDisconnected", run: changesWhileDisconnected},
		{name: "AddsAndRemovesListener", run: addsAndRemovesListener},
		{name: "Churns", run: churns},
		{name: "ControlPlaneRestarts", run: controlPlaneRestarts},
		{name: "ScopedRoutes", run: scopedRoutes},
		{name: "OnDemandScopes", run: onDemandScopes},
		// Envoy only does on-demand clusters and virtual hosts over delta.
		{name: "OnDemandClusters", run: onDemandClusters, protocols: []Protocol{Delta}, noCDS: true},
		{name: "OnDemandVirtualHosts", run: onDemandVirtualHosts, protocols: []Protocol{Delta}, minEnvoyMinor: 40,
			why: "Envoy 1.36 only takes VHDS from a DELTA_GRPC config source of its own, not over ADS"},
	}
	protocols := o.Protocols
	if len(protocols) == 0 {
		protocols = []Protocol{Sotw, Delta}
	}
	images := o.EnvoyImages
	if len(images) == 0 {
		images = EnvoyImages
	}
	// At every level: a test that isn't parallel holds up its siblings until its own subtests finish.
	parallel := func(t *testing.T) {
		if !o.Serial {
			t.Parallel()
		}
	}
	for _, protocol := range protocols {
		t.Run(string(protocol), func(t *testing.T) {
			parallel(t)
			for _, image := range images {
				version := strings.TrimSuffix(image[strings.LastIndex(image, ":")+1:], "-latest")
				t.Run(version, func(t *testing.T) {
					parallel(t)
					for _, rateLimited := range []bool{false, true} {
						mode := "normal"
						if rateLimited {
							mode = "rate-limited"
						}
						t.Run(mode, func(t *testing.T) {
							parallel(t)
							for _, test := range tests {
								if test.protocols != nil && !slices.Contains(test.protocols, protocol) {
									continue
								}
								t.Run(test.name, func(t *testing.T) {
									parallel(t)
									if reason, ok := deviation(o.Skip, string(protocol), version, mode, test.name); ok {
										t.Skip(reason)
									}
									if minor, _ := strconv.Atoi(version[strings.Index(version, ".")+1:]); minor < test.minEnvoyMinor {
										t.Skip(test.why)
									}
									test.run(newHarness(t, o, variant{protocol: protocol, image: image, rateLimited: rateLimited, noCDS: test.noCDS}))
								})
							}
						})
					}
				})
			}
		})
	}
}

// deviation finds a Skip entry whose path parts all match this test's.
func deviation(skip map[string]string, path ...string) (string, bool) {
	for key, reason := range skip {
		if !slices.ContainsFunc(strings.Split(key, "/"), func(part string) bool { return !slices.Contains(path, part) }) {
			return reason, true
		}
	}
	return "", false
}

// StartsEmpty: with no config at first, Envoy gets an answer for every type, then takes the first service.
func startsEmpty(h *harness) {
	h.cp.SetConfig(nil)
	h.start()
	c := h.synced(1)
	h.must(h.checkpoint.Sane(h.ctx))

	h.change(c, services("foo"))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertStatus("foo", 200)
}

// AddsAndRemovesServices: a route, its cluster and its endpoints added in one change, then removed in one change.
func addsAndRemovesServices(h *harness) {
	h.cp.SetConfig(services("foo"))
	h.start()
	c := h.synced(1)

	// empty has no endpoints: it still needs an answer, an empty assignment, or Envoy leaves it warming
	h.change(c, withEmptyCluster(services("foo", "bar")))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertStatus("bar", 200)
	h.assertStatus("empty", 503)

	h.change(c, services("foo"))
	// Envoy drops its EDS subscription for bar, which needs no response.
	h.must(xdstest.WaitFor(10*time.Second, func() error { return h.checks(c).UnsubscribedFrom(xdstest.EDS, "bar") }))
	h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertStatus("bar", 404)
	h.assertStatus("foo", 200)
}

// ChangesEndpoints: only a service's endpoints change, the commonest change in a mesh: one replaced and one added, then
// one removed. Envoy must route to the new endpoints, without rebuilding the cluster.
func changesEndpoints(h *harness) {
	h.cp.SetConfig(services("foo"))
	h.start()
	c := h.synced(1)
	h.assertRoutedToEndpoints("foo", "127.0.0.1")
	stats, err := h.envoy.Stats(h.ctx)
	h.must(err)
	modified := stats["cluster_manager.cluster_modified"]

	h.change(c, withEndpoints(services("foo"), "foo", "127.0.0.2", "127.0.0.3"))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertRoutedToEndpoints("foo", "127.0.0.2", "127.0.0.3")

	h.change(c, withEndpoints(services("foo"), "foo", "127.0.0.3"))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertRoutedToEndpoints("foo", "127.0.0.3")
	stats, err = h.envoy.Stats(h.ctx)
	h.must(err)
	if got := stats["cluster_manager.cluster_modified"]; got != modified {
		h.t.Fatalf("cluster_manager.cluster_modified went from %d to %d; only endpoints changed", modified, got)
	}
}

// MovesRouteToNewCluster: a route moved to a new cluster, and its old cluster removed, in one change.
func movesRouteToNewCluster(h *harness) {
	h.cp.SetConfig(service("foo", "foo-v1"))
	h.start()
	c := h.synced(1)
	h.must(h.envoy.RoutedVia(h.ctx, listenerPort, "foo", "foo-v1"))

	h.change(c, service("foo", "foo-v2"))
	h.must(h.checkpoint.Sane(h.ctx))
	h.must(h.envoy.RoutedVia(h.ctx, listenerPort, "foo", "foo-v2"))
}

// ContainsRejectedConfig: Envoy rejects a bad cluster, and the library contains it. It doesn't send the bad cluster
// again, not even as part of a later change, and doesn't drop Envoy into a reconnect loop: retrying is spam, and Envoy
// resets its reconnect backoff whenever it gets a response. Envoy keeps serving its last good config. Whether a later
// fix gets through on the same stream is up to the library, but if it's a Resyncer, the fix must get through after
// a Resync.
func containsRejectedConfig(h *harness) {
	h.cp.SetConfig(services("foo"))
	h.start()
	h.synced(1)

	h.cp.SetConfig(withBadCluster(services("foo")))
	h.must(xdstest.WaitFor(10*time.Second, h.envoyRejectedSomething))
	afterNack, err := xdstest.TakeCheckpoint(h.ctx, h.envoy, h.rec.Conversations()[0])
	h.must(err)

	contained := func() error { return h.contained(afterNack) }
	h.cp.SetConfig(withBadCluster(services("foo", "bar"))) // still bad
	h.must(holdsFor(2*time.Second, contained))
	h.cp.SetConfig(services("foo", "bar")) // fixed
	h.must(holdsFor(2*time.Second, contained))
	h.assertStatus("foo", 200)
	h.must(afterNack.EnvoyHealthy(h.ctx))

	if r, ok := h.cp.(Resyncer); ok {
		r.Resync()
		h.synced(2)
		h.must(h.checkpoint.SaneAfterReconnect(h.ctx, nil))
		h.assertStatus("bar", 200)
	}
}

// Reconnects: Envoy reconnects after a service was removed while it was away. Traffic keeps flowing and updates still
// work.
func reconnects(h *harness) {
	h.cp.SetConfig(services("foo", "bar"))
	h.start()
	h.synced(1)
	h.assertStatus("bar", 200)

	h.stopServer()
	h.cp.SetConfig(services("foo"))
	h.start()
	c := h.synced(2) // ignores Envoy's warnings about the disconnect
	h.assertRateLimitedIfAsked()
	h.must(h.checkpoint.SaneAfterReconnect(h.ctx, nil))
	h.assertStatus("foo", 200)
	h.assertStatus("bar", 404)

	h.change(c, services("foo", "baz"))
	h.must(h.checkpoint.SaneAfterReconnect(h.ctx, nil))
	h.assertStatus("baz", 200)
}

// ChangesWhileDisconnected: a route moved to a new cluster, and the old cluster removed, while Envoy was away. Envoy
// keeps its config across the disconnect, so once it reconnects the move must be make before break from there.
func changesWhileDisconnected(h *harness) {
	h.cp.SetConfig(service("foo", "foo-v1"))
	h.start()
	before := h.synced(1)
	h.must(h.envoy.RoutedVia(h.ctx, listenerPort, "foo", "foo-v1"))

	h.stopServer()
	h.cp.SetConfig(service("foo", "foo-v2"))
	h.start()
	h.synced(2)
	h.assertRateLimitedIfAsked()
	h.must(h.checkpoint.SaneAfterReconnect(h.ctx, before))
	h.must(h.envoy.RoutedVia(h.ctx, listenerPort, "foo", "foo-v2"))
	// and foo-v1 goes, once nothing uses it
	h.must(xdstest.WaitFor(10*time.Second, func() error {
		return match.InOrder(h.rec.Conversations()[1], match.AckOf(match.Response(xdstest.CDS, match.Removing("foo-v1"))))
	}))
}

// AddsAndRemovesListener: a listener, its route config, cluster and endpoints added in one change, then removed in one
// change. The listener mustn't be left warming, and a route config mustn't go while a listener Envoy has still uses it.
func addsAndRemovesListener(h *harness) {
	h.cp.SetConfig(services("foo"))
	h.start()
	c := h.synced(1)

	h.change(c, withListener(services("foo")))
	h.must(h.checkpoint.Sane(h.ctx))
	h.must(h.envoy.RoutedVia(h.ctx, dynamicListenerPort, "dynamic", "dynamic"))

	h.change(c, services("foo"))
	h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
	h.must(h.checkpoint.Sane(h.ctx))
	h.must(xdstest.WaitFor(10*time.Second, func() error { // just the static listener
		stats, err := h.envoy.Stats(h.ctx)
		if err == nil && stats["listener_manager.total_listeners_active"] != 1 {
			err = fmt.Errorf("%d active listeners, want 1", stats["listener_manager.total_listeners_active"])
		}
		return err
	}))
	h.assertStatus("foo", 200)
}

// Churns: twenty changes in quick succession, each moving foo to a new cluster. Envoy ends on the last, safely.
func churns(h *harness) {
	h.cp.SetConfig(service("foo", "foo-v0"))
	h.start()
	c := h.synced(1)

	for i := 1; i <= 20; i++ {
		h.cp.SetConfig(service("foo", fmt.Sprintf("foo-v%d", i)))
		time.Sleep(25 * time.Millisecond) // so some changes arrive while others are on their way
	}
	h.must(xdstest.WaitFor(60*time.Second, xdstest.SettledAfter(c, h.checkpoint.Message, h.settled(c))))
	h.must(h.checkpoint.Sane(h.ctx))
	h.must(h.envoy.RoutedVia(h.ctx, listenerPort, "foo", "foo-v20"))
	h.must(xdstest.WaitFor(60*time.Second, func() error {
		dump, err := h.envoy.ConfigDump(h.ctx, "?resource=dynamic_active_clusters")
		if err != nil {
			return err
		}
		names, err := match.ActiveClusterNames(dump)
		if err == nil && !slices.Equal(names, []string{"foo-v20"}) {
			err = fmt.Errorf("active clusters %v, want just foo-v20: no stale clusters left", names)
		}
		return err
	}))
}

// ControlPlaneRestarts: Envoy reconnects to a control plane that restarted, with different config. Envoy kept its
// config, which the new control plane never sent, so it can only go by what Envoy tells it. Traffic keeps flowing, and
// bar goes once nothing uses it.
func controlPlaneRestarts(h *harness) {
	h.cp.SetConfig(services("foo", "bar"))
	h.start()
	before := h.synced(1)
	h.assertStatus("bar", 200)

	h.stopServer()
	h.cp = h.newCP(h.t)
	h.cp.SetConfig(services("foo", "baz"))
	h.start()
	h.synced(2)
	h.assertRateLimitedIfAsked()
	h.must(h.checkpoint.SaneAfterReconnect(h.ctx, before))
	h.assertStatus("foo", 200)
	h.assertStatus("baz", 200)
	h.assertStatus("bar", 404)
	h.must(xdstest.WaitFor(10*time.Second, func() error {
		return match.InOrder(h.rec.Conversations()[1], match.AckOf(match.Response(xdstest.CDS, match.Removing("bar"))))
	}))
}

// ScopedRoutes: a listener picks a scope (SRDS) by the Host header, each naming its own route config. A scope, its
// route config and cluster added in one change, then removed in one change: the scope must go before its route config,
// and the route config before its cluster.
func scopedRoutes(h *harness) {
	h.cp.SetConfig(withScopes(services("foo"), false, "foo"))
	h.start()
	c := h.synced(1)
	h.must(h.envoy.RoutedVia(h.ctx, dynamicListenerPort, "foo", "foo"))

	h.change(c, withScopes(services("foo", "bar"), false, "foo", "bar"))
	h.must(h.checkpoint.Sane(h.ctx))
	h.must(h.envoy.RoutedVia(h.ctx, dynamicListenerPort, "bar", "bar"))

	h.change(c, withScopes(services("foo"), false, "foo"))
	h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertStatusOn(dynamicListenerPort, "bar", 404)
	h.must(h.envoy.RoutedVia(h.ctx, dynamicListenerPort, "foo", "foo"))
}

// OnDemandScopes: as ScopedRoutes, but Envoy asks for a scope's route config when the first request for it arrives.
func onDemandScopes(h *harness) {
	h.cp.SetConfig(withScopes(services("foo", "bar"), true, "foo", "bar"))
	h.start()
	c := h.synced(1)

	h.assertFirstRequestRoutedVia(c, "foo", "foo")
	h.assertFirstRequestRoutedVia(c, "bar", "bar")
}

// OnDemandClusters (ODCDS): Envoy subscribes to no clusters as it starts, and asks for one, then its endpoints, when
// the first request routed to it arrives.
func onDemandClusters(h *harness) {
	h.cp.SetConfig(clustersOnDemand("foo", "bar"))
	h.start()
	c := h.synced(1)

	h.assertFirstRequestRoutedVia(c, "foo", "foo")
	h.assertFirstRequestRoutedVia(c, "bar", "bar")
}

// OnDemandVirtualHosts (VHDS): Envoy's route config has none of its own. Envoy holds the route config until it gets
// virtual hosts under the route config's name, so a library sends some (here, all) to start, and the rest when Envoy
// asks for one on the first request for its host. Either way, requests for each host get routed.
func onDemandVirtualHosts(h *harness) {
	h.cp.SetConfig(withVirtualHostsOnDemand(services("foo", "bar"), "foo", "bar"))
	h.start()
	c := h.synced(1)

	for _, name := range []string{"foo", "bar"} {
		// Unlike a held on-demand request, one for a virtual host sent ahead can race Envoy's workers getting it, so
		// RoutedVia retries.
		h.must(h.envoy.RoutedVia(h.ctx, dynamicListenerPort, name, name))
		h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
		h.must(h.checkpoint.Sane(h.ctx))
	}
}

type harness struct {
	t          *testing.T
	ctx        context.Context
	v          variant
	cp         ControlPlane
	newCP      func(*testing.T) ControlPlane
	rec        *xdstest.Recorder
	server     *grpc.Server
	adsPort    int // the control plane's port, picked on the first start; 0 until then
	envoy      *xdstest.Envoy
	checkpoint *xdstest.Checkpoint
}

func newHarness(t *testing.T, o Options, v variant) *harness {
	h := &harness{t: t, ctx: context.Background(), v: v, cp: o.New(t), newCP: o.New, rec: &xdstest.Recorder{}}
	t.Cleanup(func() {
		if t.Failed() && h.envoy != nil {
			log := h.envoy.LogSince(0)
			t.Logf("last Envoy log lines:\n%s", strings.Join(log[max(0, len(log)-40):], "\n"))
		}
		if h.envoy != nil {
			_ = h.envoy.Terminate(context.Background())
		}
		h.stopServer()
	})
	return h
}

func (h *harness) must(err error) {
	h.t.Helper()
	if err != nil {
		h.t.Fatal(err)
	}
}

// start starts the control plane, then Envoy if it isn't running, so Envoy doesn't back off past its initial fetch
// timeout.
func (h *harness) start() {
	h.t.Helper()
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(h.adsPort))
	h.must(err)
	// A free port, kept for restarts: Envoy's bootstrap names it.
	h.adsPort = lis.Addr().(*net.TCPAddr).Port
	h.server = grpc.NewServer(grpc.StreamInterceptor(h.rec.Intercept))
	h.cp.Register(h.server)
	go func() { _ = h.server.Serve(lis) }()
	if h.envoy == nil {
		bootstrap := sotwBootstrap
		if h.v.protocol == Delta {
			bootstrap = deltaBootstrap
		}
		if h.v.rateLimited {
			bootstrap = rateLimited(h.t, bootstrap)
		}
		if h.v.noCDS {
			bootstrap = withoutCDS(h.t, bootstrap)
		}
		h.envoy, err = xdstest.StartEnvoy(h.ctx, xdstest.EnvoyConfig{
			Image: h.v.image, Bootstrap: bootstrap, ADSPort: h.adsPort, ListenerPorts: []int{listenerPort, dynamicListenerPort}, Args: []string{"-l", "debug"},
		})
		h.must(err)
	}
}

// rateLimited adds rate_limit_settings to the ADS config: one request at a time, ten a second, enough to queue Envoy's
// reconnect burst behind the answers to it.
func rateLimited(t *testing.T, bootstrap []byte) []byte {
	const at = "    transport_api_version: V3\n    set_node_on_first_message_only"
	limited := strings.Replace(string(bootstrap), at,
		"    transport_api_version: V3\n    rate_limit_settings:\n      max_tokens: 1\n      fill_rate: 10\n    set_node_on_first_message_only", 1)
	if limited == string(bootstrap) {
		t.Fatal("couldn't add rate_limit_settings to Envoy's config")
	}
	return []byte(limited)
}

// withoutCDS removes cds_config from Envoy's config, so it subscribes to no clusters, only asking for them on demand.
func withoutCDS(t *testing.T, bootstrap []byte) []byte {
	const cds = "  cds_config:\n    ads: { }\n    resource_api_version: V3\n"
	if !strings.Contains(string(bootstrap), cds) {
		t.Fatal("couldn't remove cds_config from Envoy's config")
	}
	return []byte(strings.Replace(string(bootstrap), cds, "", 1))
}

// assertRateLimitedIfAsked checks a rate limited Envoy's reconnect burst hit the limit, or the option isn't testing
// anything.
func (h *harness) assertRateLimitedIfAsked() {
	h.t.Helper()
	if !h.v.rateLimited {
		return
	}
	stats, err := h.envoy.Stats(h.ctx)
	h.must(err)
	if stats["control_plane.rate_limit_enforced"] == 0 {
		h.t.Fatal("rate limited, but Envoy never hit the rate limit")
	}
}

func (h *harness) stopServer() {
	if h.server != nil {
		h.server.Stop()
		h.server = nil
	}
}

// synced waits for Envoy's nth stream to sync. Every type it asked for must be answered well inside its 15s initial
// fetch timeout. Later checks only look at what happens from here on.
func (h *harness) synced(nth int) *xdstest.Conversation {
	h.t.Helper()
	h.must(xdstest.WaitFor(60*time.Second, func() error {
		if n := len(h.rec.Conversations()); n < nth {
			return fmt.Errorf("%d streams, waiting for %d", n, nth)
		}
		return nil
	}))
	c := h.rec.Conversations()[nth-1]
	types := []string{xdstest.CDS, xdstest.LDS, xdstest.RDS} // what Envoy asks for as it starts
	if h.v.noCDS {
		types = types[1:]
	}
	h.must(xdstest.WaitFor(10*time.Second, func() error {
		return errors.Join(h.checks(c).HadConversationsWith(types...), h.checks(c).AnsweredEveryType())
	}))
	// A warming listener isn't listening yet, so a first request would be reset. Its last subscription, e.g. SRDS, can
	// still be queued in Envoy when everything sent so far is ACKed and the control plane looks idle.
	h.must(xdstest.WaitFor(10*time.Second, func() error {
		stats, err := h.envoy.Stats(h.ctx)
		if err != nil {
			return err
		}
		if n := stats["listener_manager.total_listeners_warming"]; n != 0 {
			return fmt.Errorf("%d listeners still warming", n)
		}
		return nil
	}))
	h.must(xdstest.WaitFor(30*time.Second, h.settled(c)))
	cp, err := xdstest.TakeCheckpoint(h.ctx, h.envoy, c)
	h.must(err)
	h.checkpoint = cp
	return c
}

func (h *harness) change(c *xdstest.Conversation, config []proto.Message) {
	h.t.Helper()
	mark := c.Size()
	h.cp.SetConfig(config)
	h.must(xdstest.WaitFor(30*time.Second, xdstest.SettledAfter(c, mark, h.settled(c))))
}

func (h *harness) settled(c *xdstest.Conversation) func() error {
	if idle, ok := h.cp.(IdleReporter); ok {
		return xdstest.SettledIdle(c, idle.Idle)
	}
	return xdstest.Settled(c, xdstest.DefaultQuietPeriod)
}

func (h *harness) assertStatus(virtualHost string, want int) {
	h.t.Helper()
	h.assertStatusOn(listenerPort, virtualHost, want)
}

func (h *harness) assertStatusOn(port int, virtualHost string, want int) {
	h.t.Helper()
	// Envoy ACKs once its main thread has the config; workers get it a moment later.
	h.must(xdstest.WaitFor(5*time.Second, func() error {
		got, err := h.envoy.HTTPStatus(h.ctx, port, virtualHost)
		if err == nil && got != want {
			err = fmt.Errorf("%s: status %d, want %d", virtualHost, got, want)
		}
		return err
	}))
}

// assertRoutedToEndpoints checks the cluster of this name has just these endpoints, and a request for the virtual host
// of this name went to one of them: its rq_total went up. It retries while Envoy's workers catch up.
func (h *harness) assertRoutedToEndpoints(name string, addresses ...string) {
	h.t.Helper()
	var want []string
	for _, a := range addresses {
		want = append(want, fmt.Sprintf("%s::%s:%d", name, a, xdstest.AdminPort))
	}
	slices.Sort(want)
	h.must(xdstest.WaitFor(5*time.Second, func() error {
		before, err := h.envoy.Clusters(h.ctx)
		if err != nil {
			return err
		}
		var hosts []string
		for k := range before {
			if host, ok := strings.CutSuffix(k, "::rq_total"); ok && strings.HasPrefix(k, name+"::") {
				hosts = append(hosts, host)
			}
		}
		slices.Sort(hosts)
		if !slices.Equal(hosts, want) {
			return fmt.Errorf("%s's endpoints %v, want %v", name, hosts, want)
		}
		status, err := h.envoy.HTTPStatus(h.ctx, listenerPort, name)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("%s: status %d, want 200", name, status)
		}
		after, err := h.envoy.Clusters(h.ctx)
		if err != nil {
			return err
		}
		for _, host := range want {
			if after[host+"::rq_total"] > before[host+"::rq_total"] {
				return nil
			}
		}
		return fmt.Errorf("%s: request didn't go to any of %v", name, want)
	}))
}

// assertFirstRequestRoutedVia: the first request for something Envoy gets on demand is held until it arrives, so it
// succeeds first time, with no retrying. Then everything Envoy was sent for it must have been make before break.
func (h *harness) assertFirstRequestRoutedVia(c *xdstest.Conversation, virtualHost, cluster string) {
	h.t.Helper()
	// Nothing listens until Envoy starts its workers, which it does after its first listeners have warmed.
	h.must(xdstest.WaitFor(10*time.Second, func() error {
		stats, err := h.envoy.Stats(h.ctx)
		if err != nil {
			return err
		}
		if stats["listener_manager.workers_started"] != 1 {
			return errors.New("Envoy hasn't started its workers")
		}
		return nil
	}))
	status, err := h.envoy.HTTPStatus(h.ctx, dynamicListenerPort, virtualHost)
	h.must(err)
	if status != 200 {
		h.t.Fatalf("first request for %s: status %d, want 200", virtualHost, status)
	}
	h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
	h.must(h.checkpoint.Sane(h.ctx))
	h.must(h.envoy.RoutedVia(h.ctx, dynamicListenerPort, virtualHost, cluster))
}

// contained checks that there have been no rejections since the first NACK, and no reconnects.
func (h *harness) contained(afterNack *xdstest.Checkpoint) error {
	if n := len(h.rec.Conversations()); n != 1 {
		return fmt.Errorf("%d streams; dropping Envoy after a NACK makes it reconnect in a loop", n)
	}
	stats, err := h.envoy.Stats(h.ctx)
	if err != nil {
		return err
	}
	for name, value := range stats {
		if strings.HasSuffix(name, ".update_rejected") && value != afterNack.Stats[name] {
			return fmt.Errorf("%s rose by %d since the first NACK; the bad cluster was sent again", name, value-afterNack.Stats[name])
		}
	}
	return nil
}

// holdsFor fails as soon as check does, or passes once it has held for d.
func holdsFor(d time.Duration, check func() error) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if err := check(); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (h *harness) envoyRejectedSomething() error {
	for _, c := range h.rec.Conversations() {
		for _, m := range c.Messages() {
			switch m := m.(type) {
			case *discoveryv3.DiscoveryRequest:
				if m.GetErrorDetail() != nil {
					return nil
				}
			case *discoveryv3.DeltaDiscoveryRequest:
				if m.GetErrorDetail() != nil {
					return nil
				}
			}
		}
	}
	return errors.New("envoy hasn't rejected anything")
}

func (h *harness) checks(c *xdstest.Conversation) xdstest.Checks {
	if h.v.protocol == Delta {
		return xdstest.Delta(c)
	}
	return xdstest.Sotw(c)
}

// services is one service per name: a cluster and endpoints of that name, and a virtual host of that name routing
// to it.
func services(names ...string) []proto.Message {
	var config []proto.Message
	rc := &routev3.RouteConfiguration{Name: routes}
	for _, name := range names {
		config = append(config, cluster(name), endpoints(name))
		rc.VirtualHosts = append(rc.VirtualHosts, virtualHost(name, name))
	}
	return append(config, rc)
}

func withBadCluster(config []proto.Message) []proto.Message {
	bad := cluster("bad")
	bad.ConnectTimeout = &durationpb.Duration{Seconds: -1}
	return append(append([]proto.Message(nil), config...), bad)
}

// withEmptyCluster adds cluster empty, with no endpoints, and a virtual host empty routing to it.
func withEmptyCluster(config []proto.Message) []proto.Message {
	var withEmpty []proto.Message
	for _, m := range config {
		if r, ok := m.(*routev3.RouteConfiguration); ok && r.GetName() == routes {
			r = proto.Clone(r).(*routev3.RouteConfiguration)
			r.VirtualHosts = append(r.VirtualHosts, virtualHost("empty", "empty"))
			m = r
		}
		withEmpty = append(withEmpty, m)
	}
	return append(withEmpty, cluster("empty"), &endpointv3.ClusterLoadAssignment{ClusterName: "empty"})
}

// withListener adds listener dynamic on port 10001, getting route config dynamic by RDS, whose virtual host dynamic
// routes to cluster dynamic.
func withListener(config []proto.Message) []proto.Message {
	return append(append([]proto.Message(nil), config...),
		listener("dynamic", &hcmv3.HttpConnectionManager{
			StatPrefix:     "dynamic",
			RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{Rds: &hcmv3.Rds{RouteConfigName: "dynamic", ConfigSource: ads()}},
			HttpFilters:    []*hcmv3.HttpFilter{httpFilter("envoy.filters.http.router", &routerv3.Router{})},
		}),
		&routev3.RouteConfiguration{Name: "dynamic", VirtualHosts: []*routev3.VirtualHost{virtualHost("dynamic", "dynamic")}},
		cluster("dynamic"), endpoints("dynamic"))
}

// withScopes adds listener scoped on port 10001, which picks a scope by the Host header, and for each name a scope of
// that name whose route config scoped-name routes to the cluster of that name. On demand, Envoy only fetches a scope's
// route config once a request needs it, held by the on-demand filter.
func withScopes(config []proto.Message, onDemand bool, names ...string) []proto.Message {
	var filters []*hcmv3.HttpFilter
	if onDemand {
		filters = append(filters, httpFilter(onDemandFilter, &ondemandv3.OnDemand{}))
	}
	hcm := &hcmv3.HttpConnectionManager{
		StatPrefix: "scoped",
		RouteSpecifier: &hcmv3.HttpConnectionManager_ScopedRoutes{ScopedRoutes: &hcmv3.ScopedRoutes{
			Name: "scopes",
			ScopeKeyBuilder: &hcmv3.ScopedRoutes_ScopeKeyBuilder{Fragments: []*hcmv3.ScopedRoutes_ScopeKeyBuilder_FragmentBuilder{{
				Type: &hcmv3.ScopedRoutes_ScopeKeyBuilder_FragmentBuilder_HeaderValueExtractor_{
					HeaderValueExtractor: &hcmv3.ScopedRoutes_ScopeKeyBuilder_FragmentBuilder_HeaderValueExtractor{
						Name: ":authority", ElementSeparator: ":",
						ExtractType: &hcmv3.ScopedRoutes_ScopeKeyBuilder_FragmentBuilder_HeaderValueExtractor_Index{Index: 0},
					},
				},
			}}},
			RdsConfigSource: ads(),
			ConfigSpecifier: &hcmv3.ScopedRoutes_ScopedRds{ScopedRds: &hcmv3.ScopedRds{ScopedRdsConfigSource: ads()}},
		}},
		HttpFilters: append(filters, httpFilter("envoy.filters.http.router", &routerv3.Router{})),
	}
	scoped := append(append([]proto.Message(nil), config...), listener("scoped", hcm))
	for _, name := range names {
		scoped = append(scoped,
			&routev3.ScopedRouteConfiguration{Name: name, RouteConfigurationName: "scoped-" + name, OnDemand: onDemand, Key: &routev3.ScopedRouteConfiguration_Key{
				Fragments: []*routev3.ScopedRouteConfiguration_Key_Fragment{{Type: &routev3.ScopedRouteConfiguration_Key_Fragment_StringKey{StringKey: name}}},
			}},
			&routev3.RouteConfiguration{Name: "scoped-" + name, VirtualHosts: []*routev3.VirtualHost{virtualHost(name, name)}})
	}
	return scoped
}

// clustersOnDemand is listener on-demand on port 10001, whose route config on-demand routes each name to the cluster of
// that name, plus those clusters and their endpoints, for an Envoy that only gets clusters when a request needs them.
// The static listener's route config is empty.
func clustersOnDemand(names ...string) []proto.Message {
	odcds := &ondemandv3.OnDemand{Odcds: &ondemandv3.OnDemandCds{Source: ads(), Timeout: durationpb.New(5 * time.Second)}}
	config := []proto.Message{&routev3.RouteConfiguration{Name: routes}, listener("on-demand", &hcmv3.HttpConnectionManager{
		StatPrefix:     "on-demand",
		RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{Rds: &hcmv3.Rds{RouteConfigName: "on-demand", ConfigSource: ads()}},
		HttpFilters:    []*hcmv3.HttpFilter{httpFilter(onDemandFilter, odcds), httpFilter("envoy.filters.http.router", &routerv3.Router{})},
	})}
	rc := &routev3.RouteConfiguration{Name: "on-demand"}
	for _, name := range names {
		config = append(config, cluster(name), endpoints(name))
		rc.VirtualHosts = append(rc.VirtualHosts, virtualHost(name, name))
	}
	return append(config, rc)
}

// withVirtualHostsOnDemand adds listener vhds on port 10001, whose route config vhds has no virtual hosts of its own,
// and for each name a VHDS virtual host vhds/name routing to the cluster of that name.
func withVirtualHostsOnDemand(config []proto.Message, names ...string) []proto.Message {
	withVhds := append(append([]proto.Message(nil), config...),
		listener("vhds", &hcmv3.HttpConnectionManager{
			StatPrefix:     "vhds",
			RouteSpecifier: &hcmv3.HttpConnectionManager_Rds{Rds: &hcmv3.Rds{RouteConfigName: "vhds", ConfigSource: ads()}},
			HttpFilters:    []*hcmv3.HttpFilter{httpFilter(onDemandFilter, &ondemandv3.OnDemand{}), httpFilter("envoy.filters.http.router", &routerv3.Router{})},
		}),
		&routev3.RouteConfiguration{Name: "vhds", Vhds: &routev3.Vhds{ConfigSource: ads()}})
	for _, name := range names {
		vh := virtualHost(name, name)
		vh.Name = "vhds/" + name
		withVhds = append(withVhds, vh)
	}
	return withVhds
}

// listener is a listener of this name on port 10001, with this HTTP connection manager.
func listener(name string, hcm *hcmv3.HttpConnectionManager) *listenerv3.Listener {
	typed, _ := anypb.New(hcm)
	return &listenerv3.Listener{
		Name: name,
		Address: &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
			Address: "0.0.0.0", PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: dynamicListenerPort},
		}}},
		FilterChains: []*listenerv3.FilterChain{{Filters: []*listenerv3.Filter{{
			Name: "envoy.filters.network.http_connection_manager", ConfigType: &listenerv3.Filter_TypedConfig{TypedConfig: typed},
		}}}},
	}
}

func httpFilter(name string, config proto.Message) *hcmv3.HttpFilter {
	typed, _ := anypb.New(config)
	return &hcmv3.HttpFilter{Name: name, ConfigType: &hcmv3.HttpFilter_TypedConfig{TypedConfig: typed}}
}

func ads() *corev3.ConfigSource {
	return &corev3.ConfigSource{ResourceApiVersion: corev3.ApiVersion_V3, ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}}}
}

// service is a single virtual host routing to a cluster of a different name.
func service(vhost, clusterName string) []proto.Message {
	return []proto.Message{cluster(clusterName), endpoints(clusterName),
		&routev3.RouteConfiguration{Name: routes, VirtualHosts: []*routev3.VirtualHost{virtualHost(vhost, clusterName)}}}
}

func cluster(name string) *clusterv3.Cluster {
	return &clusterv3.Cluster{
		Name:                 name,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS},
		ConnectTimeout:       durationpb.New(time.Second),
		EdsClusterConfig:     &clusterv3.Cluster_EdsClusterConfig{EdsConfig: ads()},
	}
}

func endpoints(clusterName string) *endpointv3.ClusterLoadAssignment {
	return endpointsAt(clusterName, "127.0.0.1")
}

// endpointsAt points at Envoy's own admin port, which stands in for the upstream: every 127/8 address in its container
// reaches it.
func endpointsAt(clusterName string, addresses ...string) *endpointv3.ClusterLoadAssignment {
	locality := &endpointv3.LocalityLbEndpoints{}
	for _, address := range addresses {
		locality.LbEndpoints = append(locality.LbEndpoints, &endpointv3.LbEndpoint{
			HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{Address: &corev3.Address{
				Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
					Address: address, PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: xdstest.AdminPort},
				}},
			}}},
		})
	}
	return &endpointv3.ClusterLoadAssignment{ClusterName: clusterName, Endpoints: []*endpointv3.LocalityLbEndpoints{locality}}
}

// withEndpoints moves the cluster's endpoints to these 127/8 addresses, changing nothing else.
func withEndpoints(config []proto.Message, clusterName string, addresses ...string) []proto.Message {
	moved := slices.Clone(config)
	for i, m := range moved {
		if cla, ok := m.(*endpointv3.ClusterLoadAssignment); ok && cla.ClusterName == clusterName {
			moved[i] = endpointsAt(clusterName, addresses...)
		}
	}
	return moved
}

func virtualHost(name, clusterName string) *routev3.VirtualHost {
	return &routev3.VirtualHost{
		Name:    name,
		Domains: []string{name},
		Routes: []*routev3.Route{{
			Match:  &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
			Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: clusterName}}},
		}},
	}
}
