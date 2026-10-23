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
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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
	// Clusters, ClusterLoadAssignments and RouteConfigurations.
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

// EnvoyImages are the Envoy versions conformance means: the oldest supported and the newest.
var EnvoyImages = []string{"envoyproxy/envoy:v1.17-latest", "envoyproxy/envoy:v1.40-latest"}

// Options configure a run.
type Options struct {
	// New returns a fresh control plane for each test.
	New func(t *testing.T) ControlPlane
	// Protocols the library supports, by default both. Conformance results should say if it's not both.
	Protocols []Protocol
	// Skip records known deviations, with a reason. A key is a test's path, or part of it: any of protocol (sotw,
	// delta), Envoy version (v1.17), mode (normal, rate-limited) and scenario (ChangesWhileDisconnected), joined by "/".
	// "sotw/ChangesWhileDisconnected" skips that scenario on SotW everywhere.
	Skip map[string]string
}

// variant is one place in the matrix.
type variant struct {
	protocol    Protocol
	image       string
	rateLimited bool // rate_limit_settings on Envoy's requests: it queues them, and delta sends ACKs ahead of queued subscriptions
}

const (
	adsPort      = 6566
	listenerPort = 10000
	routes       = "all_routes" // Envoy's static listener asks for this
)

var (
	//go:embed envoy-sotw.yaml
	sotwBootstrap []byte
	//go:embed envoy-delta.yaml
	deltaBootstrap []byte
)

// Run runs every scenario as subtests of t: on each protocol, on each of EnvoyImages, normally and with Envoy's requests
// rate limited, e.g. t/sotw/v1.17/rate-limited/StartsEmpty. Envoy connects to port 6566 on the host, so don't run it in
// parallel.
func Run(t *testing.T, o Options) {
	tests := []struct {
		name string
		run  func(h *harness)
	}{
		{"StartsEmpty", startsEmpty},
		{"AddsAndRemovesServices", addsAndRemovesServices},
		{"MovesRouteToNewCluster", movesRouteToNewCluster},
		{"ContainsRejectedConfig", containsRejectedConfig},
		{"Reconnects", reconnects},
		{"ChangesWhileDisconnected", changesWhileDisconnected},
	}
	protocols := o.Protocols
	if len(protocols) == 0 {
		protocols = []Protocol{Sotw, Delta}
	}
	for _, protocol := range protocols {
		t.Run(string(protocol), func(t *testing.T) {
			for _, image := range EnvoyImages {
				version := strings.TrimSuffix(image[strings.LastIndex(image, ":")+1:], "-latest")
				t.Run(version, func(t *testing.T) {
					for _, rateLimited := range []bool{false, true} {
						mode := "normal"
						if rateLimited {
							mode = "rate-limited"
						}
						t.Run(mode, func(t *testing.T) {
							for _, test := range tests {
								t.Run(test.name, func(t *testing.T) {
									if reason, ok := deviation(o.Skip, string(protocol), version, mode, test.name); ok {
										t.Skip(reason)
									}
									test.run(newHarness(t, o, variant{protocol: protocol, image: image, rateLimited: rateLimited}))
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

	h.change(c, services("foo", "bar"))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertStatus("bar", 200)

	h.change(c, services("foo"))
	// Envoy drops its EDS subscription for bar, which needs no response.
	h.must(xdstest.WaitFor(10*time.Second, func() error { return h.checks(c).UnsubscribedFrom(xdstest.EDS, "bar") }))
	h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
	h.must(h.checkpoint.Sane(h.ctx))
	h.assertStatus("bar", 404)
	h.assertStatus("foo", 200)
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

type harness struct {
	t          *testing.T
	ctx        context.Context
	v          variant
	cp         ControlPlane
	rec        *xdstest.Recorder
	server     *grpc.Server
	envoy      *xdstest.Envoy
	checkpoint *xdstest.Checkpoint
}

func newHarness(t *testing.T, o Options, v variant) *harness {
	h := &harness{t: t, ctx: context.Background(), v: v, cp: o.New(t), rec: &xdstest.Recorder{}}
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
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(adsPort))
	h.must(err)
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
		h.envoy, err = xdstest.StartEnvoy(h.ctx, xdstest.EnvoyConfig{
			Image: h.v.image, Bootstrap: bootstrap, ADSPort: adsPort, ListenerPorts: []int{listenerPort}, Args: []string{"-l", "debug"},
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
	h.must(xdstest.WaitFor(10*time.Second, func() error {
		return errors.Join(h.checks(c).HadConversationsWith(xdstest.CDS, xdstest.RDS), h.checks(c).AnsweredEveryType())
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
	// Envoy ACKs once its main thread has the config; workers get it a moment later.
	h.must(xdstest.WaitFor(5*time.Second, func() error {
		got, err := h.envoy.HTTPStatus(h.ctx, listenerPort, virtualHost)
		if err == nil && got != want {
			err = fmt.Errorf("%s: status %d, want %d", virtualHost, got, want)
		}
		return err
	}))
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
		EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{EdsConfig: &corev3.ConfigSource{
			ResourceApiVersion:    corev3.ApiVersion_V3,
			ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
		}},
	}
}

// endpoints points at Envoy's own admin port, which stands in for the upstream: every 127/8 address in its container
// reaches it.
func endpoints(clusterName string) *endpointv3.ClusterLoadAssignment {
	return &endpointv3.ClusterLoadAssignment{
		ClusterName: clusterName,
		Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: []*endpointv3.LbEndpoint{{
			HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{Address: &corev3.Address{
				Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
					Address: "127.0.0.1", PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: xdstest.AdminPort},
				}},
			}}},
		}}}},
	}
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
