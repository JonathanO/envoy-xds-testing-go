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
	"regexp"
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

// Protocol is the ADS flavour Envoy uses.
type Protocol string

const (
	Sotw  Protocol = "sotw"
	Delta Protocol = "delta"
)

// Options configure a run.
type Options struct {
	// Image is the Envoy image, e.g. "envoyproxy/envoy:v1.40-latest".
	Image    string
	Protocol Protocol
	// New returns a fresh control plane for each test.
	New func(t *testing.T) ControlPlane
	// Skip records known deviations: test name to reason.
	Skip map[string]string
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

	minorVersion = regexp.MustCompile(`:v1\.(\d+)`)
)

// Run runs the suite as subtests of t. Envoy connects to port 6566 on the host, so don't run it in parallel.
func Run(t *testing.T, o Options) {
	tests := []struct {
		name string
		run  func(h *harness)
	}{
		{"StartsEmpty", startsEmpty},
		{"AddsAndRemovesServices", addsAndRemovesServices},
		{"MovesRouteToNewCluster", movesRouteToNewCluster},
		{"RecoversFromRejectedConfig", recoversFromRejectedConfig},
		{"Reconnects", reconnects},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if reason, ok := o.Skip[test.name]; ok {
				t.Skip(reason)
			}
			test.run(newHarness(t, o))
		})
	}
}

// StartsEmpty: with no config at first, Envoy gets an answer for every type, then takes the first service.
func startsEmpty(h *harness) {
	h.cp.SetConfig(nil)
	h.start()
	c := h.synced(1)
	h.assertSettled(c, false)

	h.change(c, services("foo"))
	h.assertSettled(c, false)
	h.assertStatus("foo", 200)
}

// AddsAndRemovesServices: a route, its cluster and its endpoints added in one change, then removed in one change.
func addsAndRemovesServices(h *harness) {
	h.cp.SetConfig(services("foo"))
	h.start()
	c := h.synced(1)

	h.change(c, services("foo", "bar"))
	h.assertSettled(c, false)
	h.assertStatus("bar", 200)

	h.change(c, services("foo"))
	// Envoy drops its EDS subscription for bar, which needs no response.
	h.must(xdstest.WaitFor(10*time.Second, func() error { return h.checks(c).UnsubscribedFrom(xdstest.EDS, "bar") }))
	h.must(xdstest.WaitFor(10*time.Second, h.settled(c)))
	h.assertSettled(c, false)
	h.assertStatus("bar", 404)
	h.assertStatus("foo", 200)
}

// MovesRouteToNewCluster: a route moved to a new cluster, and its old cluster removed, in one change.
func movesRouteToNewCluster(h *harness) {
	h.cp.SetConfig(service("foo", "foo-v1"))
	h.start()
	c := h.synced(1)
	h.assertStatus("foo", 200)

	h.change(c, service("foo", "foo-v2"))
	h.assertSettled(c, false)
	h.assertStatus("foo", 200)
}

// RecoversFromRejectedConfig: Envoy rejects a bad cluster; the next good config still gets through on the same stream.
func recoversFromRejectedConfig(h *harness) {
	h.cp.SetConfig(services("foo"))
	h.start()
	h.synced(1)

	bad := cluster("bad")
	bad.ConnectTimeout = &durationpb.Duration{Seconds: -1}
	h.cp.SetConfig(append(services("foo"), bad))
	h.must(xdstest.WaitFor(10*time.Second, h.envoyRejectedSomething))
	h.logMark = h.envoy.LogSize()
	stats, err := h.envoy.Stats(h.ctx)
	h.must(err)
	h.statsMark = stats

	h.cp.SetConfig(services("foo", "bar"))
	h.assertStatus("bar", 200)
	if n := len(h.rec.Conversations()); n != 1 {
		h.t.Fatalf("%d streams; the control plane shouldn't drop Envoy for a NACK", n)
	}
	h.must(xdstest.WaitFor(10*time.Second, h.settled(h.rec.Conversations()[0])))
	h.must(h.envoy.Healthy(h.ctx, h.statsMark))
	h.must(h.envoy.WarningsSince(h.logMark))
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
	h.assertSettled(c, true)
	h.assertStatus("foo", 200)
	h.assertStatus("bar", 404)

	h.change(c, services("foo", "baz"))
	h.assertSettled(c, true)
	h.assertStatus("baz", 200)
}

type harness struct {
	t         *testing.T
	ctx       context.Context
	o         Options
	cp        ControlPlane
	rec       *xdstest.Recorder
	server    *grpc.Server
	envoy     *xdstest.Envoy
	logMark   int
	msgMark   int
	statsMark map[string]uint64
}

func newHarness(t *testing.T, o Options) *harness {
	h := &harness{t: t, ctx: context.Background(), o: o, cp: o.New(t), rec: &xdstest.Recorder{}}
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
		if h.o.Protocol == Delta {
			bootstrap = deltaBootstrap
		}
		h.envoy, err = xdstest.StartEnvoy(h.ctx, xdstest.EnvoyConfig{
			Image: h.o.Image, Bootstrap: bootstrap, ADSPort: adsPort, ListenerPorts: []int{listenerPort}, Args: []string{"-l", "debug"},
		})
		h.must(err)
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
	h.logMark = h.envoy.LogSize()
	h.msgMark = c.Size()
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

// assertSettled checks everything we expect once settled: a sane conversation, and a healthy Envoy that logged no
// warnings since synced(). After a reconnect we don't know what config Envoy kept, so make before break can't be
// checked.
func (h *harness) assertSettled(c *xdstest.Conversation, reconnected bool) {
	h.t.Helper()
	checks := h.checks(c)
	h.must(checks.NothingSentUnasked(reconnected && h.brokenWildcardReconnect()))
	h.must(checks.AllWereAcked())
	h.must(checks.SentNoEmptyResponses(h.msgMark))
	if !reconnected {
		h.must(checks.WasMakeBeforeBreak(h.msgMark))
	}
	h.must(h.envoy.Healthy(h.ctx, h.statsMark))
	h.must(h.envoy.WarningsSince(h.logMark))
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
	if h.o.Protocol == Delta {
		return xdstest.Delta(c)
	}
	return xdstest.Sotw(c)
}

// brokenWildcardReconnect: before 1.19 Envoy resent resource names for wildcard types on reconnect.
func (h *harness) brokenWildcardReconnect() bool {
	m := minorVersion.FindStringSubmatch(h.o.Image)
	if m == nil {
		return false
	}
	minor, _ := strconv.Atoi(m[1])
	return minor < 19
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
