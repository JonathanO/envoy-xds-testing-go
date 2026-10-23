// Tests a control plane the way its own authors would: make a change the way users do, then check the conversation
// with Envoy, Envoy's health, and traffic. go-control-plane stands in for your control plane.
package controlplane_test

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	xdstest "github.com/JonathanO/envoy-xds-testing-go"
	"github.com/JonathanO/envoy-xds-testing-go/examples"
)

func TestMovingAServiceToNewEndpoints(t *testing.T) {
	// The same test for each protocol: only Envoy's config and the checks differ.
	t.Run("sotw", func(t *testing.T) { movingAServiceToNewEndpoints(t, "testdata/envoy-sotw.yaml", xdstest.Sotw) })
	t.Run("delta", func(t *testing.T) { movingAServiceToNewEndpoints(t, "testdata/envoy-delta.yaml", xdstest.Delta) })
}

func movingAServiceToNewEndpoints(t *testing.T, bootstrapFile string, checksFor func(*xdstest.Conversation) xdstest.Checks) {
	ctx := context.Background()
	controlPlane := examples.NewGoControlPlane()
	controlPlane.SetConfig(service("foo", "127.0.0.1"))

	// Your server first, then Envoy. The other way round, Envoy backs off and can time out its first fetches.
	rec := &xdstest.Recorder{}
	server := grpc.NewServer(grpc.StreamInterceptor(rec.Intercept))
	controlPlane.Register(server)
	lis, err := net.Listen("tcp", ":6566")
	must(t, err)
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	bootstrap, err := os.ReadFile(bootstrapFile)
	must(t, err)
	envoy, err := xdstest.StartEnvoy(ctx, xdstest.EnvoyConfig{
		Image: "envoyproxy/envoy:v1.40-latest", Bootstrap: bootstrap, ADSPort: 6566, ListenerPorts: []int{10000},
	})
	must(t, err)
	t.Cleanup(func() { envoy.Terminate(context.Background()) })

	// Wait for the initial sync. Everything Envoy asked for must be answered well inside its 15s initial fetch timeout.
	must(t, xdstest.WaitFor(30*time.Second, func() error {
		if len(rec.Conversations()) == 0 {
			return errors.New("envoy hasn't connected")
		}
		return nil
	}))
	c := rec.Conversations()[0]
	must(t, xdstest.WaitFor(10*time.Second, func() error { return checksFor(c).HadConversationsWith(xdstest.CDS, xdstest.RDS) }))
	must(t, xdstest.WaitFor(10*time.Second, checksFor(c).AnsweredEveryType))
	must(t, xdstest.WaitFor(30*time.Second, xdstest.Settled(c, xdstest.DefaultQuietPeriod)))
	messageMark, logMark := c.Size(), envoy.LogSize()

	// Make a change the way your users would, and wait for it to settle. Envoy's admin port stands in for the
	// upstream, and every 127/8 address in its container reaches it.
	controlPlane.SetConfig(service("foo", "127.0.0.2"))
	must(t, xdstest.WaitFor(30*time.Second, xdstest.SettledAfter(c, messageMark, xdstest.Settled(c, xdstest.DefaultQuietPeriod))))

	checks := checksFor(c)
	must(t, checks.NothingSentUnasked(false))
	must(t, checks.AllWereAcked())
	must(t, checks.SentNoEmptyResponses(messageMark))
	must(t, checks.WasMakeBeforeBreak(messageMark))
	must(t, envoy.Healthy(ctx, nil))
	must(t, envoy.WarningsSince(logMark))
	must(t, xdstest.WaitFor(5*time.Second, func() error {
		status, err := envoy.HTTPStatus(ctx, 10000, "foo")
		if err == nil && status != 200 {
			err = errors.New("foo isn't answering 200")
		}
		return err
	}))
}

// service is a cluster, its endpoint, and a route to it from virtual host name.
func service(name, address string) []proto.Message {
	return []proto.Message{
		&clusterv3.Cluster{
			Name:                 name,
			ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS},
			ConnectTimeout:       durationpb.New(time.Second),
			EdsClusterConfig: &clusterv3.Cluster_EdsClusterConfig{EdsConfig: &corev3.ConfigSource{
				ResourceApiVersion:    corev3.ApiVersion_V3,
				ConfigSourceSpecifier: &corev3.ConfigSource_Ads{Ads: &corev3.AggregatedConfigSource{}},
			}},
		},
		&endpointv3.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: []*endpointv3.LbEndpoint{{
				HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{Address: &corev3.Address{
					Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
						Address: address, PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: xdstest.AdminPort},
					}},
				}}},
			}}}},
		},
		&routev3.RouteConfiguration{Name: "all_routes", VirtualHosts: []*routev3.VirtualHost{{
			Name:    name,
			Domains: []string{name},
			Routes: []*routev3.Route{{
				Match:  &routev3.RouteMatch{PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
				Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: name}}},
			}},
		}}},
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
