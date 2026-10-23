// Runs the conformance suite against upstream go-control-plane. Expect it to fail. On Envoy 1.40 with go-control-plane
// v0.14.0:
//
//   - StartsEmpty, both: on SotW, the route to foo is sent before Envoy ACKs foo (not make before break). On delta, the
//     route config Envoy asks for doesn't exist yet, and in ADS mode it never answers.
//   - AddsAndRemovesServices, both: the route to bar is sent before Envoy ACKs bar.
//   - MovesRouteToNewCluster, both: foo-v1 is removed while the route still uses it, and the route to foo-v2 is sent
//     before Envoy ACKs foo-v2.
//   - ContainsRejectedConfig, SotW only: the rejected cluster is sent again, and Envoy keeps rejecting it.
//   - ChangesWhileDisconnected, both: the same as MovesRouteToNewCluster, after Envoy reconnects.
//
// Reconnects passes on both, and ContainsRejectedConfig on delta. The make before break failures are all one cause:
// go-control-plane sends each snapshot one type at a time, in a fixed order, without waiting for Envoy's ACKs.
//
// Run with: go test ./... (needs Docker)
package conformance_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	resourcev3 "github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/JonathanO/envoy-xds-testing-go/conformance"
)

func TestSotw(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Sotw,
		New:      newGoControlPlane,
	})
}

func TestDelta(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Delta,
		New:      newGoControlPlane,
	})
}

// goControlPlane is a conformance.ControlPlane serving whole snapshots with go-control-plane's ADS snapshot cache.
type goControlPlane struct {
	cache   cache.SnapshotCache
	server  server.Server
	version atomic.Int64
}

func newGoControlPlane(*testing.T) conformance.ControlPlane {
	c := cache.NewSnapshotCache(true, cache.IDHash{}, nil) // ADS mode
	return &goControlPlane{cache: c, server: server.NewServer(context.Background(), c, nil)}
}

func (g *goControlPlane) Register(s *grpc.Server) {
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(s, g.server)
}

func (g *goControlPlane) SetConfig(resources []proto.Message) {
	// Every type needs an entry, even an empty one, or go-control-plane never answers Envoy's request for it.
	byType := map[resourcev3.Type][]types.Resource{resourcev3.ClusterType: nil, resourcev3.EndpointType: nil, resourcev3.RouteType: nil}
	for _, r := range resources {
		switch r.(type) {
		case *clusterv3.Cluster:
			byType[resourcev3.ClusterType] = append(byType[resourcev3.ClusterType], r)
		case *endpointv3.ClusterLoadAssignment:
			byType[resourcev3.EndpointType] = append(byType[resourcev3.EndpointType], r)
		case *routev3.RouteConfiguration:
			byType[resourcev3.RouteType] = append(byType[resourcev3.RouteType], r)
		}
	}
	snapshot, err := cache.NewSnapshot(strconv.FormatInt(g.version.Add(1), 10), byType)
	if err != nil {
		panic(err)
	}
	// The suite's Envoy uses node id test-envoy-0.
	if err := g.cache.SetSnapshot(context.Background(), "test-envoy-0", snapshot); err != nil {
		panic(err)
	}
}
