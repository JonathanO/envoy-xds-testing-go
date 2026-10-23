// Package examples shows envoy-xds-testing-go used with upstream go-control-plane.
//
// Each example starts a real Envoy in Docker, and they all use port 6566, so run them one package at a time from this
// directory: go test -p 1 ./...
package examples

import (
	"context"
	"strconv"
	"sync/atomic"

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
)

// NodeID is the node id in the suite's Envoy config, and in the control plane example's.
const NodeID = "test-envoy-0"

// GoControlPlane serves whole snapshots of config with go-control-plane's ADS snapshot cache. It implements
// conformance.ControlPlane.
type GoControlPlane struct {
	cache   cache.SnapshotCache
	server  server.Server
	version atomic.Int64
}

// NewGoControlPlane returns a control plane with no config yet.
func NewGoControlPlane() *GoControlPlane {
	c := cache.NewSnapshotCache(true, cache.IDHash{}, nil) // ADS mode
	return &GoControlPlane{cache: c, server: server.NewServer(context.Background(), c, nil)}
}

// Register serves SotW and delta ADS on s.
func (g *GoControlPlane) Register(s *grpc.Server) {
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(s, g.server)
}

// SetConfig replaces the whole config for NodeID.
func (g *GoControlPlane) SetConfig(resources []proto.Message) {
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
	if err := g.cache.SetSnapshot(context.Background(), NodeID, snapshot); err != nil {
		panic(err)
	}
}
