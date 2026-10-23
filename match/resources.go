package match

import (
	"fmt"
	"slices"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/proto"

	xdstest "github.com/JonathanO/envoy-xds-testing-go"
)

// Named matches a resource with this name: its name field, or cluster_name for a ClusterLoadAssignment.
func Named(name string) Matcher[proto.Message] {
	return New(fmt.Sprintf("named %q", name), func(m proto.Message) bool { return xdstest.NameOf(m) == name })
}

// Cluster matches a Cluster that all of ms match.
func Cluster(ms ...Matcher[proto.Message]) Matcher[proto.Message] {
	return resource[*clusterv3.Cluster]("a Cluster", ms)
}

// ClusterType matches a Cluster of this discovery type, e.g. clusterv3.Cluster_EDS.
func ClusterType(t clusterv3.Cluster_DiscoveryType) Matcher[proto.Message] {
	return New("of type "+t.String(), func(m proto.Message) bool {
		c, ok := m.(*clusterv3.Cluster)
		return ok && c.GetType() == t
	})
}

// RouteConfig matches a RouteConfiguration that all of ms match.
func RouteConfig(ms ...Matcher[proto.Message]) Matcher[proto.Message] {
	return resource[*routev3.RouteConfiguration]("a RouteConfiguration", ms)
}

// RoutingTo matches a RouteConfiguration with a route to this cluster, directly or as a weighted cluster.
func RoutingTo(cluster string) Matcher[proto.Message] {
	return New(fmt.Sprintf("routing to %q", cluster), func(m proto.Message) bool {
		r, ok := m.(*routev3.RouteConfiguration)
		return ok && slices.Contains(clustersRoutedTo(r.GetVirtualHosts()), cluster)
	})
}

// Routing matches a RouteConfiguration whose virtual host of this name routes to this cluster.
func Routing(virtualHost, cluster string) Matcher[proto.Message] {
	return New(fmt.Sprintf("routing virtual host %q to %q", virtualHost, cluster), func(m proto.Message) bool {
		r, ok := m.(*routev3.RouteConfiguration)
		if !ok {
			return false
		}
		hosts := slices.DeleteFunc(slices.Clone(r.GetVirtualHosts()), func(v *routev3.VirtualHost) bool { return v.GetName() != virtualHost })
		return slices.Contains(clustersRoutedTo(hosts), cluster)
	})
}

// EndpointAssignment matches a ClusterLoadAssignment that all of ms match.
func EndpointAssignment(ms ...Matcher[proto.Message]) Matcher[proto.Message] {
	return resource[*endpointv3.ClusterLoadAssignment]("a ClusterLoadAssignment", ms)
}

// WithEndpoint matches a ClusterLoadAssignment with an endpoint at this address and port.
func WithEndpoint(address string, port uint32) Matcher[proto.Message] {
	return New(fmt.Sprintf("with endpoint %s:%d", address, port), func(m proto.Message) bool {
		cla, ok := m.(*endpointv3.ClusterLoadAssignment)
		if !ok {
			return false
		}
		for _, l := range cla.GetEndpoints() {
			for _, e := range l.GetLbEndpoints() {
				a := e.GetEndpoint().GetAddress().GetSocketAddress()
				if a.GetAddress() == address && a.GetPortValue() == port {
					return true
				}
			}
		}
		return false
	})
}

func resource[R proto.Message](what string, ms []Matcher[proto.Message]) Matcher[proto.Message] {
	all := AllOf(ms...)
	return New(with(what, ms), func(m proto.Message) bool {
		_, ok := m.(R)
		return ok && all.Match(m)
	})
}

func clustersRoutedTo(hosts []*routev3.VirtualHost) []string {
	var clusters []string
	for _, v := range hosts {
		for _, r := range v.GetRoutes() {
			if c := r.GetRoute().GetCluster(); c != "" {
				clusters = append(clusters, c)
			}
			for _, w := range r.GetRoute().GetWeightedClusters().GetClusters() {
				clusters = append(clusters, w.GetName())
			}
		}
	}
	return clusters
}
