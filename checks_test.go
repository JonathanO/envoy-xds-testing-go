package xdstest

import (
	"strings"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func anyOf(t *testing.T, m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func edsCluster(name string) *clusterv3.Cluster {
	return &clusterv3.Cluster{Name: name, ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS}}
}

func routeTo(clusters ...string) *routev3.RouteConfiguration {
	vh := &routev3.VirtualHost{Name: "vh"}
	for _, c := range clusters {
		vh.Routes = append(vh.Routes, &routev3.Route{Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: c}}}})
	}
	return &routev3.RouteConfiguration{Name: "routes", VirtualHosts: []*routev3.VirtualHost{vh}}
}

// sotwExchange is a SotW response and Envoy's ACK of it.
func sotwExchange(t *testing.T, c *Conversation, typeURL, nonce string, resources ...proto.Message) {
	r := &discoveryv3.DiscoveryResponse{TypeUrl: typeURL, Nonce: nonce}
	for _, m := range resources {
		r.Resources = append(r.Resources, anyOf(t, m))
	}
	c.Add(r)
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: typeURL, ResponseNonce: nonce})
}

func deltaExchange(t *testing.T, c *Conversation, typeURL, nonce string, removed []string, resources ...proto.Message) {
	r := &discoveryv3.DeltaDiscoveryResponse{TypeUrl: typeURL, Nonce: nonce, RemovedResources: removed}
	for _, m := range resources {
		name, err := resourceName(anyOf(t, m))
		if err != nil {
			t.Fatal(err)
		}
		r.Resources = append(r.Resources, &discoveryv3.Resource{Name: name, Resource: anyOf(t, m)})
	}
	c.Add(r)
	c.Add(&discoveryv3.DeltaDiscoveryRequest{TypeUrl: typeURL, ResponseNonce: nonce})
}

func wantProblem(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("got %v, want an error containing %q", err, want)
	}
}

func TestSotwMakeBeforeBreak(t *testing.T) {
	// Move a route from a to b: add b and its endpoints, then the route, then remove a.
	c := NewConversation()
	sotwExchange(t, c, CDS, "1", edsCluster("a"))
	sotwExchange(t, c, EDS, "2", &endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	sotwExchange(t, c, RDS, "3", routeTo("a"))
	mark := c.Size()
	sotwExchange(t, c, CDS, "4", edsCluster("a"), edsCluster("b"))
	sotwExchange(t, c, EDS, "5", &endpointv3.ClusterLoadAssignment{ClusterName: "a"}, &endpointv3.ClusterLoadAssignment{ClusterName: "b"})
	sotwExchange(t, c, RDS, "6", routeTo("b"))
	sotwExchange(t, c, CDS, "7", edsCluster("b"))
	if err := Sotw(c).WasMakeBeforeBreak(mark); err != nil {
		t.Error(err)
	}

	// The same move all at once.
	c = NewConversation()
	sotwExchange(t, c, CDS, "1", edsCluster("a"))
	sotwExchange(t, c, EDS, "2", &endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	sotwExchange(t, c, RDS, "3", routeTo("a"))
	mark = c.Size()
	sotwExchange(t, c, CDS, "4", edsCluster("b"))
	sotwExchange(t, c, RDS, "5", routeTo("b"))
	err := Sotw(c).WasMakeBeforeBreak(mark)
	wantProblem(t, err, `cluster "a" removed while Envoy's ACKed route "routes" still uses it`)
	wantProblem(t, err, `route "routes" sent using cluster "b" before Envoy ACKed its endpoints`)
}

func TestDeltaMakeBeforeBreak(t *testing.T) {
	c := NewConversation()
	deltaExchange(t, c, CDS, "1", nil, edsCluster("a"))
	deltaExchange(t, c, EDS, "2", nil, &endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	deltaExchange(t, c, RDS, "3", nil, routeTo("a"))
	mark := c.Size()
	deltaExchange(t, c, CDS, "4", nil, edsCluster("b"))
	deltaExchange(t, c, EDS, "5", nil, &endpointv3.ClusterLoadAssignment{ClusterName: "b"})
	deltaExchange(t, c, RDS, "6", nil, routeTo("b"))
	deltaExchange(t, c, CDS, "7", []string{"a"})
	if err := Delta(c).WasMakeBeforeBreak(mark); err != nil {
		t.Error(err)
	}

	c = NewConversation()
	deltaExchange(t, c, CDS, "1", nil, edsCluster("a"))
	deltaExchange(t, c, RDS, "2", nil, routeTo("a"))
	mark = c.Size()
	deltaExchange(t, c, CDS, "3", []string{"a"})
	// Sent before the ACK of cluster b.
	c.Add(&discoveryv3.DeltaDiscoveryResponse{TypeUrl: CDS, Nonce: "4", Resources: []*discoveryv3.Resource{{Name: "b", Resource: anyOf(t, edsCluster("b"))}}})
	c.Add(&discoveryv3.DeltaDiscoveryResponse{TypeUrl: RDS, Nonce: "5", Resources: []*discoveryv3.Resource{{Name: "routes", Resource: anyOf(t, routeTo("b"))}}})
	err := Delta(c).WasMakeBeforeBreak(mark)
	wantProblem(t, err, `cluster "a" removed while Envoy's ACKed route "routes" still uses it`)
	wantProblem(t, err, `route "routes" sent using cluster "b" before Envoy ACKed the cluster`)
}
