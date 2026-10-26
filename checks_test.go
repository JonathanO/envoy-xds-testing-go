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

func sotwResponse(t *testing.T, c *Conversation, typeURL, nonce string, resources ...proto.Message) {
	r := &discoveryv3.DiscoveryResponse{TypeUrl: typeURL, Nonce: nonce}
	for _, m := range resources {
		r.Resources = append(r.Resources, anyOf(t, m))
	}
	c.Add(r)
}

// sotwExchange is a SotW response and Envoy's ACK of it.
func sotwExchange(t *testing.T, c *Conversation, typeURL, nonce string, resources ...proto.Message) {
	sotwResponse(t, c, typeURL, nonce, resources...)
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

func TestDeltaMakeBeforeBreakAfterAReconnect(t *testing.T) {
	// Envoy left the first stream with a route to v1.
	before := NewConversation()
	deltaExchange(t, before, CDS, "1", nil, edsCluster("v1"))
	deltaExchange(t, before, EDS, "2", nil, &endpointv3.ClusterLoadAssignment{ClusterName: "v1"})
	deltaExchange(t, before, RDS, "3", nil, routeTo("v1"))

	after := NewConversation()
	deltaExchange(t, after, CDS, "4", nil, edsCluster("v1"), edsCluster("v2"))
	deltaExchange(t, after, EDS, "5", nil, &endpointv3.ClusterLoadAssignment{ClusterName: "v2"})
	deltaExchange(t, after, RDS, "6", nil, routeTo("v2"))
	deltaExchange(t, after, CDS, "7", []string{"v1"})
	if err := Delta(after).WasMakeBeforeBreakAfter(before, 0); err != nil {
		t.Error(err)
	}

	removedFirst := NewConversation()
	deltaExchange(t, removedFirst, CDS, "4", []string{"v1"}, edsCluster("v2"))
	wantProblem(t, Delta(removedFirst).WasMakeBeforeBreakAfter(before, 0), `cluster "v1" removed while Envoy's ACKed route "routes" still uses it`)
	if err := Delta(removedFirst).WasMakeBeforeBreak(0); err != nil {
		t.Errorf("without the conversation before, there's nothing to compare with: %v", err)
	}
}

func TestSotwNonceEchoedFromThePreviousStream(t *testing.T) {
	c := NewConversation()
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: CDS, ResponseNonce: "1"}) // Envoy reconnecting, with its last nonce
	c.Add(&discoveryv3.DiscoveryResponse{TypeUrl: CDS, Nonce: "1"})
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: CDS, ResponseNonce: "1"})
	wantProblem(t, Sotw(c).AllWereAcked(), "which Envoy had echoed from its previous stream")
}

func TestWildcardTypesAreTheOnesEnvoySubscribesToWithAWildcard(t *testing.T) {
	// The xDS docs name only LDS and CDS, but Envoy's SRDS subscription also starts with no names:
	// source/common/router/scoped_rds.h, subscription_->start({}).
	for _, typeURL := range []string{LDS, RDS, SRDS, VHDS, CDS, EDS, SDS, RTDS} {
		if want := typeURL == LDS || typeURL == CDS || typeURL == SRDS; IsWildcard(typeURL) != want {
			t.Errorf("IsWildcard(%s) = %v, want %v", typeURL, !want, want)
		}
	}
}

func TestMakeBeforeBreakFindsEndpointsUnderTheClustersEdsServiceName(t *testing.T) {
	cluster := edsCluster("v1")
	cluster.EdsClusterConfig = &clusterv3.Cluster_EdsClusterConfig{ServiceName: "v1-endpoints"}
	endpoints := &endpointv3.ClusterLoadAssignment{ClusterName: "v1-endpoints"}

	c := NewConversation()
	sotwExchange(t, c, CDS, "1", cluster)
	sotwExchange(t, c, EDS, "2", endpoints)
	sotwExchange(t, c, RDS, "3", routeTo("v1"))
	if err := Sotw(c).WasMakeBeforeBreak(0); err != nil {
		t.Error(err)
	}

	c = NewConversation()
	deltaExchange(t, c, CDS, "1", nil, cluster)
	deltaExchange(t, c, EDS, "2", nil, endpoints)
	deltaExchange(t, c, RDS, "3", nil, routeTo("v1"))
	if err := Delta(c).WasMakeBeforeBreak(0); err != nil {
		t.Error(err)
	}
}

func TestSotwMakeBeforeBreakDoesNotTakeAnAckOfAnotherTypesNonce(t *testing.T) {
	// Envoy keeps SotW nonces per type, so EDS's ACK of its nonce 2 isn't CDS's.
	c := NewConversation()
	sotwExchange(t, c, CDS, "1", edsCluster("a"))
	sotwExchange(t, c, EDS, "1", &endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	sotwResponse(t, c, EDS, "2", &endpointv3.ClusterLoadAssignment{ClusterName: "a"})
	sotwResponse(t, c, CDS, "2", edsCluster("a"), &clusterv3.Cluster{Name: "b"})
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: EDS, ResponseNonce: "2"})
	sotwResponse(t, c, RDS, "1", routeTo("b"))
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: CDS, ResponseNonce: "2"})
	c.Add(&discoveryv3.DiscoveryRequest{TypeUrl: RDS, ResponseNonce: "1"})
	if err := Sotw(c).AllWereAcked(); err != nil {
		t.Error(err)
	}
	wantProblem(t, Sotw(c).WasMakeBeforeBreak(0), `route "routes" sent using cluster "b" before Envoy ACKed the cluster`)
}
