package match_test

import (
	"strings"
	"testing"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	xdstest "github.com/JonathanO/envoy-xds-testing-go"
	"github.com/JonathanO/envoy-xds-testing-go/match"
)

func routes(virtualHost, cluster string) *routev3.RouteConfiguration {
	return &routev3.RouteConfiguration{Name: "all_routes", VirtualHosts: []*routev3.VirtualHost{{Name: virtualHost, Routes: []*routev3.Route{{
		Action: &routev3.Route_Route{Route: &routev3.RouteAction{ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: cluster}}},
	}}}}}
}

func pack(t *testing.T, m proto.Message) *anypb.Any {
	a, err := anypb.New(m)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func deltaResponse(t *testing.T, typeURL, nonce string, route *routev3.RouteConfiguration, removed ...string) *discoveryv3.DeltaDiscoveryResponse {
	r := &discoveryv3.DeltaDiscoveryResponse{TypeUrl: typeURL, Nonce: nonce, RemovedResources: removed}
	if route != nil {
		r.Resources = []*discoveryv3.Resource{{Name: route.GetName(), Resource: pack(t, route)}}
	}
	return r
}

func ack(typeURL, nonce string) *discoveryv3.DeltaDiscoveryRequest {
	return &discoveryv3.DeltaDiscoveryRequest{TypeUrl: typeURL, ResponseNonce: nonce}
}

var routeAwayAckedBeforeRemoval = []match.Matcher[proto.Message]{
	match.AckOf(match.Response(xdstest.RDS, match.Containing(match.RouteConfig(match.Not(match.RoutingTo("bar")))))),
	match.Response(xdstest.CDS, match.Removing("bar")),
}

func TestResources(t *testing.T) {
	bar := &clusterv3.Cluster{Name: "bar", ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_EDS}}
	for _, c := range []struct {
		m    match.Matcher[proto.Message]
		v    proto.Message
		want bool
	}{
		{match.Cluster(match.Named("bar"), match.ClusterType(clusterv3.Cluster_EDS)), bar, true},
		{match.Cluster(match.Named("foo")), bar, false},
		{match.RouteConfig(match.Routing("foo", "foo-v2")), routes("foo", "foo-v2"), true},
		{match.RouteConfig(match.Routing("bar", "foo-v2")), routes("foo", "foo-v2"), false},
		{match.Cluster(), routes("foo", "foo"), false},
	} {
		if got := c.m.Match(c.v); got != c.want {
			t.Errorf("%s matching %v: got %v, want %v", c.m, c.v, got, c.want)
		}
	}
}

func TestInOrderWithAnAck(t *testing.T) {
	inOrder := xdstest.NewConversation()
	inOrder.Add(deltaResponse(t, xdstest.RDS, "1", routes("foo", "foo")))
	inOrder.Add(ack(xdstest.RDS, "1"))
	inOrder.Add(deltaResponse(t, xdstest.CDS, "2", nil, "bar"))
	if err := match.InOrder(inOrder, routeAwayAckedBeforeRemoval...); err != nil {
		t.Error(err)
	}

	removedFirst := xdstest.NewConversation()
	removedFirst.Add(deltaResponse(t, xdstest.RDS, "1", routes("foo", "foo")))
	removedFirst.Add(deltaResponse(t, xdstest.CDS, "2", nil, "bar"))
	removedFirst.Add(ack(xdstest.RDS, "1"))
	err := match.InOrder(removedFirst, routeAwayAckedBeforeRemoval...)
	if err == nil || !strings.Contains(err.Error(), `step 2, a response for Cluster (removing "bar"), not found after message 2`) {
		t.Errorf("got %v", err)
	}
}

func TestNacks(t *testing.T) {
	nack := &discoveryv3.DiscoveryRequest{TypeUrl: xdstest.CDS, ResponseNonce: "1", ErrorDetail: &status.Status{Message: "bad connect_timeout"}}
	if !match.Nack(xdstest.CDS, match.WithMessage("connect_timeout")).Match(nack) {
		t.Error("didn't match the NACK")
	}
	if match.Nack("").Match(ack(xdstest.CDS, "1")) {
		t.Error("matched an ACK")
	}
}

func TestSotwRemovesByLeavingOutOfAWildcardType(t *testing.T) {
	cdsWithoutBar := &discoveryv3.DiscoveryResponse{TypeUrl: xdstest.CDS, Resources: []*anypb.Any{pack(t, &clusterv3.Cluster{Name: "foo"})}}
	rdsWithoutBar := &discoveryv3.DiscoveryResponse{TypeUrl: xdstest.RDS}
	if !match.Response("", match.Removing("bar")).Match(cdsWithoutBar) {
		t.Error("CDS without bar should remove it")
	}
	if match.Response("", match.Removing("foo")).Match(cdsWithoutBar) {
		t.Error("CDS with foo shouldn't remove it")
	}
	if match.Response("", match.Removing("bar")).Match(rdsWithoutBar) {
		t.Error("RDS isn't a wildcard type")
	}
}
