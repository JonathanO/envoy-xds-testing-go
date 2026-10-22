package xdstest

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3" // so resourceName can decode listeners
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/types/known/anypb"
)

// ackedState tracks what Envoy has ACKed, to check each new response against it for make before break.
type ackedState struct {
	clusters    map[string]bool
	edsClusters map[string]bool
	endpoints   map[string]bool
	routes      map[string]map[string]bool // clusters used, by route config name
	problems    []string
}

func newAckedState() *ackedState {
	return &ackedState{clusters: map[string]bool{}, edsClusters: map[string]bool{}, endpoints: map[string]bool{}, routes: map[string]map[string]bool{}}
}

// checkResponse checks a response before Envoy sees it.
func (s *ackedState) checkResponse(typeURL string, resources []*anypb.Any, removed []string) error {
	switch typeURL {
	case RDS:
		for _, a := range resources {
			route := &routev3.RouteConfiguration{}
			if err := a.UnmarshalTo(route); err != nil {
				return err
			}
			for _, c := range sortedKeys(referencedClusters(route)) {
				if !s.clusters[c] {
					s.problems = append(s.problems, fmt.Sprintf("route %q sent using cluster %q before Envoy ACKed the cluster", route.GetName(), c))
				} else if s.edsClusters[c] && !s.endpoints[c] {
					s.problems = append(s.problems, fmt.Sprintf("route %q sent using cluster %q before Envoy ACKed its endpoints", route.GetName(), c))
				}
			}
		}
	case CDS:
		for _, c := range removed {
			for _, name := range sortedKeys(boolKeys(s.routes)) {
				if s.routes[name][c] {
					s.problems = append(s.problems, fmt.Sprintf("cluster %q removed while Envoy's ACKed route %q still uses it", c, name))
				}
			}
		}
	}
	return nil
}

// acked records a response Envoy has ACKed.
func (s *ackedState) acked(typeURL string, resources []*anypb.Any, removed []string) error {
	switch typeURL {
	case CDS:
		for _, a := range resources {
			cluster := &clusterv3.Cluster{}
			if err := a.UnmarshalTo(cluster); err != nil {
				return err
			}
			s.clusters[cluster.GetName()] = true
			if cluster.GetType() == clusterv3.Cluster_EDS {
				s.edsClusters[cluster.GetName()] = true
			} else {
				delete(s.edsClusters, cluster.GetName())
			}
		}
		for _, c := range removed {
			delete(s.clusters, c)
			delete(s.edsClusters, c)
		}
	case EDS:
		for _, a := range resources {
			cla := &endpointv3.ClusterLoadAssignment{}
			if err := a.UnmarshalTo(cla); err != nil {
				return err
			}
			s.endpoints[cla.GetClusterName()] = true
		}
		for _, c := range removed {
			delete(s.endpoints, c)
		}
	case RDS:
		for _, a := range resources {
			route := &routev3.RouteConfiguration{}
			if err := a.UnmarshalTo(route); err != nil {
				return err
			}
			s.routes[route.GetName()] = referencedClusters(route)
		}
		for _, r := range removed {
			delete(s.routes, r)
		}
	}
	return nil
}

func (s *ackedState) err() error {
	if len(s.problems) == 0 {
		return nil
	}
	sort.Strings(s.problems)
	return errors.New("not make before break: " + strings.Join(s.problems, "; "))
}

func referencedClusters(route *routev3.RouteConfiguration) map[string]bool {
	used := map[string]bool{}
	for _, vh := range route.GetVirtualHosts() {
		for _, r := range vh.GetRoutes() {
			action := r.GetRoute()
			if action.GetCluster() != "" {
				used[action.GetCluster()] = true
			}
			for _, w := range action.GetWeightedClusters().GetClusters() {
				used[w.GetName()] = true
			}
		}
	}
	return used
}

func boolKeys[V any](m map[string]V) map[string]bool {
	keys := make(map[string]bool, len(m))
	for k := range m {
		keys[k] = true
	}
	return keys
}
