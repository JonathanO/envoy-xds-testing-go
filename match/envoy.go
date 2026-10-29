package match

import (
	"fmt"
	"slices"

	adminv3 "github.com/envoyproxy/go-control-plane/envoy/admin/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	"google.golang.org/protobuf/proto"
)

// ActiveClusters returns Envoy's active dynamic clusters from a config dump that has its clusters section: the whole
// dump, not one filtered with ?resource=, which lists clusters on their own.
func ActiveClusters(dump *adminv3.ConfigDump) ([]*clusterv3.Cluster, error) {
	var clusters []*clusterv3.Cluster
	for _, a := range dump.GetConfigs() {
		section := &adminv3.ClustersConfigDump{}
		if !a.MessageIs(section) {
			continue
		}
		if err := a.UnmarshalTo(section); err != nil {
			return nil, err
		}
		for _, c := range section.GetDynamicActiveClusters() {
			cluster := &clusterv3.Cluster{}
			if err := c.GetCluster().UnmarshalTo(cluster); err != nil {
				return nil, err
			}
			clusters = append(clusters, cluster)
		}
	}
	return clusters, nil
}

// ActiveClusterNames returns the names of Envoy's active dynamic clusters, sorted.
func ActiveClusterNames(dump *adminv3.ConfigDump) ([]string, error) {
	clusters, err := ActiveClusters(dump)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(clusters))
	for i, c := range clusters {
		names[i] = c.GetName()
	}
	slices.Sort(names)
	return names, nil
}

// HasActiveCluster checks Envoy has an active dynamic cluster that m matches.
func HasActiveCluster(dump *adminv3.ConfigDump, m Matcher[proto.Message]) error {
	clusters, err := ActiveClusters(dump)
	if err != nil {
		return err
	}
	for _, c := range clusters {
		if m.Match(c) {
			return nil
		}
	}
	names, _ := ActiveClusterNames(dump)
	return fmt.Errorf("no active cluster %s; active clusters are %v", m, names)
}

// HasRouteConfig checks Envoy has a dynamic route config that m matches.
func HasRouteConfig(dump *adminv3.ConfigDump, m Matcher[proto.Message]) error {
	for _, a := range dump.GetConfigs() {
		section := &adminv3.RoutesConfigDump{}
		if !a.MessageIs(section) {
			continue
		}
		if err := a.UnmarshalTo(section); err != nil {
			return err
		}
		for _, r := range section.GetDynamicRouteConfigs() {
			route := &routev3.RouteConfiguration{}
			if err := r.GetRouteConfig().UnmarshalTo(route); err != nil {
				return err
			}
			if m.Match(route) {
				return nil
			}
		}
	}
	return fmt.Errorf("no dynamic route config %s", m)
}
