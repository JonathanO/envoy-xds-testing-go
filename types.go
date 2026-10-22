package xdstest

// xDS type URLs.
const (
	LDS  = "type.googleapis.com/envoy.config.listener.v3.Listener"
	RDS  = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration"
	SRDS = "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration"
	VHDS = "type.googleapis.com/envoy.config.route.v3.VirtualHost"
	CDS  = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	EDS  = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment"
	SDS  = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.Secret"
	RTDS = "type.googleapis.com/envoy.service.runtime.v3.Runtime"
)

// IsWildcard reports whether Envoy subscribes to a type as a wildcard (all resources) by default: LDS and CDS.
func IsWildcard(typeURL string) bool {
	return typeURL == LDS || typeURL == CDS
}
