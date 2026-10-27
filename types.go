package xdstest

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

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

// IsWildcard reports whether Envoy subscribes to a type as a wildcard (all resources) by default: LDS, CDS, SRDS and VHDS.
func IsWildcard(typeURL string) bool {
	return typeURL == LDS || typeURL == CDS || typeURL == SRDS || typeURL == VHDS
}

// NameOf is the name Envoy knows a resource by: its name field, or cluster_name for a ClusterLoadAssignment. It's ""
// if the resource has neither.
func NameOf(resource proto.Message) string {
	fields := resource.ProtoReflect().Descriptor().Fields()
	for _, name := range []protoreflect.Name{"name", "cluster_name"} {
		if f := fields.ByName(name); f != nil && f.Kind() == protoreflect.StringKind {
			return resource.ProtoReflect().Get(f).String()
		}
	}
	return ""
}
