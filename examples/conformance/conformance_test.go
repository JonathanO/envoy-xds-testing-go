// Runs the conformance suite against upstream go-control-plane, with what it fails recorded as known deviations.
package conformance_test

import (
	"testing"

	"github.com/JonathanO/envoy-xds-testing-go/conformance"
	"github.com/JonathanO/envoy-xds-testing-go/examples"
)

// What go-control-plane v0.14.0 fails on Envoy 1.40. It applies each snapshot one type at a time, in a fixed order,
// without waiting for Envoy to ACK, and in ADS mode won't answer a request for a name that isn't in the snapshot.
const (
	notMakeBeforeBreak = "sends routes before Envoy has ACKed their clusters, and removes clusters still in use"
	unansweredRoutes   = "never answers Envoy's request for a route config that doesn't exist yet"
)

func TestSotw(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Sotw,
		New:      func(*testing.T) conformance.ControlPlane { return examples.NewGoControlPlane() },
		Skip: map[string]string{
			"StartsEmpty":            notMakeBeforeBreak,
			"AddsAndRemovesServices": notMakeBeforeBreak,
			"MovesRouteToNewCluster": notMakeBeforeBreak,
			"ContainsRejectedConfig": "sends the rejected cluster again",
		},
	})
}

func TestDelta(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Delta,
		New:      func(*testing.T) conformance.ControlPlane { return examples.NewGoControlPlane() },
		Skip: map[string]string{
			"StartsEmpty":            unansweredRoutes,
			"AddsAndRemovesServices": notMakeBeforeBreak,
			"MovesRouteToNewCluster": notMakeBeforeBreak,
		},
	})
}
