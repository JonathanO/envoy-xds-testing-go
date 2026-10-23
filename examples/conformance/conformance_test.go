// Runs the conformance suite against upstream go-control-plane. Expect it to fail: on Envoy 1.40 with go-control-plane
// v0.14.0, only Reconnects passes on SotW, and Reconnects and ContainsRejectedConfig on delta.
//
// go-control-plane applies each snapshot one type at a time, in a fixed order, without waiting for Envoy to ACK, so it
// isn't make before break. In ADS mode it won't answer a request for a name that isn't in the snapshot. And on SotW it
// sends a rejected cluster again.
package conformance_test

import (
	"testing"

	"github.com/JonathanO/envoy-xds-testing-go/conformance"
	"github.com/JonathanO/envoy-xds-testing-go/examples"
)

func TestSotw(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Sotw,
		New:      func(*testing.T) conformance.ControlPlane { return examples.NewGoControlPlane() },
	})
}

func TestDelta(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Delta,
		New:      func(*testing.T) conformance.ControlPlane { return examples.NewGoControlPlane() },
	})
}
