// Package xdstest tests an xDS control plane against a real Envoy, run in Docker with testcontainers-go.
//
// Record the xDS conversation with a [Recorder], check it with [Sotw] or [Delta], wait for it to go quiet with
// [Settled], and check Envoy itself with [Envoy.Healthy] and [Envoy.WarningsSince].
//
// Every check returns an error rather than taking a *testing.T, so use them with [WaitFor] or with your own
// assertions.
package xdstest
