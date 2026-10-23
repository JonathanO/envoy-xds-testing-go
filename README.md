# Envoy xDS testing for Go

Test an xDS control plane against a real Envoy, run in Docker with
[testcontainers-go](https://golang.testcontainers.org/). It works for any Go control plane, whichever xDS library it
uses. There are two ways to use it:

- [Testing your control plane](#testing-your-control-plane): write your own scenarios ("service X loses an
  instance"), and check that Envoy ends up where it should, safely.
- [Conformance suite for xDS libraries](#conformance-suite-for-xds-libraries): check that a library serves any given
  config correctly.

This is a Go port of `envoy-xds-testing` from [java-envoy-xds](https://github.com/JonathanO/java-envoy-xds).
[`examples/`](examples) has both, working against upstream go-control-plane.

## Setup

```sh
go get github.com/JonathanO/envoy-xds-testing-go
```

You also need Docker. The protos are go-control-plane's (`github.com/envoyproxy/go-control-plane/envoy`).

What's in it:

- `Recorder`: a gRPC stream interceptor that records each xDS stream to and from your server, as a `Conversation`.
- `Sotw(c)` / `Delta(c)`: checks on a recorded conversation.
- `StartEnvoy`: starts an Envoy container that keeps its log. It can fetch `Stats`, fetch a `ConfigDump`, and send a
  request through a listener with `HTTPStatus`.
- `Envoy.Healthy` / `Envoy.WarningsSince`: checks on Envoy itself. It is connected, rejected nothing, has nothing
  warming, timed out no initial fetches, and logged no warnings or errors.
- `Settled`, `SettledIdle`, `SettledAfter`: conditions to wait on until your control plane and Envoy have stopped
  talking.
- `WaitFor(timeout, check)`: polls a check until it passes.
- Package `conformance`: the conformance suite.

Every check returns an `error` rather than taking a `*testing.T`, so it works with `WaitFor` and with whatever
assertions you use.

## Testing your control plane

### Envoy's config

Write a bootstrap config for the Envoy under test. The suite's
[`envoy-sotw.yaml`](conformance/envoy-sotw.yaml) and [`envoy-delta.yaml`](conformance/envoy-delta.yaml) make a good
start. It must:

- put the admin interface on `0.0.0.0:9901`;
- point ADS at `host.testcontainers.internal` (`xdstest.HostInternal`) on your gRPC server's port;
- bind any listener you send traffic through to `0.0.0.0`, or it can't be reached from the test;
- use the node id and cluster that your control plane expects.

To give Envoy an upstream to send traffic to, use its own admin port. Every `127.0.0.x:9901` address inside the
container reaches it, and `GET /` answers 200.

### A test

[`examples/controlplane`](examples/controlplane/controlplane_test.go) is this test, runnable, with go-control-plane
standing in for your control plane, for SotW and delta.

```go
func TestAddsAService(t *testing.T) {
	ctx := context.Background()
	controlPlane := newMyControlPlane()

	// Your server first, then Envoy. The other way round, Envoy backs off and can time out its first fetches.
	rec := &xdstest.Recorder{}
	server := grpc.NewServer(grpc.StreamInterceptor(rec.Intercept))
	controlPlane.Register(server)
	lis, err := net.Listen("tcp", ":6566")
	must(t, err)
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	bootstrap, err := os.ReadFile("testdata/envoy.yaml")
	must(t, err)
	envoy, err := xdstest.StartEnvoy(ctx, xdstest.EnvoyConfig{
		Image: "envoyproxy/envoy:v1.40-latest", Bootstrap: bootstrap, ADSPort: 6566, ListenerPorts: []int{10000},
	})
	must(t, err)
	t.Cleanup(func() { envoy.Terminate(context.Background()) })

	// Wait for the initial sync. Everything Envoy asked for must be answered well inside its 15s initial fetch timeout.
	must(t, xdstest.WaitFor(30*time.Second, func() error {
		if len(rec.Conversations()) == 0 {
			return errors.New("envoy hasn't connected")
		}
		return nil
	}))
	c := rec.Conversations()[0]
	must(t, xdstest.WaitFor(10*time.Second, func() error { return xdstest.Sotw(c).HadConversationsWith(xdstest.CDS) }))
	must(t, xdstest.WaitFor(10*time.Second, xdstest.Sotw(c).AnsweredEveryType))
	must(t, xdstest.WaitFor(30*time.Second, xdstest.Settled(c, xdstest.DefaultQuietPeriod)))
	messageMark, logMark := c.Size(), envoy.LogSize()

	// Make a change the way your users would, and wait for it to settle.
	controlPlane.AddService("foo", "127.0.0.1:9901")
	must(t, xdstest.WaitFor(30*time.Second, xdstest.SettledAfter(c, messageMark, xdstest.Settled(c, xdstest.DefaultQuietPeriod))))

	checks := xdstest.Sotw(c)
	must(t, checks.NothingSentUnasked(false))
	must(t, checks.AllWereAcked())
	must(t, checks.SentNoEmptyResponses(messageMark))
	must(t, checks.WasMakeBeforeBreak(messageMark))
	must(t, envoy.Healthy(ctx, nil))
	must(t, envoy.WarningsSince(logMark))
	status, err := envoy.HTTPStatus(ctx, 10000, "foo")
	must(t, err)
	if status != 200 {
		t.Errorf("status %d", status)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
```

For delta xDS, use `xdstest.Delta(c)` and set `api_type: DELTA_GRPC` in Envoy's config.

### Waiting for things to settle

If your control plane can tell when a stream is idle, wait with `SettledIdle(c, idle)`. Otherwise
`Settled(c, quietPeriod)` judges from the recorded conversation alone: every response answered, and nothing sent
either way for the quiet period. After a change, wrap either one in `SettledAfter(c, mark, ...)`. When you start
waiting, the control plane may not have begun sending the change yet, and look idle.

### What the checks mean

- `NothingSentUnasked`: every response is for a type, and resource names, that Envoy subscribed to.
- `AllWereAcked`: Envoy ACKed every response, and NACKed none.
- `AnsweredEveryType`: every type Envoy asked for got a response. Without one, Envoy waits out its initial fetch
  timeout, or forever if that is disabled. A request for a name that doesn't exist needs a response too.
- `SentNoEmptyResponses(mark)`: no empty response after the mark. An empty first response for a wildcard type is fine,
  and is why this takes a mark.
- `WasMakeBeforeBreak(mark)`: no route was sent before Envoy ACKed the clusters and endpoints it uses, and no cluster
  was removed while an ACKed route still used it. Start after the initial sync: Envoy orders its own startup.
- `UnsubscribedFrom(type, name)`: Envoy dropped a subscription, e.g. EDS for a cluster you removed. Wait for this
  before checking a removal has settled. Envoy unsubscribes after it ACKs the cluster removal.
- `Envoy.Healthy(ctx, baseline)`: Envoy is connected, has nothing warming, and rejected nothing and timed out no
  initial fetches since the baseline. Pass nil, or after a deliberate NACK, a `Stats` taken after it.
- `Envoy.WarningsSince(mark)`: no warnings or errors in Envoy's log since the mark. Take the mark after startup, and
  again after anything you did on purpose that Envoy warns about, like a disconnect.

For anything else, look at what Envoy has: `envoy.ConfigDump(ctx, "?resource=dynamic_active_clusters")`, or
`envoy.Stats(ctx)`. Decoding a config dump needs every type in it linked in, so blank import the go-control-plane
packages for any extensions your config uses beyond the HTTP connection manager and router.

### Reconnects

To test a reconnect, stop your gRPC server and start a new one on the same port, with the same control plane and
`Recorder`. Envoy then opens a second conversation. On that one, skip `WasMakeBeforeBreak`, because the checks can't
know what config Envoy kept. Envoy before 1.19 resends resource names for wildcard types when it reconnects, so use
`NothingSentUnasked(true)` there.

### Things to know

- Each Envoy start costs a few seconds, and the first, which pulls the image, much more. Add checks to the scenarios
  you have before adding new scenarios.
- Envoy connects to a fixed port, so don't run these tests in parallel.
- Take marks from `LogSize()` and `Size()`. Then checks only look at what happened since.

## Conformance suite for xDS libraries

Package `conformance` checks the layer that serves config to Envoy: given desired config, does a real Envoy get there
safely? It doesn't test how a control plane decides what to serve. For that, write your own scenarios as above.

Implement `conformance.ControlPlane` for your library, and run it once per Envoy version and protocol. Here it is for
go-control-plane; [`examples/conformance`](examples/conformance/conformance_test.go) is the runnable version.

```go
type goControlPlane struct {
	cache   cache.SnapshotCache
	server  server.Server
	version atomic.Int64
}

func (g *goControlPlane) Register(s *grpc.Server) {
	discoveryv3.RegisterAggregatedDiscoveryServiceServer(s, g.server)
}

// SetConfig replaces the whole desired config: Clusters, ClusterLoadAssignments and RouteConfigurations.
func (g *goControlPlane) SetConfig(resources []proto.Message) {
	// Group resources by type into a cache.NewSnapshot, with an entry for every type even if empty, then:
	g.cache.SetSnapshot(context.Background(), "test-envoy-0", snapshot)
}

func TestSotw(t *testing.T) {
	conformance.Run(t, conformance.Options{
		Image:    "envoyproxy/envoy:v1.40-latest",
		Protocol: conformance.Sotw,
		New: func(*testing.T) conformance.ControlPlane {
			c := cache.NewSnapshotCache(true, cache.IDHash{}, nil)
			return &goControlPlane{cache: c, server: server.NewServer(context.Background(), c, nil)}
		},
	})
}
```

Expect that example to fail: go-control-plane fails most of the suite. It applies a snapshot one type at a time
without waiting for ACKs, which breaks make before break. In ADS mode it doesn't answer a request for a name it doesn't
have. And on SotW it sends a rejected cluster again.

Implement `Idle() bool` too (`conformance.IdleReporter`) if your library can tell when its latest stream is idle, and
`Resync()` (`conformance.Resyncer`) if it can make Envoy reconnect.
The scenarios:

- `StartsEmpty`: no config at first. Envoy gets an answer for every type it asks for, including a route config that
  doesn't exist yet, then takes the first service.
- `AddsAndRemovesServices`: a route, its cluster and its endpoints added in one change, then removed in one change.
- `MovesRouteToNewCluster`: a route moved to a new cluster, and the old cluster removed, in one change.
- `ContainsRejectedConfig`: Envoy NACKs a bad cluster. The library mustn't send it again, not even as part of a later
  change, nor drop Envoy into a reconnect loop, and Envoy must keep serving its last good config. If it implements
  `conformance.Resyncer`, a fixed config must get through after `Resync()`.
- `Reconnects`: a service is removed while Envoy is disconnected. After it reconnects, traffic still flows and updates
  still work.

After each change, the suite checks that the conversation stayed sane. Every response was ACKed, nothing was sent
unasked, there were no empty responses, and changes were make before break. It also checks that Envoy is healthy and
logged no warnings. To record a known deviation, add the test's name to `Skip`, with the reason.

Envoy's config uses node id `test-envoy-0` and cluster `test-cluster`, and Envoy connects to port 6566 on the host.

## Examples

Each example in `examples` is its own module, with everything it needs, so you can copy one directory and start from
it; just drop the `replace` line from its `go.mod`. They need Docker. Run one from its directory:

```sh
cd examples/controlplane && go test ./...
```

- [`controlplane`](examples/controlplane): testing your own control plane, as in "A test" above.
- [`conformance`](examples/conformance): the conformance suite against go-control-plane. It's expected to fail.
