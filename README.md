# Envoy xDS testing for Go

Test an xDS control plane against a real Envoy, run in Docker with
[testcontainers-go](https://golang.testcontainers.org/). It works for any Go control plane, whichever xDS library it
uses: write your own scenarios ("service X loses an instance"), and check that Envoy ends up where it should, safely.

To check that an xDS library serves any given config correctly, use the conformance suite in
[envoy-xds-spec](https://github.com/JonathanO/envoy-xds-spec/blob/main/docs/CONFORMANCE.md) instead. It runs against a
small conformance server for any language, so every port of the library runs the same one.

These helpers are a Go port of the conformance harness in envoy-xds-spec. [`examples/`](examples) has them working
against upstream go-control-plane.

## Setup

```sh
go get github.com/JonathanO/envoy-xds-testing-go
```

You also need Docker, or another [container runtime](#container-runtimes). The protos are go-control-plane's
(`github.com/envoyproxy/go-control-plane/envoy`).

What's in it:

- `Recorder`: a gRPC stream interceptor that records each xDS stream to and from your server, as a `Conversation`.
- `Sotw(c)` / `Delta(c)`: checks on a recorded conversation.
- `StartEnvoy`: starts an Envoy container that keeps its log. It can fetch `Stats`, per-host `Clusters`, fetch a
  `ConfigDump`, and send a request through a listener with `HTTPStatus`.
- `Envoy.Healthy` / `Envoy.WarningsSince`: checks on Envoy itself. It is connected, rejected nothing, has nothing
  warming, timed out no initial fetches, and logged no warnings or errors.
- `Settled`, `SettledIdle`, `SettledAfter`: conditions to wait on until your control plane and Envoy have stopped
  talking.
- `WaitFor(timeout, check)`: polls a check until it passes.
- `Checkpoint`: marks a conversation, Envoy's log and its stats, and checks everything since in one call.
- `Envoy.RoutedVia`: a request answers 200 and went to the cluster you expect.
- Package `match`: composable matchers for resources, messages, the order things happened in, and what Envoy holds.

### Container runtimes

Anything [testcontainers-go supports](https://golang.testcontainers.org/system_requirements/docker/) should do:

- Docker Engine on Linux, or Docker Desktop on macOS or Windows: works as is.
- [Rancher Desktop](https://golang.testcontainers.org/system_requirements/rancher/): set its engine to dockerd (moby),
  not containerd, and on macOS the other settings on that page.
- [Podman](https://golang.testcontainers.org/system_requirements/using_podman/): set `DOCKER_HOST` to its
  Docker-compatible socket, e.g. `unix://$XDG_RUNTIME_DIR/podman/podman.sock`, and start it with
  `systemctl --user start podman.socket`. Where Ryuk can't start, as on macOS, run it privileged
  (`TESTCONTAINERS_RYUK_CONTAINER_PRIVILEGED=true`, or `ryuk.container.privileged=true` in
  `~/.testcontainers.properties`, with a rootful Podman machine), or disable it (`TESTCONTAINERS_RYUK_DISABLED=true`).

Envoy reaches your control plane at `host.testcontainers.internal`, through
[testcontainers-go's SSH tunnel to the host](https://golang.testcontainers.org/features/networking/#exposing-host-ports-to-the-container),
not Docker's own route to the host, which differs by platform. So it needs nothing more of the runtime than the rest of
testcontainers-go does, whether the runtime runs in a VM or not.

Every check returns an `error` rather than taking a `*testing.T`, so it works with `WaitFor` and with whatever
assertions you use.

## Testing your control plane

### Envoy's config

Write a bootstrap config for the Envoy under test. The example's
[`envoy-sotw.yaml`](examples/controlplane/testdata/envoy-sotw.yaml) and
[`envoy-delta.yaml`](examples/controlplane/testdata/envoy-delta.yaml) make a good start. It must:

- put the admin interface on `0.0.0.0:9901`;
- point ADS at `host.testcontainers.internal` (`xdstest.HostInternal`), on port `${ADS_PORT}`, which `StartEnvoy`
  fills in with `ADSPort`;
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
	// On a free port, so tests can run in parallel.
	lis, err := net.Listen("tcp", ":0")
	must(t, err)
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	bootstrap, err := os.ReadFile("testdata/envoy.yaml")
	must(t, err)
	envoy, err := xdstest.StartEnvoy(ctx, xdstest.EnvoyConfig{
		Image: "envoyproxy/envoy:v1.40-latest", Bootstrap: bootstrap, ADSPort: lis.Addr().(*net.TCPAddr).Port,
		ListenerPorts: []int{10000},
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
	checkpoint, err := xdstest.TakeCheckpoint(ctx, envoy, c)
	must(t, err)

	// Make a change the way your users would, and wait for it to settle.
	controlPlane.AddService("foo", "127.0.0.1:9901")
	must(t, xdstest.WaitFor(30*time.Second, xdstest.SettledAfter(c, checkpoint.Message, xdstest.Settled(c, xdstest.DefaultQuietPeriod))))

	must(t, checkpoint.Sane(ctx)) // the conversation, and Envoy's health and log, since the checkpoint
	must(t, envoy.RoutedVia(ctx, 10000, "foo", "foo"))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
```

For delta xDS, use `xdstest.Delta(c)` and set `api_type: DELTA_GRPC` in Envoy's config. `Checkpoint` and
`xdstest.ChecksFor(c)` work out which protocol a conversation uses.

### Waiting for things to settle

If your control plane can tell when a stream is idle, wait with `SettledIdle(c, idle)`. Otherwise
`Settled(c, quietPeriod)` judges from the recorded conversation alone: every response answered, and nothing sent
either way for the quiet period. After a change, wrap either one in `SettledAfter(c, mark, ...)`. When you start
waiting, the control plane may not have begun sending the change yet, and look idle.

### What the checks mean

- `TakeCheckpoint(ctx, envoy, c)`: marks the conversation, Envoy's log and its stats, so later checks only look at what
  happened since. Take one once Envoy has synced, and again after anything you did on purpose that Envoy warns about,
  like a disconnect or a NACK.
  - `Sane(ctx)`: all of the conversation checks below from the checkpoint on, then `EnvoyHealthy`.
  - `SaneAfterReconnect(ctx, before)`: the same for a conversation Envoy started by reconnecting. It allows Envoy before
    1.19 resending wildcard names. Given `before`, the conversation before it, it checks make before break from where
    Envoy left off; with nil, it skips that.
  - `EnvoyHealthy(ctx)`: `Envoy.Healthy` and `Envoy.WarningsSince`, both since the checkpoint.

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
- `Envoy.RoutedVia(ctx, listenerPort, virtualHost, cluster)`: a request to the virtual host answers 200 and went to that
  cluster, going by its `upstream_rq_total`. A 200 alone doesn't say which cluster answered.
- `WasMakeBeforeBreakAfter(before, from)`: make before break on a conversation Envoy started by reconnecting, starting
  from what it last ACKed on `before`. Envoy keeps its config across a disconnect.

### Matchers

Package `match` has matchers for your own expectations. Message matchers work the same for SotW and delta.

```go
// Envoy ACKed routes that no longer use bar before bar's cluster was removed.
err := match.InOrder(c,
	match.AckOf(match.Response(xdstest.RDS, match.Containing(match.RouteConfig(match.Not(match.RoutingTo("bar")))))),
	match.Response(xdstest.CDS, match.Removing("bar")))

// Wait for Envoy to reject something.
err = xdstest.WaitFor(10*time.Second, func() error {
	return match.HasMessage(c, match.Nack(xdstest.CDS, match.WithMessage("connect_timeout")))
})

// What Envoy holds.
dump, err := envoy.ConfigDump(ctx, "")
names, err := match.ActiveClusterNames(dump)
err = match.HasRouteConfig(dump, match.Routing("foo", "foo-v2"))
```

- Resources: `Cluster(...)`, `RouteConfig(...)`, `EndpointAssignment(...)`, with `Named`, `ClusterType`, `RoutingTo`,
  `Routing(virtualHost, cluster)` and `WithEndpoint(address, port)`.
- Messages: `Response(typeURL, ...)`, `Request(typeURL, ...)` and `Nack(typeURL, ...)` (`""` for any type), with
  `Containing`, `Removing`, `SubscribingTo`, `UnsubscribingFrom` (delta only) and `WithMessage`. For SotW, `Removing`
  matches a wildcard (LDS, CDS, SRDS, VHDS) response without the resource.
- Conversations: `HasMessage`, and `InOrder` with `AckOf`, Envoy's ACK of a matching response.
- Envoy: `ActiveClusters`, `ActiveClusterNames`, `HasActiveCluster` and `HasRouteConfig` on a config dump.
- `Not`, `AllOf` and `AnyOf` combine matchers, and `New` makes your own.

For anything else, look at what Envoy has: `envoy.ConfigDump(ctx, "?resource=dynamic_active_clusters")`, or
`envoy.Stats(ctx)`. Decoding a config dump needs every type in it linked in, so blank import the go-control-plane
packages for any extensions your config uses beyond the HTTP connection manager and router.

### Reconnects

To test a reconnect, stop your gRPC server and start a new one on the same port, with the same control plane and
`Recorder`. Envoy then opens a second conversation. Check it with `SaneAfterReconnect(ctx, before)`, passing the
conversation before it: Envoy keeps its config across the disconnect, so make before break carries on from there.

### Things to know

- Each Envoy start costs a few seconds, and the first, which pulls the image, much more. Add checks to the scenarios
  you have before adding new scenarios.
- Envoy connects to whichever `ADSPort` you give it, so tests on free ports can run in parallel.
- Take a `Checkpoint` after Envoy has synced. Then checks only look at what happened since.

## Examples

Each example in `examples` is its own module, with everything it needs, so you can copy one directory and start from
it; just drop the `replace` line from its `go.mod`. They need Docker. Run one from its directory:

```sh
cd examples/controlplane && go test ./...
```

Their tests run in parallel, each with its own Envoy; `go test -parallel 4 ./...` runs at most four at once.

- [`controlplane`](examples/controlplane): testing your own control plane, as in "A test" above.
