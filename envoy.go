package xdstest

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	adminv3 "github.com/envoyproxy/go-control-plane/envoy/admin/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/protobuf/encoding/protojson"

	// Types that Envoy's config dump refers to, so it can be decoded. Blank import any other extensions your config uses.
	_ "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	_ "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
)

// AdminPort is where Envoy's config must put its admin interface, on 0.0.0.0.
const AdminPort = 9901

// ADSPortPlaceholder is replaced in Envoy's bootstrap with EnvoyConfig.ADSPort, e.g. "port_value: ${ADS_PORT}".
const ADSPortPlaceholder = "${ADS_PORT}"

// HostInternal is the name Envoy's config uses to reach your control plane on the host, e.g. in the ADS cluster.
const HostInternal = testcontainers.HostInternal

// EnvoyConfig says how to run Envoy.
type EnvoyConfig struct {
	// Image is the Envoy image, e.g. "envoyproxy/envoy:v1.40-latest".
	Image string
	// Bootstrap is Envoy's YAML config. It must put the admin interface on 0.0.0.0:9901 and point ADS at
	// HostInternal on ADSPortPlaceholder, which is replaced with ADSPort.
	Bootstrap []byte
	// ADSPort is the port your control plane listens on, on the host. Start it before Envoy, or Envoy backs off and
	// can time out its initial fetches. Listen on ":0" and pass the port it got, so tests can run in parallel.
	ADSPort int
	// ListenerPorts are listener ports to reach from the test, e.g. with HTTPStatus. Bind those listeners to 0.0.0.0.
	ListenerPorts []int
	// Args are extra Envoy arguments, e.g. "-l", "debug".
	Args []string
}

// Envoy is a running Envoy container. It keeps Envoy's log.
type Envoy struct {
	container testcontainers.Container
	host      string
	mu        sync.Mutex
	log       []string
}

// StartEnvoy starts Envoy and waits for its admin interface. Old Envoy builds occasionally crash at startup, so it
// tries up to three times.
func StartEnvoy(ctx context.Context, cfg EnvoyConfig) (*Envoy, error) {
	var err error
	for range 3 {
		var e *Envoy
		if e, err = startEnvoy(ctx, cfg); err == nil {
			return e, nil
		}
	}
	return nil, err
}

func startEnvoy(ctx context.Context, cfg EnvoyConfig) (*Envoy, error) {
	e := &Envoy{}
	exposed := []string{fmt.Sprintf("%d/tcp", AdminPort)}
	for _, p := range cfg.ListenerPorts {
		exposed = append(exposed, fmt.Sprintf("%d/tcp", p))
	}
	bootstrap := bytes.ReplaceAll(cfg.Bootstrap, []byte(ADSPortPlaceholder), []byte(strconv.Itoa(cfg.ADSPort)))
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:           cfg.Image,
			Cmd:             append([]string{"-c", "/envoy.yaml"}, cfg.Args...),
			ExposedPorts:    exposed,
			HostAccessPorts: []int{cfg.ADSPort},
			Files:           []testcontainers.ContainerFile{{Reader: bytes.NewReader(bootstrap), ContainerFilePath: "/envoy.yaml", FileMode: 0o644}},
			WaitingFor:      wait.ForHTTP("/server_info").WithPort(fmt.Sprintf("%d/tcp", AdminPort)).WithStartupTimeout(time.Minute),
			LogConsumerCfg:  &testcontainers.LogConsumerConfig{Consumers: []testcontainers.LogConsumer{e}},
		},
		Started: true,
	})
	if err != nil {
		if container != nil {
			_ = container.Terminate(context.Background())
		}
		return nil, err
	}
	e.container = container
	if e.host, err = container.Host(ctx); err != nil {
		_ = container.Terminate(context.Background())
		return nil, err
	}
	return e, nil
}

// Accept implements testcontainers.LogConsumer.
func (e *Envoy) Accept(l testcontainers.Log) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for line := range strings.SplitSeq(strings.TrimRight(string(l.Content), "\n"), "\n") {
		e.log = append(e.log, line)
	}
}

// Terminate stops and removes the container.
func (e *Envoy) Terminate(ctx context.Context) error {
	return e.container.Terminate(ctx)
}

// Container is the underlying container, for anything this doesn't cover.
func (e *Envoy) Container() testcontainers.Container {
	return e.container
}

// LogSize is the number of log lines so far. Take it as a mark once Envoy has synced, to skip startup noise.
func (e *Envoy) LogSize() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.log)
}

// LogSince returns a copy of the log lines from fromLine on.
func (e *Envoy) LogSince(fromLine int) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.log[fromLine:]...)
}

// URL is the base URL of a port in the container, as reached from the test.
func (e *Envoy) URL(ctx context.Context, port int) (string, error) {
	mapped, err := e.container.MappedPort(ctx, fmt.Sprintf("%d/tcp", port))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%d", e.host, mapped.Num()), nil
}

// HTTPStatus sends GET / with the given Host header through one of Envoy's listeners and returns the status code.
func (e *Envoy) HTTPStatus(ctx context.Context, listenerPort int, virtualHost string) (int, error) {
	base, err := e.URL(ctx, listenerPort)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/", nil)
	if err != nil {
		return 0, err
	}
	req.Host = virtualHost
	req.Close = true
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

// RoutedVia checks that GET / with this Host header through one of Envoy's listeners answers 200 and went to this
// cluster, going by its upstream_rq_total. A 200 alone doesn't say which cluster answered. It retries for up to 5
// seconds, while Envoy's workers catch up with an ACKed change.
func (e *Envoy) RoutedVia(ctx context.Context, listenerPort int, virtualHost, cluster string) error {
	requests := "cluster." + cluster + ".upstream_rq_total"
	return WaitFor(5*time.Second, func() error {
		before, err := e.Stats(ctx)
		if err != nil {
			return err
		}
		status, err := e.HTTPStatus(ctx, listenerPort, virtualHost)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("%s: status %d, want 200", virtualHost, status)
		}
		after, err := e.Stats(ctx)
		if err != nil {
			return err
		}
		if after[requests] <= before[requests] {
			return fmt.Errorf("%s: request didn't go to cluster %s (%s stayed at %d)", virtualHost, cluster, requests, after[requests])
		}
		return nil
	})
}

// Stats returns Envoy's counters and gauges by name.
func (e *Envoy) Stats(ctx context.Context) (map[string]uint64, error) {
	body, err := e.admin(ctx, "/stats")
	if err != nil {
		return nil, err
	}
	stats := map[string]uint64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		name, value, ok := strings.Cut(scanner.Text(), ": ")
		if !ok {
			continue
		}
		if n, err := strconv.ParseUint(value, 10, 64); err == nil {
			stats[name] = n // histograms aren't numbers, and are left out
		}
	}
	return stats, scanner.Err()
}

// Clusters returns what Envoy has per host, from its /clusters admin page: the numeric values by everything before the
// last "::", e.g. "foo::127.0.0.2:9901::rq_total".
func (e *Envoy) Clusters(ctx context.Context) (map[string]uint64, error) {
	body, err := e.admin(ctx, "/clusters")
	if err != nil {
		return nil, err
	}
	values := map[string]uint64{}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		sep := strings.LastIndex(line, "::")
		if sep <= 0 {
			continue
		}
		if n, err := strconv.ParseUint(line[sep+2:], 10, 64); err == nil {
			values[line[:sep]] = n
		}
	}
	return values, scanner.Err()
}

// ConfigDump returns Envoy's config dump. query is added to the admin URL, e.g. "?resource=dynamic_active_clusters",
// or "" for everything. Decoding needs every type in the dump to be linked in: blank import the go-control-plane
// packages for any extensions your config uses.
func (e *Envoy) ConfigDump(ctx context.Context, query string) (*adminv3.ConfigDump, error) {
	body, err := e.admin(ctx, "/config_dump"+query)
	if err != nil {
		return nil, err
	}
	dump := &adminv3.ConfigDump{}
	return dump, protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, dump)
}

func (e *Envoy) admin(ctx context.Context, path string) ([]byte, error) {
	base, err := e.URL(ctx, AdminPort)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", path, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// Healthy checks that Envoy is connected to its control plane, has nothing warming, and rejected no updates and timed
// out no initial fetches (Envoy 1.24 on; older ones have no stat for it) since baseline, an earlier [Envoy.Stats] or
// nil for ever. Stats can trail an ACK a little, so it retries for up to 10 seconds.
func (e *Envoy) Healthy(ctx context.Context, baseline map[string]uint64) error {
	return WaitFor(10*time.Second, func() error {
		stats, err := e.Stats(ctx)
		if err != nil {
			return err
		}
		var problems []string
		for name, want := range map[string]uint64{"control_plane.connected_state": 1, "cluster_manager.warming_clusters": 0, "listener_manager.total_listeners_warming": 0} {
			if got, ok := stats[name]; !ok || got != want {
				problems = append(problems, fmt.Sprintf("%s=%d, want %d", name, got, want))
			}
		}
		for name, value := range stats {
			if (strings.HasSuffix(name, ".update_rejected") || strings.HasSuffix(name, ".init_fetch_timeout")) && value != baseline[name] {
				problems = append(problems, fmt.Sprintf("%s=%d", name, value-baseline[name]))
			}
		}
		if len(problems) > 0 {
			sort.Strings(problems)
			return fmt.Errorf("envoy unhealthy: %v", problems)
		}
		return nil
	})
}

var warning = regexp.MustCompile(`\]\[(warning|error|critical)\]\[`)

// WarningsSince checks that Envoy logged no warnings or errors from fromLine on, e.g. a [Envoy.LogSize] taken once
// Envoy has synced.
func (e *Envoy) WarningsSince(fromLine int) error {
	var found []string
	for _, line := range e.LogSince(fromLine) {
		if warning.MatchString(line) {
			found = append(found, line)
		}
	}
	if len(found) > 0 {
		return fmt.Errorf("envoy logged warnings or errors:\n%s", strings.Join(found, "\n"))
	}
	return nil
}
