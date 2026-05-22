//go:build e2e

package e2e_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflow/internal/e2e"
)

// TestSmoke_SingleNodeReflowdBoots builds the Dockerfile.reflowd image,
// starts a one-node reflowd container with minimal env-var config, and
// asserts the ingress listener becomes reachable. Covers the image
// build path, the e2e doctor preflight, and the env-driven boot of
// `reflowd run` against a real container — the foundation every later
// e2e test stacks on.
func TestSmoke_SingleNodeReflowdBoots(t *testing.T) {
	e2e.SkipUnlessDocker(t)
	image := e2e.ReflowdImage(t)

	// 5min covers a cold pull on slow links; warm runs settle in well
	// under 30s. The wait strategy below is the per-readiness budget.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image: image,
		Cmd:   []string{"run"},
		Env: map[string]string{
			"REFLOW_NODE_ID":        "1",
			"REFLOW_NODE_RAFT_ADDR": "0.0.0.0:9001",
			"REFLOW_INGRESS_ADDR":   "0.0.0.0:8080",
			// Disable metrics so the test doesn't depend on Prometheus
			// scrape being ready; ingress liveness is what we assert.
			"REFLOW_METRICS_DISABLED": "true",
		},
		ExposedPorts: []string{"8080/tcp"},
		WaitingFor: wait.ForListeningPort("8080/tcp").
			WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start reflowd container: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("container host: %v", err)
	}
	port, err := c.MappedPort(ctx, "8080/tcp")
	if err != nil {
		t.Fatalf("mapped port: %v", err)
	}

	// Independent TCP dial proves we're not just trusting the wait
	// strategy: the host-mapped ingress port accepts a connection.
	addr := net.JoinHostPort(host, port.Port())
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial ingress %s: %v", addr, err)
	}
	_ = conn.Close()
}
