//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	connect "connectrpc.com/connect"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
)

// loadhandlerInternalPort is the port cmd/loadhandler binds inside its
// container. Exposed as a constant for diagnostic clarity in test logs.
const loadhandlerInternalPort = "9100"

// HandlerContainer is a sidecar running cmd/loadhandler on the same
// docker network as the reflowd cluster. The engine dials it by DNS
// name (http://loadhandler:9100) so killing a reflowd node does not
// drop the handler — the precondition that unblocks the previously
// skipped TestChaos_LeaderSIGKILL flow once it ports to e2e.
type HandlerContainer struct {
	URL       string // http://loadhandler:9100 — what we register with the engine
	container testcontainers.Container

	mu         sync.Mutex
	terminated bool
}

// StartHandlerContainer brings up cmd/loadhandler attached to nw with
// alias `loadhandler`. Blocks until the listen port is reachable.
func StartHandlerContainer(t *testing.T, nw *testcontainers.DockerNetwork) *HandlerContainer {
	t.Helper()
	SkipUnlessDocker(t)
	image := LoadhandlerImage(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{loadhandlerInternalPort + "/tcp"},
		Cmd:          []string{"-addr", ":" + loadhandlerInternalPort},
		WaitingFor: wait.ForListeningPort(loadhandlerInternalPort + "/tcp").
			WithStartupTimeout(90 * time.Second),
	}
	gcr := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	if err := network.WithNetwork([]string{"loadhandler"}, nw)(&gcr); err != nil {
		t.Fatalf("e2e: handler network attach: %v", err)
	}
	c, err := testcontainers.GenericContainer(ctx, gcr)
	if err != nil {
		t.Fatalf("e2e: start loadhandler: %v", err)
	}
	h := &HandlerContainer{
		URL:       "http://loadhandler:" + loadhandlerInternalPort,
		container: c,
	}
	t.Cleanup(h.Close)
	return h
}

// Close terminates the handler container. Idempotent.
func (h *HandlerContainer) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.terminated || h.container == nil {
		return
	}
	_ = testcontainers.TerminateContainer(h.container)
	h.terminated = true
}

// RegisterHandler calls Config.RegisterDeployment against each node's
// host-mapped admin endpoint in turn until one returns success. The
// non-leader nodes return connect.CodeUnavailable with a docker-internal
// LeaderHint the test process can't resolve, so straightforward
// CallWithLeaderRedirect doesn't fit — round-robin is the simplest
// resilient option for the insecure smoke. mTLS clusters will install a
// sidecar that uses CallWithLeaderRedirect via in-network DNS.
//
// The handler URL we register is `h.URL` (http://loadhandler:9100), the
// docker-internal address; engine dials it from inside the network.
func RegisterHandler(ctx context.Context, cluster *ContainerCluster, h *HandlerContainer) error {
	if cluster == nil || h == nil {
		return errors.New("e2e: cluster and handler must be non-nil")
	}
	var lastErr error
	for hop := 0; hop < 10; hop++ {
		for _, node := range cluster.Nodes {
			if node == nil {
				continue
			}
			err := registerOnce(ctx, node.AdminURLForTest(), h.URL)
			if err == nil {
				return nil
			}
			lastErr = err
			// Only retry on Unavailable (not-leader); anything else is
			// terminal and we should surface it immediately.
			var cerr *connect.Error
			if !errors.As(err, &cerr) || cerr.Code() != connect.CodeUnavailable {
				return err
			}
		}
		// Brief pause between full sweeps lets a freshly-elected leader
		// settle if we caught the cluster mid-election.
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no eligible node")
	}
	return fmt.Errorf("e2e: register deployment exhausted retries: %w", lastErr)
}

// registerOnce dials the given admin URL (http://host:port) and issues
// one RegisterDeployment call. Returns Connect errors verbatim so the
// caller can route on connect.Code.
func registerOnce(ctx context.Context, adminURL, deploymentURL string) error {
	// reflowclient.Dial takes host:port + creds. Strip the scheme since
	// the dialer derives http:// for insecure / https:// for TLS.
	addr := stripScheme(adminURL)
	cli, err := reflowclient.Dial(ctx, reflowclient.DialOptions{Addr: addr})
	if err != nil {
		return fmt.Errorf("dial admin %s: %w", addr, err)
	}
	defer cli.Close()
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = cli.Config.RegisterDeployment(rctx, connect.NewRequest(&configv1.RegisterDeploymentRequest{
		Url: deploymentURL,
	}))
	return err
}

func stripScheme(u string) string {
	for _, p := range [...]string{"http://", "https://"} {
		if len(u) > len(p) && u[:len(p)] == p {
			return u[len(p):]
		}
	}
	return u
}
