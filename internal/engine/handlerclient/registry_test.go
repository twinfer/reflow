package handlerclient

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/twinfer/reflow/pkg/handler/wire"
	handlerv1 "github.com/twinfer/reflow/proto/handlerv1"
)

// fakeClient is a noop Client. Tracks Close calls so the URL-evict test
// can assert the stale instance is torn down.
type fakeClient struct {
	url    string
	mu     sync.Mutex
	closed int
}

func (c *fakeClient) Invoke(_ context.Context, _ wire.Route, _ *handlerv1.InvokeRequest) (*handlerv1.InvokeResponse, error) {
	return nil, errors.New("fakeClient: Invoke not used in registry tests")
}

func (c *fakeClient) Close() error {
	c.mu.Lock()
	c.closed++
	c.mu.Unlock()
	return nil
}

func (c *fakeClient) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// fakeDialer hands out fresh fakeClient instances and remembers the URLs
// it was asked to dial.
type fakeDialer struct {
	mu      sync.Mutex
	dialed  []string
	clients []*fakeClient
}

func (d *fakeDialer) dial(_, rawURL string) (Client, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	c := &fakeClient{url: rawURL}
	d.dialed = append(d.dialed, rawURL)
	d.clients = append(d.clients, c)
	return c, nil
}

func (d *fakeDialer) lastClient() *fakeClient {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.clients) == 0 {
		return nil
	}
	return d.clients[len(d.clients)-1]
}

// TestRegistry_GetCachesSameURL: repeated Get with same id+URL must
// reuse the cached Client.
func TestRegistry_GetCachesSameURL(t *testing.T) {
	r := NewRegistry()
	d := &fakeDialer{}
	r.Register("inproc", d.dial)

	first, err := r.Get("dep-1", "inproc://a")
	if err != nil {
		t.Fatalf("Get #1: %v", err)
	}
	second, err := r.Get("dep-1", "inproc://a")
	if err != nil {
		t.Fatalf("Get #2: %v", err)
	}
	if first != second {
		t.Fatalf("expected cached Client; got different instances")
	}
	if got := len(d.dialed); got != 1 {
		t.Errorf("dialer invoked %d times; want 1", got)
	}
}

// TestRegistry_GetEvictsOnURLChange: when the URL for the same
// deployment id changes (e.g. the embedded handler restarted on a new
// loopback port), the cached Client must be closed and replaced.
func TestRegistry_GetEvictsOnURLChange(t *testing.T) {
	r := NewRegistry()
	d := &fakeDialer{}
	r.Register("inproc", d.dial)

	first, err := r.Get("dep-1", "inproc://a")
	if err != nil {
		t.Fatalf("Get #1: %v", err)
	}
	firstFake := first.(*fakeClient)

	second, err := r.Get("dep-1", "inproc://b")
	if err != nil {
		t.Fatalf("Get #2: %v", err)
	}
	if first == second {
		t.Fatalf("expected new Client after URL change; got cached instance")
	}
	if got := firstFake.closeCount(); got != 1 {
		t.Errorf("stale client Close called %d times; want 1", got)
	}
	if got := len(d.dialed); got != 2 {
		t.Errorf("dialer invoked %d times; want 2", got)
	}
	if got := d.dialed[1]; got != "inproc://b" {
		t.Errorf("second dial URL = %q; want %q", got, "inproc://b")
	}
}

// TestRegistry_GetMissingScheme: an unregistered scheme must surface a
// clear error rather than dialing.
func TestRegistry_GetMissingScheme(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("dep-1", "missing://x"); err == nil {
		t.Fatal("expected error for unregistered scheme; got nil")
	}
}
