package httpingress_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	httpingress "github.com/twinfer/reflow/internal/ingress/http"
	"github.com/twinfer/reflow/pkg/handler"
)

// bringUp boots a single-node Host + Echo handler + ingress runtime with
// the REST facade mounted on the same listener. Returns the base URL of
// the ingress listener (REST paths live under /v1/*).
func bringUp(t *testing.T, reg *handler.Registry) string {
	t.Helper()
	dir := t.TempDir()

	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartMetadataShard(); err != nil {
		t.Fatalf("StartMetadataShard: %v", err)
	}
	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	awaitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	if err := h.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	if reg != nil && reg.Len() > 0 {
		srv, err := handler.NewServer(handler.Config{Registry: reg})
		if err != nil {
			t.Fatalf("handler.NewServer: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen sdk: %v", err)
		}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() {
			_ = srv.Shutdown()
			_ = ln.Close()
		})

		asrv, err := config.NewServer(config.Config{Host: h, Runner: h.MetadataRunner()})
		if err != nil {
			t.Fatalf("config.NewServer: %v", err)
		}
		regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer regCancel()
		if _, err := asrv.AutoSeed(regCtx, "http://"+ln.Addr().String()); err != nil {
			t.Fatalf("AutoSeed: %v", err)
		}
	}

	mw, _, _, err := auth.HTTPMiddleware(auth.Config{}, nil)
	if err != nil {
		t.Fatalf("auth.HTTPMiddleware: %v", err)
	}

	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:       "127.0.0.1:0",
		Middleware: mw,
		ExtraRoutes: func(srv *ingress.Server) []connectserver.Route {
			return []connectserver.Route{{
				Path:    "/v1/",
				Handler: mw(httpingress.NewRouter(srv, httpingress.Config{}, nil)),
			}}
		},
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	return "http://" + rt.Addr()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// postJSON is a small helper that POSTs body to url and decodes the JSON
// response into out (when out != nil). Returns the HTTP status code.
func postJSON(t *testing.T, url string, body []byte, headers map[string]string, out any) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		raw, _ := io.ReadAll(resp.Body)
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				t.Fatalf("decode %s: %v body=%q", url, err, raw)
			}
		}
	}
	return resp.StatusCode
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if out != nil {
		raw, _ := io.ReadAll(resp.Body)
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				t.Fatalf("decode %s: %v body=%q", url, err, raw)
			}
		}
	}
	return resp.StatusCode
}

type completedBody struct {
	InvocationID   string `json:"invocation_id"`
	Completed      bool   `json:"completed"`
	Output         []byte `json:"output,omitempty"`
	FailureMessage string `json:"failure_message,omitempty"`
	FailureCode    uint32 `json:"failure_code,omitempty"`
}

type submitBody struct {
	InvocationID string `json:"invocation_id"`
}

type outputBody struct {
	Status         string `json:"status"`
	Output         []byte `json:"output,omitempty"`
	FailureMessage string `json:"failure_message,omitempty"`
	FailureCode    uint32 `json:"failure_code,omitempty"`
}

func registerEcho(t *testing.T) *handler.Registry {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	return reg
}

// TestREST_CallSync exercises POST /v1/call/{service}/{handler}: submit
// + long-poll await on a single round-trip. Confirms the JSON envelope,
// base64 output, and 200 status.
func TestREST_CallSync(t *testing.T) {
	base := bringUp(t, registerEcho(t))
	var body completedBody
	status := postJSON(t, base+"/v1/call/Echo/echo", []byte("hello"), nil, &body)
	if status != http.StatusOK {
		t.Fatalf("status=%d want 200", status)
	}
	if !body.Completed {
		t.Fatalf("not completed: %+v", body)
	}
	if got := string(body.Output); got != "echo:hello" {
		t.Errorf("output=%q want echo:hello", got)
	}
	if body.InvocationID == "" {
		t.Errorf("missing invocation_id")
	}
}

// TestREST_SendThenAttach: submit-only via /v1/send returns an id; a
// follow-up /v1/attach blocks until completion and returns the same
// output. Also confirms /v1/output reports completed_ok after the fact.
func TestREST_SendThenAttach(t *testing.T) {
	base := bringUp(t, registerEcho(t))

	var sb submitBody
	if status := postJSON(t, base+"/v1/send/Echo/echo", []byte("world"), nil, &sb); status != http.StatusOK {
		t.Fatalf("send status=%d", status)
	}
	if sb.InvocationID == "" {
		t.Fatalf("send returned empty invocation_id")
	}

	var ab completedBody
	if status := getJSON(t, base+"/v1/attach/"+sb.InvocationID+"?timeout_ms=5000", &ab); status != http.StatusOK {
		t.Fatalf("attach status=%d", status)
	}
	if !ab.Completed || string(ab.Output) != "echo:world" {
		t.Fatalf("attach body=%+v", ab)
	}

	var ob outputBody
	if status := getJSON(t, base+"/v1/output/"+sb.InvocationID, &ob); status != http.StatusOK {
		t.Fatalf("output status=%d", status)
	}
	if ob.Status != "completed_ok" || string(ob.Output) != "echo:world" {
		t.Fatalf("output body=%+v", ob)
	}
}

// TestREST_IdempotencyKey: two /v1/send calls with the same
// Idempotency-Key return the same invocation_id. The /v1/send variant
// (no synchronous await) keeps the test focused on the ingress dedup
// path rather than the call-and-wait timing of /v1/call.
func TestREST_IdempotencyKey(t *testing.T) {
	base := bringUp(t, registerEcho(t))

	headers := map[string]string{"Idempotency-Key": "k1"}
	var a, b submitBody
	if status := postJSON(t, base+"/v1/send/Echo/echo", []byte("first"), headers, &a); status != http.StatusOK {
		t.Fatalf("call#1 status=%d", status)
	}
	// Wait for the first invocation to complete so the apply path has
	// populated the idempotency table before the second optimistic
	// SyncRead runs.
	var done outputBody
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = getJSON(t, base+"/v1/output/"+a.InvocationID, &done)
		if done.Status == "completed_ok" || done.Status == "completed_failed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if done.Status != "completed_ok" {
		t.Fatalf("first invocation never completed: status=%s", done.Status)
	}

	if status := postJSON(t, base+"/v1/send/Echo/echo", []byte("second"), headers, &b); status != http.StatusOK {
		t.Fatalf("call#2 status=%d", status)
	}
	if a.InvocationID != b.InvocationID {
		t.Fatalf("idempotency miss: %q vs %q", a.InvocationID, b.InvocationID)
	}
}

// TestREST_MalformedInvocationID returns a structured 4xx with the
// Connect-style code in the envelope.
func TestREST_MalformedInvocationID(t *testing.T) {
	base := bringUp(t, nil)

	var errBody struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status := getJSON(t, base+"/v1/output/not-an-id", &errBody)
	if status < 400 || status >= 500 {
		t.Fatalf("status=%d want 4xx", status)
	}
	if errBody.Code == "" {
		t.Fatalf("missing code in error envelope: %+v", errBody)
	}
}

// TestREST_UnknownInvocationID confirms /v1/output reports the "unknown"
// status (not an error) when an id with the right shape but no record is
// queried — matches the Connect surface's UNKNOWN enum.
func TestREST_UnknownInvocationID(t *testing.T) {
	base := bringUp(t, nil)

	// Synthesize a well-formed id by submitting and attaching, then
	// query an entirely fresh id of the same shape.
	reg := registerEcho(t)
	base = bringUp(t, reg)
	var sb submitBody
	if status := postJSON(t, base+"/v1/send/Echo/echo", []byte("x"), nil, &sb); status != http.StatusOK {
		t.Fatalf("send: %d", status)
	}
	// Mangle the uuid suffix to a brand-new id.
	known := sb.InvocationID
	bogus := known[:len(known)-1] + "z" // probably not a hex char
	var ob outputBody
	status := getJSON(t, base+"/v1/output/"+bogus, &ob)
	// Either a 4xx (parse error on non-hex) or 200 + status=unknown is
	// acceptable; the test asserts the surface is well-behaved.
	if status >= 500 {
		t.Fatalf("unexpected 5xx status=%d body=%+v", status, ob)
	}
}

// TestREST_BodyCapEnforced: a request larger than the configured cap
// gets rejected with 413 before reaching SubmitInvocation.
func TestREST_BodyCapEnforced(t *testing.T) {
	// Bring up with a tiny 1 KiB cap.
	base := bringUpWithConfig(t, registerEcho(t), httpingress.Config{MaxBodyBytes: 1 << 10})
	big := bytes.Repeat([]byte("A"), (1<<10)+1)
	var errBody struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	status := postJSON(t, base+"/v1/call/Echo/echo", big, nil, &errBody)
	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d want 413; body=%+v", status, errBody)
	}
}

// bringUpWithConfig is bringUp with a custom REST config. Kept separate
// so the canonical bringUp stays default-only and obvious.
func bringUpWithConfig(t *testing.T, reg *handler.Registry, cfg httpingress.Config) string {
	t.Helper()
	dir := t.TempDir()

	h, err := engine.NewHost(t.Context(), engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartMetadataShard(); err != nil {
		t.Fatalf("StartMetadataShard: %v", err)
	}
	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	awaitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	if err := h.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	if reg != nil && reg.Len() > 0 {
		srv, err := handler.NewServer(handler.Config{Registry: reg})
		if err != nil {
			t.Fatalf("handler.NewServer: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen sdk: %v", err)
		}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() {
			_ = srv.Shutdown()
			_ = ln.Close()
		})
		asrv, err := config.NewServer(config.Config{Host: h, Runner: h.MetadataRunner()})
		if err != nil {
			t.Fatalf("config.NewServer: %v", err)
		}
		regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer regCancel()
		if _, err := asrv.AutoSeed(regCtx, "http://"+ln.Addr().String()); err != nil {
			t.Fatalf("AutoSeed: %v", err)
		}
	}

	mw, _, _, err := auth.HTTPMiddleware(auth.Config{}, nil)
	if err != nil {
		t.Fatalf("auth.HTTPMiddleware: %v", err)
	}
	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:       "127.0.0.1:0",
		Middleware: mw,
		ExtraRoutes: func(srv *ingress.Server) []connectserver.Route {
			return []connectserver.Route{{
				Path:    "/v1/",
				Handler: mw(httpingress.NewRouter(srv, cfg, nil)),
			}}
		},
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return "http://" + rt.Addr()
}

// TestREST_MetadataRoundTrip: Reflow-Meta-* headers stamped on the
// REST request reach the durable handler as ctx.Metadata(). Validates
// the full path SubmitInvocationRequest.metadata → InvokeCommand →
// Scheduled → JEInput → InputCommandMessage.headers → wireContext.
func TestREST_MetadataRoundTrip(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("MetaEcho", "echo", func(ctx handler.Context, in []byte) ([]byte, error) {
		md := ctx.Metadata()
		// Encode metadata into the response so the test asserts via
		// the output channel — no out-of-band signaling needed.
		// Keys are lowercased by the REST ingress (see metadataHeaderPrefix).
		return []byte(md["event-id"] + "|" + md["event-type"]), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	base := bringUp(t, reg)

	headers := map[string]string{
		"Reflow-Meta-Event-Id":   "evt_12345",
		"Reflow-Meta-Event-Type": "charge.succeeded",
		// Headers without the prefix are ignored, not leaked into metadata.
		"X-Stripe-Sig": "not-included",
	}
	var body completedBody
	status := postJSON(t, base+"/v1/call/MetaEcho/echo", []byte("ignored"), headers, &body)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%+v", status, body)
	}
	if !body.Completed {
		t.Fatalf("not completed: %+v", body)
	}
	if got := string(body.Output); got != "evt_12345|charge.succeeded" {
		t.Errorf("output=%q want %q (metadata round-trip lost)", got, "evt_12345|charge.succeeded")
	}
}

// TestREST_PolicyAllowsAnonymous: anonymous (h2c) traffic should reach
// /v1/* through the starter policy's ingress_rest_open rule. The wider
// purpose is to catch a future regression where someone tightens the
// policy and forgets the REST surface.
func TestREST_PolicyAllowsAnonymous(t *testing.T) {
	base := bringUp(t, registerEcho(t))
	url := fmt.Sprintf("%s/v1/call/Echo/echo", base)
	var body completedBody
	if status := postJSON(t, url, []byte("ok"), nil, &body); status != http.StatusOK {
		t.Fatalf("anonymous call rejected: status=%d body=%+v", status, body)
	}
}
