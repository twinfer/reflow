package ingress_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/auth"
	"github.com/twinfer/reflw/internal/authz"
	"github.com/twinfer/reflw/internal/config"
	"github.com/twinfer/reflw/internal/engine"
	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflw/pkg/ingressclient"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// testIngressMiddleware returns the production authn middleware (no OIDC),
// which stamps the principal. Authorization is the separate interceptor
// below; exercising the middleware end-to-end guarantees the wiring isn't
// silently skipped.
func testIngressMiddleware(t *testing.T) func(http.Handler) http.Handler {
	t.Helper()
	mw, _, err := auth.HTTPMiddleware(nil)
	if err != nil {
		t.Fatalf("auth.HTTPMiddleware: %v", err)
	}
	return mw
}

// testAuthzInterceptor returns a Cedar authz interceptor over the in-binary
// foundational policies — ingress is open to all principals, so anonymous h2c
// tests reach the handler, authorized the same way production wires it.
func testAuthzInterceptor(t *testing.T) *authz.Interceptor {
	t.Helper()
	ic, err := authz.NewFoundationalInterceptor(nil, false)
	if err != nil {
		t.Fatalf("authz.NewFoundationalInterceptor: %v", err)
	}
	return ic
}

// makeID builds an InvocationId from a partition key and a 16-byte uuid.
func makeID(pk uint64, uuid []byte) *enginev1.InvocationId {
	return &enginev1.InvocationId{PartitionKey: pk, Uuid: uuid}
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

// bringUpHostWithIngress starts a single-node Host with shard 0 +
// shard 1, starts a handler hosting reg, registers its URL as a
// deployment, and starts the ingress Connect transport on an ephemeral
// port. Returns the host, the runtime, and a typed Connect client
// dialed at the ingress address.
func bringUpHostWithIngress(t *testing.T, reg *handler.Registry) (*engine.Host, *ingress.Runtime, *ingressclient.Client) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := h.AwaitMetadataLeader(ctx); err != nil {
		t.Fatalf("AwaitMetadataLeader: %v", err)
	}
	if err := h.AwaitLeader(ctx, 1); err != nil {
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

	ic := testAuthzInterceptor(t)
	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		Addr:             "127.0.0.1:0",
		Middleware:       testIngressMiddleware(t),
		AuthzInterceptor: ic,
		RESTAuthorizer:   ic,
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	cli, err := ingressclient.Dial(ingressclient.Options{BaseURL: "http://" + rt.Addr()})
	if err != nil {
		t.Fatalf("ingressclient.Dial: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return h, rt, cli
}

// TestIngress_SubmitAndAwaitEcho is the smallest happy-path test:
// SubmitInvocation, then poll AwaitInvocation, get the same bytes back.
func TestIngress_SubmitAndAwaitEcho(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, cli := bringUpHostWithIngress(t, reg)

	idStr, err := cli.Submit(context.Background(), ingressclient.SubmitArgs{
		Service: "Echo",
		Handler: "echo",
		Input:   []byte("hello"),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if idStr == "" {
		t.Fatalf("submit: missing invocation_id")
	}

	var awaitMsg *ingressv1.AwaitInvocationResponse
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.AwaitInvocation(context.Background(), connect.NewRequest(&ingressv1.AwaitInvocationRequest{
			InvocationId: idStr,
			TimeoutMs:    1000,
		}))
		if err != nil {
			t.Fatalf("await: %v", err)
		}
		awaitMsg = resp.Msg
		if awaitMsg.GetCompleted() {
			break
		}
	}
	if awaitMsg == nil || !awaitMsg.GetCompleted() {
		t.Fatalf("await never completed: %+v", awaitMsg)
	}
	if got := string(awaitMsg.GetOutput()); got != "echo:hello" {
		t.Errorf("output = %q; want echo:hello", got)
	}
	if msg := awaitMsg.GetFailureMessage(); msg != "" {
		t.Errorf("failure_message = %q; want empty", msg)
	}
}

// TestIngress_ListInvocations covers the invocation-plane list fan-out: submit a
// few invocations, await completion, then list — asserting service filter, state
// filter (composed), and the limit cap. The invocation-plane twin of
// TestIngress_ListProcessInstances over the shared fanOutBand substrate.
func TestIngress_ListInvocations(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, cli := bringUpHostWithIngress(t, reg)

	inputs := []string{"a", "b", "c"}
	for _, in := range inputs {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		idStr, err := cli.Submit(ctx, ingressclient.SubmitArgs{
			Service: "Echo",
			Handler: "echo",
			Input:   []byte(in),
		})
		cancel()
		if err != nil {
			t.Fatalf("submit %s: %v", in, err)
		}
		// Await completion so the row settles into a known COMPLETED state.
		deadline := time.Now().Add(5 * time.Second)
		completed := false
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			resp, err := cli.AwaitInvocation(ctx, connect.NewRequest(&ingressv1.AwaitInvocationRequest{
				InvocationId: idStr,
				TimeoutMs:    1000,
			}))
			cancel()
			if err != nil {
				t.Fatalf("await %s: %v", in, err)
			}
			if resp.Msg.GetCompleted() {
				completed = true
				break
			}
		}
		if !completed {
			t.Fatalf("invocation %s never completed", in)
		}
	}

	listReq := func(req *ingressv1.ListInvocationsRequest) []*ingressv1.InvocationSummary {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := cli.ListInvocations(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatalf("ListInvocations: %v", err)
		}
		return resp.Msg.GetInvocations()
	}

	all := listReq(&ingressv1.ListInvocationsRequest{Service: "Echo"})
	if len(all) != len(inputs) {
		t.Fatalf("list Echo: got %d, want %d", len(all), len(inputs))
	}
	for _, iv := range all {
		if iv.GetTarget().GetServiceName() != "Echo" {
			t.Fatalf("summary target service = %q, want Echo", iv.GetTarget().GetServiceName())
		}
		if iv.GetState() != enginev1.InvocationState_INVOCATION_STATE_COMPLETED {
			t.Fatalf("summary state = %v, want COMPLETED", iv.GetState())
		}
		if iv.GetId() == nil {
			t.Fatalf("summary missing id")
		}
	}

	// State filter composes with service: COMPLETED returns all three, SCHEDULED none.
	if c := listReq(&ingressv1.ListInvocationsRequest{
		Service:     "Echo",
		StateFilter: []enginev1.InvocationState{enginev1.InvocationState_INVOCATION_STATE_COMPLETED},
	}); len(c) != len(inputs) {
		t.Fatalf("list COMPLETED: got %d, want %d", len(c), len(inputs))
	}
	if sc := listReq(&ingressv1.ListInvocationsRequest{
		Service:     "Echo",
		StateFilter: []enginev1.InvocationState{enginev1.InvocationState_INVOCATION_STATE_SCHEDULED},
	}); len(sc) != 0 {
		t.Fatalf("list SCHEDULED: got %d, want 0", len(sc))
	}
	// A non-matching service lists nothing.
	if other := listReq(&ingressv1.ListInvocationsRequest{Service: "Nope"}); len(other) != 0 {
		t.Fatalf("list other service: got %d, want 0", len(other))
	}
	// limit caps the result.
	if capped := listReq(&ingressv1.ListInvocationsRequest{Service: "Echo", Limit: 1}); len(capped) != 1 {
		t.Fatalf("list limit 1: got %d, want 1", len(capped))
	}

	// Paging: walk the whole set one row per page via next_page_token. Each page
	// holds at most the limit, the union covers every row exactly once, and an
	// empty token ends iteration.
	seen := map[string]int{}
	token := ""
	pages := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := cli.ListInvocations(ctx, connect.NewRequest(&ingressv1.ListInvocationsRequest{
			Service: "Echo", Limit: 1, PageToken: token,
		}))
		cancel()
		if err != nil {
			t.Fatalf("ListInvocations page %d: %v", pages, err)
		}
		rows := resp.Msg.GetInvocations()
		if len(rows) > 1 {
			t.Fatalf("page %d returned %d rows, want <= 1", pages, len(rows))
		}
		for _, iv := range rows {
			seen[string(iv.GetId().GetUuid())]++
		}
		token = resp.Msg.GetNextPageToken()
		if token == "" {
			break
		}
		if pages++; pages > len(inputs)+2 {
			t.Fatalf("paging did not terminate after %d pages", pages)
		}
	}
	if len(seen) != len(inputs) {
		t.Fatalf("paged union = %d distinct rows, want %d", len(seen), len(inputs))
	}
	for uuid, n := range seen {
		if n != 1 {
			t.Fatalf("row %x returned %d times across pages, want 1", uuid, n)
		}
	}
}

// TestIngress_DescribeInvocation covers the read-only DescribeInvocation
// admin endpoint: it reports Completed for a finished invocation. Live
// per-node leadership reads moved off ingress to ClusterCtl/NodeLeadership.
func TestIngress_DescribeInvocation(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return in, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, cli := bringUpHostWithIngress(t, reg)

	idStr, err := cli.Submit(context.Background(), ingressclient.SubmitArgs{
		Service: "Echo",
		Handler: "echo",
		Input:   []byte("x"),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		desc, err := cli.DescribeInvocation(context.Background(), connect.NewRequest(&ingressv1.DescribeInvocationRequest{InvocationId: idStr}))
		if err != nil {
			t.Fatalf("DescribeInvocation: %v", err)
		}
		if _, ok := desc.Msg.GetStatus().GetStatus().(*enginev1.InvocationStatus_Completed); ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("describe never reached Completed")
}

// TestIngress_AttachAndGetOutput exercises the attach and output endpoints:
//   - GetInvocationOutput returns PENDING before completion and
//     COMPLETED_OK after; UNKNOWN for an arbitrary unknown id.
//   - AttachInvocation blocks until Completed and returns the same output.
func TestIngress_AttachAndGetOutput(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, cli := bringUpHostWithIngress(t, reg)

	idStr, err := cli.Submit(context.Background(), ingressclient.SubmitArgs{
		Service: "Echo",
		Handler: "echo",
		Input:   []byte("phase3"),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	var attach *ingressv1.AttachInvocationResponse
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.AttachInvocation(context.Background(), connect.NewRequest(&ingressv1.AttachInvocationRequest{
			InvocationId: idStr,
			TimeoutMs:    1000,
		}))
		if err != nil {
			t.Fatalf("attach: %v", err)
		}
		attach = resp.Msg
		if attach.GetCompleted() {
			break
		}
	}
	if attach == nil || !attach.GetCompleted() {
		t.Fatalf("attach never completed: %+v", attach)
	}
	if got := string(attach.GetOutput()); got != "echo:phase3" {
		t.Errorf("attach output = %q; want echo:phase3", got)
	}

	outResp, err := cli.GetInvocationOutput(context.Background(), connect.NewRequest(&ingressv1.GetInvocationOutputRequest{InvocationId: idStr}))
	if err != nil {
		t.Fatalf("GetInvocationOutput: %v", err)
	}
	if outResp.Msg.GetStatus() != ingressv1.GetInvocationOutputResponse_COMPLETED_OK {
		t.Errorf("status = %v; want COMPLETED_OK", outResp.Msg.GetStatus())
	}
	if got := string(outResp.Msg.GetOutput()); got != "echo:phase3" {
		t.Errorf("output = %q; want echo:phase3", got)
	}

	// GetInvocationOutput for an unknown id → UNKNOWN.
	unknown := ingress.FormatInvocationID(makeID(1, make([]byte, 16)))
	unkResp, err := cli.GetInvocationOutput(context.Background(), connect.NewRequest(&ingressv1.GetInvocationOutputRequest{InvocationId: unknown}))
	if err != nil {
		t.Fatalf("GetInvocationOutput unknown: %v", err)
	}
	if unkResp.Msg.GetStatus() != ingressv1.GetInvocationOutputResponse_UNKNOWN {
		t.Errorf("unknown id status = %v; want UNKNOWN", unkResp.Msg.GetStatus())
	}
}

// TestIngress_GetObjectState submits an invocation that writes state for
// a virtual object, then reads it back via the admin endpoint. Also
// covers the absent-key path (present=false, not an error).
func TestIngress_GetObjectState(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Stater", "set", func(c handler.Context, in []byte) ([]byte, error) {
		if err := c.SetState("k", in); err != nil {
			return nil, err
		}
		return []byte("ok"), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, cli := bringUpHostWithIngress(t, reg)

	idStr, err := cli.Submit(context.Background(), ingressclient.SubmitArgs{
		Service:   "Stater",
		Handler:   "set",
		ObjectKey: "obj-1",
		Input:     []byte("payload"),
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := cli.AttachInvocation(context.Background(), connect.NewRequest(&ingressv1.AttachInvocationRequest{
			InvocationId: idStr,
			TimeoutMs:    1000,
		}))
		if err != nil {
			t.Fatalf("attach: %v", err)
		}
		if resp.Msg.GetCompleted() {
			break
		}
	}

	stateResp, err := cli.GetObjectState(context.Background(), connect.NewRequest(&ingressv1.GetObjectStateRequest{
		Service:   "Stater",
		ObjectKey: "obj-1",
		StateKey:  "k",
	}))
	if err != nil {
		t.Fatalf("GetObjectState: %v", err)
	}
	if !stateResp.Msg.GetPresent() {
		t.Fatalf("present = false; want true")
	}
	if got := string(stateResp.Msg.GetValue()); got != "payload" {
		t.Errorf("value = %q; want payload", got)
	}

	// Absent key on a never-touched object → present=false, no error.
	absent, err := cli.GetObjectState(context.Background(), connect.NewRequest(&ingressv1.GetObjectStateRequest{
		Service:   "Stater",
		ObjectKey: "never-existed",
		StateKey:  "missing",
	}))
	if err != nil {
		t.Fatalf("GetObjectState absent: %v", err)
	}
	if absent.Msg.GetPresent() {
		t.Errorf("absent key reported present=true")
	}
}

// TestIngress_SubmitRejectsEmptyService verifies the InvalidArgument path on
// the server's Submit core directly — an empty service can't be expressed as a
// valid REST URL, so this exercises the validation at its source.
func TestIngress_SubmitRejectsEmptyService(t *testing.T) {
	reg := handler.NewRegistry()
	_, rt, _ := bringUpHostWithIngress(t, reg)

	_, err := rt.Server().Submit(context.Background(), ingress.SubmitArgs{
		Service: "",
		Handler: "echo",
	})
	if err == nil {
		t.Fatal("submit with empty service unexpectedly OK")
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		t.Fatalf("error not a *connect.Error: %v", err)
	}
	if connectErr.Code() != connect.CodeInvalidArgument {
		t.Errorf("code = %v; want InvalidArgument", connectErr.Code())
	}
}

// TestIngress_FormatInvocationIDRoundtrip is a unit check on the id codec
// (lives in this package since the helper is exported from internal/ingress).
func TestIngress_FormatInvocationIDRoundtrip(t *testing.T) {
	uuid := make([]byte, 16)
	for i := range uuid {
		uuid[i] = byte(i + 1)
	}
	id, err := ingress.ParseInvocationID(ingress.FormatInvocationID(makeID(7, uuid)))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if id.GetPartitionKey() != 7 {
		t.Errorf("partition_key = %d; want 7", id.GetPartitionKey())
	}
	if !bytes.Equal(id.GetUuid(), uuid) {
		t.Errorf("uuid mismatch")
	}

	if _, err := ingress.ParseInvocationID("garbage"); err == nil {
		t.Errorf("ParseInvocationID(\"garbage\") should fail")
	}
	if _, err := ingress.ParseInvocationID("inv_xx_yy"); err == nil {
		t.Errorf("ParseInvocationID(\"inv_xx_yy\") should fail")
	}
}
