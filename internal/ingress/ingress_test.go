package ingress_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/admin"
	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/handler"
	"github.com/twinfer/reflow/pkg/ingressclient"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

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

		asrv, err := admin.NewServer(admin.Config{Host: h, Runner: h.MetadataRunner()})
		if err != nil {
			t.Fatalf("admin.NewServer: %v", err)
		}
		regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer regCancel()
		if _, err := asrv.AutoSeed(regCtx, "http://"+ln.Addr().String()); err != nil {
			t.Fatalf("AutoSeed: %v", err)
		}
	}

	rt, err := ingress.Start(context.Background(), h, ingress.Config{Addr: "127.0.0.1:0"})
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

	submitResp, err := cli.SubmitInvocation(context.Background(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service: "Echo",
		Handler: "echo",
		Input:   []byte("hello"),
	}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	idStr := submitResp.Msg.GetInvocationIdStr()
	if idStr == "" {
		t.Fatalf("submit: missing invocation_id_str")
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

// TestIngress_DescribeAndListPartitions covers the read-only admin
// endpoints: ListPartitions surfaces shard 1 as leader, DescribeInvocation
// reports Completed for a finished invocation.
func TestIngress_DescribeAndListPartitions(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return in, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, _, cli := bringUpHostWithIngress(t, reg)

	listResp, err := cli.ListPartitions(context.Background(), connect.NewRequest(&ingressv1.ListPartitionsRequest{}))
	if err != nil {
		t.Fatalf("ListPartitions: %v", err)
	}
	parts := listResp.Msg.GetPartitions()
	if len(parts) != 1 || parts[0].GetShardId() != 1 {
		t.Fatalf("ListPartitions: got %+v", parts)
	}
	if !parts[0].GetIsLeader() {
		t.Errorf("shard 1 should be leader; got %+v", parts[0])
	}

	submitResp, err := cli.SubmitInvocation(context.Background(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service: "Echo",
		Handler: "echo",
		Input:   []byte("x"),
	}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	idStr := submitResp.Msg.GetInvocationIdStr()
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

	submitResp, err := cli.SubmitInvocation(context.Background(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service: "Echo",
		Handler: "echo",
		Input:   []byte("phase3"),
	}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	idStr := submitResp.Msg.GetInvocationIdStr()

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

	submitResp, err := cli.SubmitInvocation(context.Background(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service:   "Stater",
		Handler:   "set",
		ObjectKey: "obj-1",
		Input:     []byte("payload"),
	}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	idStr := submitResp.Msg.GetInvocationIdStr()

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

// TestIngress_SubmitRejectsEmptyService verifies the InvalidArgument path.
func TestIngress_SubmitRejectsEmptyService(t *testing.T) {
	reg := handler.NewRegistry()
	_, _, cli := bringUpHostWithIngress(t, reg)

	_, err := cli.SubmitInvocation(context.Background(), connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service: "",
		Handler: "echo",
	}))
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
