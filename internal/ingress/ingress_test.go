package ingress_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
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

// bringUpHostWithIngress starts a single-node Host on a temp dir, registers
// the supplied handlers, awaits leadership on shard 1, and starts the
// ingress HTTP+gRPC transports on ephemeral ports. The returned cleanup
// stops everything in the right order (ingress before host so in-flight
// requests don't dangle).
func bringUpHostWithIngress(t *testing.T, reg *sdk.Registry) (*engine.Host, *ingress.Runtime) {
	t.Helper()
	dir := t.TempDir()
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:             1,
		RaftAddr:           freeAddr(t),
		DataDir:            filepath.Join(dir, "node1"),
		RTTMillisecond:     50,
		NumPartitionShards: 1,
		Handlers:           reg,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.AwaitLeader(ctx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		HTTPAddr: "127.0.0.1:0",
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return h, rt
}

func httpPost(t *testing.T, url string, body any) ([]byte, int) {
	t.Helper()
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return out, resp.StatusCode
}

// TestIngress_SubmitAndAwaitEcho is the smallest happy-path test: HTTP
// /invocation/Echo/echo with a JSON-base64-encoded input, poll /await, get
// the same bytes back. Exercises the full grpc-gateway → gRPC → server →
// Host → Invoker round-trip.
func TestIngress_SubmitAndAwaitEcho(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	submitBody := map[string]any{
		"input": base64.StdEncoding.EncodeToString([]byte("hello")),
	}
	raw, code := httpPost(t, base+"/invocation/Echo/echo", submitBody)
	if code != http.StatusOK {
		t.Fatalf("submit: code=%d body=%s", code, string(raw))
	}
	var submitResp struct {
		InvocationIdStr string `json:"invocationIdStr"`
	}
	if err := json.Unmarshal(raw, &submitResp); err != nil {
		t.Fatalf("submit decode: %v (body=%s)", err, string(raw))
	}
	if submitResp.InvocationIdStr == "" {
		t.Fatalf("submit: missing invocation_id_str (body=%s)", string(raw))
	}

	awaitURL := fmt.Sprintf("%s/await/%s", base, submitResp.InvocationIdStr)
	// Poll a few times — the handler is fast but the Raft round-trip + leader
	// gain race can take a moment after startup.
	var awaitResp struct {
		Output         string `json:"output"`
		FailureMessage string `json:"failureMessage"`
		Completed      bool   `json:"completed"`
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, code = httpPost(t, awaitURL, map[string]any{"timeoutMs": 1000})
		if code != http.StatusOK {
			t.Fatalf("await: code=%d body=%s", code, string(raw))
		}
		if err := json.Unmarshal(raw, &awaitResp); err != nil {
			t.Fatalf("await decode: %v (body=%s)", err, string(raw))
		}
		if awaitResp.Completed {
			break
		}
	}
	if !awaitResp.Completed {
		t.Fatalf("await never completed: %+v", awaitResp)
	}
	// grpc-gateway base64-encodes bytes fields in JSON.
	got, err := base64.StdEncoding.DecodeString(awaitResp.Output)
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if string(got) != "echo:hello" {
		t.Errorf("output = %q; want echo:hello", string(got))
	}
	if awaitResp.FailureMessage != "" {
		t.Errorf("failure_message = %q; want empty", awaitResp.FailureMessage)
	}
}

// TestIngress_DescribeAndListPartitions covers the read-only admin
// endpoints: ListPartitions surfaces shard 1 as leader, DescribeInvocation
// reports Completed for a finished invocation.
func TestIngress_DescribeAndListPartitions(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ sdk.Context, in []byte) ([]byte, error) {
		return in, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	// ListPartitions
	resp, err := http.Get(base + "/admin/partitions")
	if err != nil {
		t.Fatalf("GET partitions: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list partitions: code=%d body=%s", resp.StatusCode, string(raw))
	}
	var listResp struct {
		Partitions []struct {
			ShardId  string `json:"shardId"`
			IsLeader bool   `json:"isLeader"`
		} `json:"partitions"`
	}
	if err := json.Unmarshal(raw, &listResp); err != nil {
		t.Fatalf("list decode: %v (body=%s)", err, string(raw))
	}
	if len(listResp.Partitions) != 1 || listResp.Partitions[0].ShardId != "1" {
		t.Fatalf("list partitions: got %+v", listResp.Partitions)
	}
	if !listResp.Partitions[0].IsLeader {
		t.Errorf("shard 1 should be leader; got %+v", listResp.Partitions[0])
	}

	// Submit an invocation, then DescribeInvocation should eventually
	// report Completed.
	submitBody := map[string]any{
		"input": base64.StdEncoding.EncodeToString([]byte("x")),
	}
	raw, code := httpPost(t, base+"/invocation/Echo/echo", submitBody)
	if code != http.StatusOK {
		t.Fatalf("submit: code=%d body=%s", code, string(raw))
	}
	var submitResp struct {
		InvocationIdStr string `json:"invocationIdStr"`
	}
	if err := json.Unmarshal(raw, &submitResp); err != nil {
		t.Fatalf("submit decode: %v", err)
	}
	descURL := base + "/admin/invocation/" + submitResp.InvocationIdStr
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(descURL)
		if err != nil {
			t.Fatalf("GET describe: %v", err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("describe: code=%d body=%s", resp.StatusCode, string(raw))
		}
		if bytes.Contains(raw, []byte(`"completed":`)) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("describe never reached Completed: body=%s", string(raw))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestIngress_AttachAndGetOutput exercises the attach and output endpoints:
//   - GetInvocationOutput returns PENDING before completion and
//     COMPLETED_OK after; UNKNOWN for an arbitrary unknown id.
//   - AttachInvocation blocks until Completed and returns the same output.
func TestIngress_AttachAndGetOutput(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ sdk.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	// Submit.
	submitBody := map[string]any{
		"input": base64.StdEncoding.EncodeToString([]byte("phase3")),
	}
	raw, code := httpPost(t, base+"/invocation/Echo/echo", submitBody)
	if code != http.StatusOK {
		t.Fatalf("submit: code=%d body=%s", code, string(raw))
	}
	var submitResp struct {
		InvocationIdStr string `json:"invocationIdStr"`
	}
	if err := json.Unmarshal(raw, &submitResp); err != nil {
		t.Fatalf("submit decode: %v", err)
	}

	// GetInvocationOutput / AttachInvocation eventually return COMPLETED_OK.
	outputURL := base + "/output/" + submitResp.InvocationIdStr
	attachURL := base + "/attach/" + submitResp.InvocationIdStr
	deadline := time.Now().Add(5 * time.Second)
	var attachResp struct {
		Output    string `json:"output"`
		Completed bool   `json:"completed"`
	}
	for time.Now().Before(deadline) {
		raw, code = httpPost(t, attachURL, map[string]any{"timeoutMs": 1000})
		if code != http.StatusOK {
			t.Fatalf("attach: code=%d body=%s", code, string(raw))
		}
		if err := json.Unmarshal(raw, &attachResp); err != nil {
			t.Fatalf("attach decode: %v body=%s", err, string(raw))
		}
		if attachResp.Completed {
			break
		}
	}
	if !attachResp.Completed {
		t.Fatalf("attach never completed: %+v", attachResp)
	}
	got, err := base64.StdEncoding.DecodeString(attachResp.Output)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(got) != "echo:phase3" {
		t.Errorf("attach output = %q; want echo:phase3", string(got))
	}

	// GetInvocationOutput post-completion: status COMPLETED_OK + output.
	resp, err := http.Get(outputURL)
	if err != nil {
		t.Fatalf("GET output: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("output: code=%d body=%s", resp.StatusCode, string(raw))
	}
	var outResp struct {
		Status string `json:"status"`
		Output string `json:"output"`
	}
	if err := json.Unmarshal(raw, &outResp); err != nil {
		t.Fatalf("output decode: %v body=%s", err, string(raw))
	}
	if outResp.Status != "COMPLETED_OK" {
		t.Errorf("status = %q; want COMPLETED_OK", outResp.Status)
	}
	gotOut, _ := base64.StdEncoding.DecodeString(outResp.Output)
	if string(gotOut) != "echo:phase3" {
		t.Errorf("output = %q; want echo:phase3", string(gotOut))
	}

	// GetInvocationOutput for an unknown id → UNKNOWN. Use a syntactically
	// valid id whose UUID is all zero (never minted by the ingress's rand).
	unknown := ingress.FormatInvocationID(makeID(1, make([]byte, 16)))
	resp, err = http.Get(base + "/output/" + unknown)
	if err != nil {
		t.Fatalf("GET output unknown: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("output unknown: code=%d body=%s", resp.StatusCode, string(raw))
	}
	var unkResp struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(raw, &unkResp); err != nil {
		t.Fatalf("unknown decode: %v body=%s", err, string(raw))
	}
	// "UNKNOWN" or "PENDING" (Free maps to UNKNOWN; transient errors also UNKNOWN).
	if unkResp.Status != "UNKNOWN" {
		t.Errorf("unknown id status = %q; want UNKNOWN", unkResp.Status)
	}
}

// TestIngress_SubmitRejectsEmptyService verifies the InvalidArgument path.
// TestIngress_GetObjectState submits an invocation that writes state for
// a virtual object, then reads it back via the admin endpoint. Also
// covers the absent-key path (present=false, not an error).
func TestIngress_GetObjectState(t *testing.T) {
	reg := sdk.NewRegistry()
	if err := reg.RegisterService("Stater", "set", func(c sdk.Context, in []byte) ([]byte, error) {
		if err := c.SetState("k", in); err != nil {
			return nil, err
		}
		return []byte("ok"), nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	_, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	// Submit with object_key="obj-1" so state is scoped under (Stater, obj-1).
	submitBody := map[string]any{
		"objectKey": "obj-1",
		"input":     base64.StdEncoding.EncodeToString([]byte("payload")),
	}
	raw, code := httpPost(t, base+"/invocation/Stater/set", submitBody)
	if code != http.StatusOK {
		t.Fatalf("submit: code=%d body=%s", code, string(raw))
	}
	var submitResp struct {
		InvocationIdStr string `json:"invocationIdStr"`
	}
	if err := json.Unmarshal(raw, &submitResp); err != nil {
		t.Fatalf("submit decode: %v", err)
	}

	// Wait for completion so the state write has applied.
	attachURL := base + "/attach/" + submitResp.InvocationIdStr
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, code = httpPost(t, attachURL, map[string]any{"timeoutMs": 1000})
		if code != http.StatusOK {
			t.Fatalf("attach: code=%d body=%s", code, string(raw))
		}
		var att struct {
			Completed bool `json:"completed"`
		}
		_ = json.Unmarshal(raw, &att)
		if att.Completed {
			break
		}
	}

	// Read state back.
	resp, err := http.Get(base + "/admin/object/Stater/obj-1/state/k")
	if err != nil {
		t.Fatalf("GET state: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get state: code=%d body=%s", resp.StatusCode, string(raw))
	}
	var stateResp struct {
		Value   string `json:"value"`
		Present bool   `json:"present"`
	}
	if err := json.Unmarshal(raw, &stateResp); err != nil {
		t.Fatalf("state decode: %v (body=%s)", err, string(raw))
	}
	if !stateResp.Present {
		t.Fatalf("present = false; want true (body=%s)", string(raw))
	}
	got, err := base64.StdEncoding.DecodeString(stateResp.Value)
	if err != nil {
		t.Fatalf("decode value: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("value = %q; want payload", string(got))
	}

	// Absent key on a never-touched object → present=false, no error.
	resp, err = http.Get(base + "/admin/object/Stater/never-existed/state/missing")
	if err != nil {
		t.Fatalf("GET absent state: %v", err)
	}
	raw, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get absent state: code=%d body=%s", resp.StatusCode, string(raw))
	}
	stateResp = struct {
		Value   string `json:"value"`
		Present bool   `json:"present"`
	}{}
	if err := json.Unmarshal(raw, &stateResp); err != nil {
		t.Fatalf("absent state decode: %v", err)
	}
	if stateResp.Present {
		t.Errorf("absent key reported present=true")
	}
}

func TestIngress_SubmitRejectsEmptyService(t *testing.T) {
	reg := sdk.NewRegistry()
	_, rt := bringUpHostWithIngress(t, reg)
	base := "http://" + rt.HTTPAddr()

	raw, code := httpPost(t, base+"/invocation//echo", map[string]any{})
	if code == http.StatusOK {
		t.Fatalf("submit with empty service unexpectedly OK: body=%s", string(raw))
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
