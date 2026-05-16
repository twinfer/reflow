package engine_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/admin"
	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/internal/storage/tables"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerAwakeable exercises ctx.Awakeable end-to-end.
//
//   - First invocation (known_entries=1, just JEInput):
//     1. Mint awakeable id deterministically from the StartMessage's
//     partition_key.
//     2. Emit AwakeableCommandMessage(slot=1, id).
//     3. Emit SetStateCommandMessage(key="awk_id", value=id) so the
//     test can read the id from StateTable to drive ResolveAwakeable.
//     4. Emit SuspensionMessage waiting on completion=2.
//   - Respawn (known_entries=4: Input+Awakeable+AwakeableResult+SetState):
//     read the resolved value off the SignalNotificationMessage and
//     emit OutputCommandMessage carrying it.
type fakeHandlerAwakeable struct {
	objectKey string
}

func (f *fakeHandlerAwakeable) discoveryBody(t *testing.T) []byte {
	t.Helper()
	resp := &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "Waiter", Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{"await"}},
		},
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal DiscoveryResponse: %v", err)
	}
	return body
}

func (f *fakeHandlerAwakeable) handler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/discover":
			w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
			_, _ = w.Write(f.discoveryBody(t))
			return
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/invoke/"):
			f.serveInvoke(t, w, r)
			return
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fakeHandlerAwakeable) serveInvoke(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()

	startFrame, err := readFrame(r.Body)
	if err != nil {
		http.Error(w, "read start: "+err.Error(), http.StatusBadRequest)
		return
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		http.Error(w, "decode StartMessage: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Scan replay frames for the awakeable id (in the AwakeableCommandMessage)
	// and the resolved value (in SignalNotificationMessage).
	var (
		resolvedValue []byte
		resolvedID    string
		resolved      bool
	)
	for range sm.GetKnownEntries() {
		f, err := readFrame(r.Body)
		if err != nil {
			http.Error(w, "read replay frame: "+err.Error(), http.StatusBadRequest)
			return
		}
		tc, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch tc {
		case handlerclient.TypeCmdAwakeable:
			var ac protocolv1.AwakeableCommandMessage
			if err := proto.Unmarshal(f.GetPayload(), &ac); err == nil {
				resolvedID = ac.GetAwakeableId()
			}
		case handlerclient.TypeNoteSignal:
			var sn protocolv1.SignalNotificationMessage
			if err := proto.Unmarshal(f.GetPayload(), &sn); err == nil {
				if v, ok := sn.GetResult().(*protocolv1.SignalNotificationMessage_Value); ok {
					resolvedValue = v.Value.GetContent()
					resolved = true
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/vnd.reflow.invocation.v1+protobuf")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	if !resolved {
		// First invocation. Mint the awakeable id, journal it, surface
		// via SetState, then suspend.
		id, err := mintAwakeable(sm.GetPartitionKey())
		if err != nil {
			http.Error(w, "mint id: "+err.Error(), http.StatusInternalServerError)
			return
		}
		akCmd := &protocolv1.AwakeableCommandMessage{
			ResultCompletionId: 2,
			AwakeableId:        id,
		}
		akPayload, _ := proto.Marshal(akCmd)
		_ = writeFrame(w, handlerclient.TypeCmdAwakeable, akPayload)
		flusher.Flush()

		setCmd := &protocolv1.SetStateCommandMessage{
			Key:   []byte("awk_id"),
			Value: &protocolv1.Value{Content: []byte(id)},
		}
		setPayload, _ := proto.Marshal(setCmd)
		_ = writeFrame(w, handlerclient.TypeCmdSetState, setPayload)
		flusher.Flush()

		sus := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
		susPayload, _ := proto.Marshal(sus)
		_ = writeFrame(w, handlerclient.TypeSuspension, susPayload)
		flusher.Flush()
		_, _ = io.Copy(io.Discard, r.Body)
		return
	}

	_ = resolvedID
	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: resolvedValue},
		},
	}
	outPayload, _ := proto.Marshal(out)
	_ = writeFrame(w, handlerclient.TypeCmdOutput, outPayload)
	flusher.Flush()
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	_ = writeFrame(w, handlerclient.TypeEnd, endPayload)
	flusher.Flush()
	_, _ = io.Copy(io.Discard, r.Body)
}

// mintAwakeable mirrors pkg/sdk/server.mintAwakeableID for the test
// fixture so we don't have to export the helper out of the public SDK
// surface.
func mintAwakeable(ownerPartitionKey uint64) (string, error) {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], ownerPartitionKey)
	if _, err := rand.Read(buf[8:]); err != nil {
		return "", fmt.Errorf("awakeable id rng: %w", err)
	}
	return "awk_" + base64.RawURLEncoding.EncodeToString(buf[:]), nil
}

// TestWireDispatch_HTTP2_Awakeable drives the full Awakeable
// suspend → external resolution → respawn → completion cycle. End-to-end:
//
//  1. Wire handler emits AwakeableCommandMessage + SetStateCommandMessage
//     (publishing the id to StateTable) + SuspensionMessage.
//  2. Test reads the id from StateTable.
//  3. Test sets up the ingress server and calls ResolveAwakeable(id,
//     "wakeup-value").
//  4. Engine writes JEAwakeableResult; status transitions Suspended →
//     Invoked; new wire session opens with the SignalNotificationMessage
//     in replay.
//  5. Handler emits OutputCommandMessage carrying "wakeup-value".
//  6. Assert completion.Output == "wakeup-value".
func TestWireDispatch_HTTP2_Awakeable(t *testing.T) {
	const wantOutput = "wakeup-value"

	fake := &fakeHandlerAwakeable{objectKey: "user-1"}
	addr, teardown := startFakeHandlerHTTP2WithHandler(t, fake.handler(t))
	defer teardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{
		N: 3,
	})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host

	srv, err := admin.NewServer(admin.Config{
		Host:   host,
		Runner: host.MetadataRunner(),
	})
	if err != nil {
		t.Fatalf("admin.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	regResp, err := callRegisterDeployment(regCtx, srv, "http://"+addr)
	if err != nil {
		t.Fatalf("RegisterDeployment: %v", err)
	}

	id := buildID(1, "wire-awakeable")
	target := &enginev1.InvocationTarget{
		ServiceName: "Waiter",
		HandlerName: "await",
		ObjectKey:   fake.objectKey,
	}
	shardID := host.Partitioner().ShardForInvocation(id)
	pr := host.Partition(shardID)
	if pr == nil {
		t.Fatalf("partition %d not running", shardID)
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer subCancel()
	if err := pr.Proposer().ProposeIngress(subCtx, "test/wire-awakeable", shardID, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: id,
			Target:       target,
			Input:        []byte("hello"),
			DeploymentId: regResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress: %v", err)
	}

	// Wait for the handler to journal its awakeable id via SetState.
	store := pr.Snapshotter().Store()
	st := tables.StateTable{S: store}
	var awakeableID string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		v, present, err := st.Get(target, "awk_id")
		if err == nil && present {
			awakeableID = string(v)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if awakeableID == "" {
		t.Fatal("awakeable id never landed in StateTable")
	}

	// Resolve the awakeable via the ingress server. The result wakes
	// the suspended invocation; respawn replays with the cached value.
	if err := proposeAwakeableResolved(host, awakeableID, []byte(wantOutput)); err != nil {
		t.Fatalf("ResolveAwakeable: %v", err)
	}

	completed := awaitCompleted(t, host, shardID, id, 10*time.Second)
	if got := string(completed.GetOutput()); got != wantOutput {
		t.Errorf("output = %q; want %q", got, wantOutput)
	}
	if completed.GetFailureMessage() != "" {
		t.Errorf("failure_message = %q; want empty", completed.GetFailureMessage())
	}
}

// proposeAwakeableResolved drives the engine-side AwakeableResolved
// effect directly. Skips the gRPC ingress server hop so the test stays
// self-contained, mirroring what internal/ingress/awakeable.go does
// after looking up the owner: SyncRead the awakeable directory →
// Propose InvokerEffect_AwakeableResolved on the owner's shard.
func proposeAwakeableResolved(host *engine.Host, awakeableID string, value []byte) error {
	ownerPK, err := awakeableOwnerPartitionKey(awakeableID)
	if err != nil {
		return err
	}
	shardID := host.Partitioner().ShardForKey(ownerPK)
	runner := host.Partition(shardID)
	if runner == nil {
		return fmt.Errorf("no partition for shard %d", shardID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := host.NodeHost().SyncRead(ctx, shardID, engine.LookupAwakeable{ID: awakeableID})
	if err != nil {
		return fmt.Errorf("lookup awakeable: %w", err)
	}
	entry, ok := res.(*enginev1.AwakeableEntry)
	if !ok || entry == nil {
		return fmt.Errorf("awakeable %q not found", awakeableID)
	}
	effect := &enginev1.InvokerEffect{
		InvocationId: entry.GetOwner(),
		Kind: &enginev1.InvokerEffect_AwakeableResolved{AwakeableResolved: &enginev1.AwakeableResolved{
			AwakeableId: awakeableID,
			Value:       value,
		}},
	}
	cmd := &enginev1.Command{Kind: &enginev1.Command_InvokerEffect{InvokerEffect: effect}}
	return runner.Proposer().ProposeIngress(ctx, "awk/"+awakeableID, 1, cmd)
}

// awakeableOwnerPartitionKey decodes the partition_key embedded in the
// id's first 8 bytes. Mirrors keys.AwakeableOwnerPartitionKey.
func awakeableOwnerPartitionKey(id string) (uint64, error) {
	const prefix = "awk_"
	if !strings.HasPrefix(id, prefix) {
		return 0, fmt.Errorf("awakeable id %q missing %q prefix", id, prefix)
	}
	body, err := base64.RawURLEncoding.DecodeString(id[len(prefix):])
	if err != nil {
		return 0, fmt.Errorf("decode awakeable id: %w", err)
	}
	if len(body) != 16 {
		return 0, fmt.Errorf("awakeable id body len %d; want 16", len(body))
	}
	return binary.BigEndian.Uint64(body[:8]), nil
}
