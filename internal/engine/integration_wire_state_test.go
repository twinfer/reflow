package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/config"
	"github.com/twinfer/reflow/internal/loadgen"
	"github.com/twinfer/reflow/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// fakeHandlerLazyState exercises the lazy state fetch path.
//
// mode controls behavior:
//   - "write": emit a sequence of SetState frames (writes are journaled,
//     populating the StateTable).
//   - "lazyRead": on the first session (known_entries==1) emit a
//     GetLazyStateCommandMessage for lazyReadKey + SuspensionMessage.
//     On respawn (known_entries==3, after JEGetState + JEGetStateResult
//     have been journaled) decode the result frame at slot 2 and echo
//     its value as the invocation output.
//   - "lazyReadKeys": same shape but for GetLazyStateKeysCommandMessage.
//     Echoes the comma-joined keys list as output.
//   - "eagerReadKeys": single-session — derives the sorted keys list
//     from StartMessage.state_map, emits GetEagerStateKeysCommandMessage
//     (single slot, no completion) and echoes the comma-joined keys.
type fakeHandlerLazyState struct {
	mode         string
	writes       map[string][]byte
	lazyReadKey  string
	output       []byte
	partialFound bool // set by the test driver to record partial_state observation
}

func (f *fakeHandlerLazyState) discovery() *discoveryv1.DiscoveryResponse {
	return &discoveryv1.DiscoveryResponse{
		ProtocolVersion: "v1",
		Handlers: []*discoveryv1.DiscoveredHandler{
			{Service: "BigState", Kind: protocolv1.Kind_KIND_OBJECT, HandlerNames: []string{"tick"}},
		},
	}
}

func (f *fakeHandlerLazyState) serveInvoke(t *testing.T, stream *fakeBidi) error {
	t.Helper()

	startFrame, err := stream.Receive()
	if err != nil {
		return err
	}
	var sm protocolv1.StartMessage
	if err := proto.Unmarshal(startFrame.GetPayload(), &sm); err != nil {
		return err
	}
	known := sm.GetKnownEntries()
	// Buffer all replay frames so we can decode by slot for lazyRead.
	replay := make(map[uint32]*protocolv1.Frame, known)
	for range known {
		fr, err := stream.Receive()
		if err != nil {
			return err
		}
		replay[fr.GetSlot()] = fr
	}

	switch f.mode {
	case "write":
		return f.handleWrite(stream)
	case "lazyRead":
		f.partialFound = sm.GetPartialState()
		return f.handleLazyRead(stream, known, replay)
	case "lazyReadKeys":
		f.partialFound = sm.GetPartialState()
		return f.handleLazyReadKeys(stream, known, replay)
	case "eagerReadKeys":
		f.partialFound = sm.GetPartialState()
		return f.handleEagerReadKeys(stream, sm.GetStateMap())
	default:
		return fmt.Errorf("unknown mode %q", f.mode)
	}
}

func (f *fakeHandlerLazyState) handleWrite(stream *fakeBidi) error {
	for k, v := range f.writes {
		setMsg := &protocolv1.SetStateCommandMessage{
			Key:   []byte(k),
			Value: &protocolv1.Value{Content: v},
		}
		payload, err := proto.Marshal(setMsg)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeCmdSetState, payload)); err != nil {
			return err
		}
	}
	return f.sendOutputAndEnd(stream)
}

func (f *fakeHandlerLazyState) handleLazyRead(stream *fakeBidi, known uint32, replay map[uint32]*protocolv1.Frame) error {
	if known <= 1 {
		cmd := &protocolv1.GetLazyStateCommandMessage{
			Key:                []byte(f.lazyReadKey),
			ResultCompletionId: 2,
		}
		payload, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeCmdGetLazyState, payload)); err != nil {
			return err
		}
		sus := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
		susPayload, err := proto.Marshal(sus)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeSuspension, susPayload)); err != nil {
			return err
		}
		return drainStream(stream)
	}
	// Respawn — decode the JEGetStateResult at slot 2 and echo.
	resultFrame, ok := replay[2]
	if !ok {
		return fmt.Errorf("lazyRead respawn: no replay entry at slot 2")
	}
	var note protocolv1.GetLazyStateCompletionNotificationMessage
	if err := proto.Unmarshal(resultFrame.GetPayload(), &note); err != nil {
		return fmt.Errorf("decode GetLazyStateCompletionNotificationMessage: %w", err)
	}
	var out []byte
	switch r := note.GetResult().(type) {
	case *protocolv1.GetLazyStateCompletionNotificationMessage_Value:
		out = r.Value.GetContent()
	case *protocolv1.GetLazyStateCompletionNotificationMessage_Void:
		out = []byte("VOID")
	}
	f.output = out
	return f.sendOutputAndEnd(stream)
}

func (f *fakeHandlerLazyState) handleLazyReadKeys(stream *fakeBidi, known uint32, replay map[uint32]*protocolv1.Frame) error {
	if known <= 1 {
		cmd := &protocolv1.GetLazyStateKeysCommandMessage{ResultCompletionId: 2}
		payload, err := proto.Marshal(cmd)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeCmdGetLazyStateKeys, payload)); err != nil {
			return err
		}
		sus := &protocolv1.SuspensionMessage{WaitingCompletions: []uint32{2}}
		susPayload, err := proto.Marshal(sus)
		if err != nil {
			return err
		}
		if err := stream.Send(frameFor(wire.TypeSuspension, susPayload)); err != nil {
			return err
		}
		return drainStream(stream)
	}
	resultFrame, ok := replay[2]
	if !ok {
		return fmt.Errorf("lazyReadKeys respawn: no replay entry at slot 2")
	}
	var note protocolv1.GetLazyStateKeysCompletionNotificationMessage
	if err := proto.Unmarshal(resultFrame.GetPayload(), &note); err != nil {
		return fmt.Errorf("decode GetLazyStateKeysCompletionNotificationMessage: %w", err)
	}
	keys := make([]string, 0, len(note.GetStateKeys().GetKeys()))
	for _, k := range note.GetStateKeys().GetKeys() {
		keys = append(keys, string(k))
	}
	f.output = []byte(strings.Join(keys, ","))
	return f.sendOutputAndEnd(stream)
}

func (f *fakeHandlerLazyState) handleEagerReadKeys(stream *fakeBidi, stateMap []*protocolv1.StartMessage_StateEntry) error {
	keys := make([]string, 0, len(stateMap))
	for _, e := range stateMap {
		keys = append(keys, string(e.GetKey()))
	}
	// StateTable.ScanObject populates state_map in lex order, but be
	// defensive — the SDK contract is "sorted on the wire".
	sortStrings(keys)
	keysBytes := make([][]byte, len(keys))
	for i, k := range keys {
		keysBytes[i] = []byte(k)
	}
	cmd := &protocolv1.GetEagerStateKeysCommandMessage{
		Value: &protocolv1.StateKeys{Keys: keysBytes},
	}
	payload, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(wire.TypeCmdGetEagerStateKeys, payload)); err != nil {
		return err
	}
	f.output = []byte(strings.Join(keys, ","))
	return f.sendOutputAndEnd(stream)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func (f *fakeHandlerLazyState) sendOutputAndEnd(stream *fakeBidi) error {
	out := &protocolv1.OutputCommandMessage{
		Result: &protocolv1.OutputCommandMessage_Value{
			Value: &protocolv1.Value{Content: f.output},
		},
	}
	payload, err := proto.Marshal(out)
	if err != nil {
		return err
	}
	if err := stream.Send(frameFor(wire.TypeCmdOutput, payload)); err != nil {
		return err
	}
	endPayload, _ := proto.Marshal(&protocolv1.EndMessage{})
	if err := stream.Send(frameFor(wire.TypeEnd, endPayload)); err != nil {
		return err
	}
	return drainStream(stream)
}

// TestWireDispatch_HTTP2_LazyState_FetchPresent populates state beyond
// the engine's 64 KiB eager-preload cap, then opens a fresh invocation
// on the same (service, object_key) — StartMessage.partial_state=true —
// and exercises the lazy fetch path for a specific key.
//
// Verifies:
//   - the engine signals partial_state=true after overflow;
//   - the GetLazyStateCommandMessage round-trips to a journaled
//     JEGetStateResult; and
//   - the handler's respawn decodes the result and echoes the value as
//     the invocation output.
func TestWireDispatch_HTTP2_LazyState_FetchPresent(t *testing.T) {
	// Three 30 KiB values push the total past the 64 KiB cap.
	big := strings.Repeat("x", 30*1024)
	writes := map[string][]byte{
		"alpha":  []byte(big),
		"beta":   []byte(big),
		"gamma":  []byte(big),
		"target": []byte("found-it"),
	}
	writer := &fakeHandlerLazyState{
		mode:   "write",
		writes: writes,
		output: []byte("write:ok"),
	}
	reader := &fakeHandlerLazyState{
		mode:        "lazyRead",
		lazyReadKey: "target",
	}

	writerAddr, writerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, writer.discovery(), writer.serveInvoke))
	defer writerTeardown()
	readerAddr, readerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, reader.discovery(), reader.serveInvoke))
	defer readerTeardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host
	srv, err := config.NewServer(config.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	writerResp, err := callRegisterDeployment(regCtx, srv, "http://"+writerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment writer: %v", err)
	}
	readerResp, err := callRegisterDeployment(regCtx, srv, "http://"+readerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment reader: %v", err)
	}

	target := &enginev1.InvocationTarget{ServiceName: "BigState", HandlerName: "tick", ObjectKey: "obj-1"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subCancel()

	// Step 1: populate state.
	idA := buildID(1, "lazy-write")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/lazy-write", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idA, Target: target, Input: []byte("hello"),
			DeploymentId: writerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress writer: %v", err)
	}
	_ = awaitCompleted(t, host, 1, idA, 15*time.Second)

	// Step 2: lazy-read the "target" key. Reader handler suspends on the
	// fetch and respawns; the second session decodes the result and echoes.
	idB := buildID(1, "lazy-read")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/lazy-read", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idB, Target: target, Input: []byte("hello"),
			DeploymentId: readerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress reader: %v", err)
	}
	completedB := awaitCompleted(t, host, 1, idB, 15*time.Second)
	if got := string(completedB.GetOutput()); got != "found-it" {
		t.Errorf("output = %q; want %q (lazy fetch should have returned the journaled value)", got, "found-it")
	}
	if !reader.partialFound {
		t.Errorf("reader observed partial_state=false; want true (eager preload should have overflowed)")
	}
}

// TestWireDispatch_HTTP2_LazyState_FetchAbsent exercises the
// (present=false) branch of JEGetStateResult: when the SDK lazy-fetches
// a key that doesn't exist, the engine appends a void result and the
// handler observes (nil, false).
func TestWireDispatch_HTTP2_LazyState_FetchAbsent(t *testing.T) {
	// Need partialState=true so the SDK actually fetches; populate state
	// past the cap, then read a non-existent key.
	big := strings.Repeat("x", 30*1024)
	writes := map[string][]byte{
		"alpha": []byte(big),
		"beta":  []byte(big),
		"gamma": []byte(big),
	}
	writer := &fakeHandlerLazyState{mode: "write", writes: writes, output: []byte("write:ok")}
	reader := &fakeHandlerLazyState{mode: "lazyRead", lazyReadKey: "missing"}

	writerAddr, writerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, writer.discovery(), writer.serveInvoke))
	defer writerTeardown()
	readerAddr, readerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, reader.discovery(), reader.serveInvoke))
	defer readerTeardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host
	srv, err := config.NewServer(config.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	writerResp, err := callRegisterDeployment(regCtx, srv, "http://"+writerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment writer: %v", err)
	}
	readerResp, err := callRegisterDeployment(regCtx, srv, "http://"+readerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment reader: %v", err)
	}

	target := &enginev1.InvocationTarget{ServiceName: "BigState", HandlerName: "tick", ObjectKey: "obj-absent"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subCancel()

	idA := buildID(1, "lazy-write-abs")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/lazy-write-abs", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idA, Target: target, Input: []byte("hello"),
			DeploymentId: writerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress writer: %v", err)
	}
	_ = awaitCompleted(t, host, 1, idA, 15*time.Second)

	idB := buildID(1, "lazy-read-abs")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/lazy-read-abs", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idB, Target: target, Input: []byte("hello"),
			DeploymentId: readerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress reader: %v", err)
	}
	completedB := awaitCompleted(t, host, 1, idB, 15*time.Second)
	if got := string(completedB.GetOutput()); got != "VOID" {
		t.Errorf("output = %q; want %q (lazy fetch should have returned void)", got, "VOID")
	}
}

// TestWireDispatch_HTTP2_LazyStateKeys verifies the GetStateKeys lazy
// fetch: handler emits GetLazyStateKeysCommandMessage, engine scans the
// StateTable and appends JEGetStateKeysResult inline; respawn reads the
// keys list.
func TestWireDispatch_HTTP2_LazyStateKeys(t *testing.T) {
	writer := &fakeHandlerLazyState{
		mode: "write",
		writes: map[string][]byte{
			"alpha":   []byte("a"),
			"beta":    []byte("b"),
			"charlie": []byte("c"),
		},
		output: []byte("write:ok"),
	}
	reader := &fakeHandlerLazyState{mode: "lazyReadKeys"}

	writerAddr, writerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, writer.discovery(), writer.serveInvoke))
	defer writerTeardown()
	readerAddr, readerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, reader.discovery(), reader.serveInvoke))
	defer readerTeardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host
	srv, err := config.NewServer(config.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	writerResp, err := callRegisterDeployment(regCtx, srv, "http://"+writerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment writer: %v", err)
	}
	readerResp, err := callRegisterDeployment(regCtx, srv, "http://"+readerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment reader: %v", err)
	}

	target := &enginev1.InvocationTarget{ServiceName: "BigState", HandlerName: "tick", ObjectKey: "obj-keys"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subCancel()

	idA := buildID(1, "keys-write")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/keys-write", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idA, Target: target, Input: []byte("hello"),
			DeploymentId: writerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress writer: %v", err)
	}
	_ = awaitCompleted(t, host, 1, idA, 15*time.Second)

	idB := buildID(1, "keys-read")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/keys-read", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idB, Target: target, Input: []byte("hello"),
			DeploymentId: readerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress reader: %v", err)
	}
	completedB := awaitCompleted(t, host, 1, idB, 15*time.Second)
	// StateTable.ScanObject returns keys in lex order; assert exact match.
	if got := string(completedB.GetOutput()); got != "alpha,beta,charlie" {
		t.Errorf("output = %q; want %q", got, "alpha,beta,charlie")
	}
}

// TestWireDispatch_HTTP2_EagerStateKeys verifies the single-slot eager
// keys path: with state small enough to fit the 64 KiB cap, the SDK
// derives sorted keys from StartMessage.state_map and emits
// GetEagerStateKeysCommandMessage inline. The engine stamps
// JEGetEagerStateKeys; the invocation completes without suspension.
func TestWireDispatch_HTTP2_EagerStateKeys(t *testing.T) {
	writer := &fakeHandlerLazyState{
		mode: "write",
		writes: map[string][]byte{
			"alpha":   []byte("a"),
			"beta":    []byte("b"),
			"charlie": []byte("c"),
		},
		output: []byte("write:ok"),
	}
	reader := &fakeHandlerLazyState{mode: "eagerReadKeys"}

	writerAddr, writerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, writer.discovery(), writer.serveInvoke))
	defer writerTeardown()
	readerAddr, readerTeardown := startFakeHandlerHTTP2WithHandler(t, mountFakeHandler(t, reader.discovery(), reader.serveInvoke))
	defer readerTeardown()

	cluster := loadgen.NewCluster(t, loadgen.ClusterOptions{N: 3})
	defer cluster.Close()
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer awaitCancel()
	if err := cluster.AwaitAnyMetadataLeader(awaitCtx); err != nil {
		t.Fatalf("AwaitAnyMetadataLeader: %v", err)
	}
	leaderRig := findMetadataLeader(t, cluster)
	host := leaderRig.Host
	srv, err := config.NewServer(config.Config{Host: host, Runner: host.MetadataRunner()})
	if err != nil {
		t.Fatalf("config.NewServer: %v", err)
	}

	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer regCancel()
	writerResp, err := callRegisterDeployment(regCtx, srv, "http://"+writerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment writer: %v", err)
	}
	readerResp, err := callRegisterDeployment(regCtx, srv, "http://"+readerAddr)
	if err != nil {
		t.Fatalf("RegisterDeployment reader: %v", err)
	}

	target := &enginev1.InvocationTarget{ServiceName: "BigState", HandlerName: "tick", ObjectKey: "obj-eager-keys"}
	pr := host.Partition(1)
	if pr == nil {
		t.Fatal("partition 1 not running")
	}
	subCtx, subCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer subCancel()

	idA := buildID(1, "eager-keys-write")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/eager-keys-write", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idA, Target: target, Input: []byte("hello"),
			DeploymentId: writerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress writer: %v", err)
	}
	_ = awaitCompleted(t, host, 1, idA, 15*time.Second)

	idB := buildID(1, "eager-keys-read")
	if err := pr.Proposer().ProposeIngress(subCtx, "test/eager-keys-read", 1, &enginev1.Command{
		Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
			InvocationId: idB, Target: target, Input: []byte("hello"),
			DeploymentId: readerResp.GetDeploymentId(),
		}},
	}); err != nil {
		t.Fatalf("ProposeIngress reader: %v", err)
	}
	completedB := awaitCompleted(t, host, 1, idB, 15*time.Second)
	if got := string(completedB.GetOutput()); got != "alpha,beta,charlie" {
		t.Errorf("output = %q; want %q", got, "alpha,beta,charlie")
	}
	if reader.partialFound {
		t.Errorf("reader observed partial_state=true; want false (state fits the eager cap)")
	}
}
