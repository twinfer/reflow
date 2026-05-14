package loadgen

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/dragonboat/v4/raftpb"
)

func TestPartitionMatrix_DefaultAllowsEverything(t *testing.T) {
	m := NewPartitionMatrix()
	if !m.Allowed("a", "b") {
		t.Fatal("zero matrix should allow a->b")
	}
	if !m.Allowed("a", "a") {
		t.Fatal("self-traffic should always be allowed")
	}
}

func TestPartitionMatrix_CutHealSymmetric(t *testing.T) {
	m := NewPartitionMatrix()
	m.Cut("a", "b")
	if m.Allowed("a", "b") || m.Allowed("b", "a") {
		t.Fatal("Cut(a,b) must block both directions")
	}
	if !m.Allowed("a", "c") {
		t.Fatal("Cut(a,b) must not affect a->c")
	}
	m.Heal("b", "a") // healed via reversed args — symmetric
	if !m.Allowed("a", "b") {
		t.Fatal("Heal(b,a) should heal the (a,b) pair")
	}
}

func TestPartitionMatrix_CutSelfNoop(t *testing.T) {
	m := NewPartitionMatrix()
	m.Cut("a", "a")
	if !m.Allowed("a", "a") {
		t.Fatal("self-Cut must not block self-traffic")
	}
}

func TestPartitionMatrix_ConcurrentCutHealAllowed(t *testing.T) {
	m := NewPartitionMatrix()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writers flip a-b on and off; readers query Allowed in parallel.
	// Race detector catches any unsynchronized access.
	wg.Add(3)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.Cut("a", "b")
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.Heal("a", "b")
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = m.Allowed("a", "b")
				_ = m.Allowed("b", "a")
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestBufconnTransport_RoundTrip sets up two transports sharing a hub
// + matrix, sends a MessageBatch in each direction, and confirms
// delivery via the message handler. Then it Cuts the pair and
// verifies subsequent sends return errPartitioned without ever
// reaching the handler.
func TestBufconnTransport_RoundTrip(t *testing.T) {
	hub := NewBufconnHub()
	matrix := NewPartitionMatrix()
	factory := NewBufconnTransportFactory(hub, matrix)

	var aRecvCount, bRecvCount atomic.Int32
	aRecvCh := make(chan raftpb.MessageBatch, 4)
	bRecvCh := make(chan raftpb.MessageBatch, 4)

	tA := factory.Create(
		config.NodeHostConfig{RaftAddress: "nodeA"},
		func(batch raftpb.MessageBatch) {
			aRecvCount.Add(1)
			select {
			case aRecvCh <- batch:
			default:
			}
		},
		func(chunk raftpb.Chunk) bool { return true },
	)
	tB := factory.Create(
		config.NodeHostConfig{RaftAddress: "nodeB"},
		func(batch raftpb.MessageBatch) {
			bRecvCount.Add(1)
			select {
			case bRecvCh <- batch:
			default:
			}
		},
		func(chunk raftpb.Chunk) bool { return true },
	)
	if err := tA.Start(); err != nil {
		t.Fatalf("tA.Start: %v", err)
	}
	defer tA.Close()
	if err := tB.Start(); err != nil {
		t.Fatalf("tB.Start: %v", err)
	}
	defer tB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// A -> B
	connAB, err := tA.GetConnection(ctx, "nodeB")
	if err != nil {
		t.Fatalf("tA.GetConnection(nodeB): %v", err)
	}
	defer connAB.Close()
	if err := connAB.SendMessageBatch(raftpb.MessageBatch{SourceAddress: "nodeA"}); err != nil {
		t.Fatalf("send A->B: %v", err)
	}
	select {
	case got := <-bRecvCh:
		if got.SourceAddress != "nodeA" {
			t.Fatalf("B received batch with SourceAddress=%q, want %q", got.SourceAddress, "nodeA")
		}
	case <-time.After(time.Second):
		t.Fatal("B did not receive A's batch within 1s")
	}

	// B -> A
	connBA, err := tB.GetConnection(ctx, "nodeA")
	if err != nil {
		t.Fatalf("tB.GetConnection(nodeA): %v", err)
	}
	defer connBA.Close()
	if err := connBA.SendMessageBatch(raftpb.MessageBatch{SourceAddress: "nodeB"}); err != nil {
		t.Fatalf("send B->A: %v", err)
	}
	select {
	case got := <-aRecvCh:
		if got.SourceAddress != "nodeB" {
			t.Fatalf("A received batch with SourceAddress=%q, want %q", got.SourceAddress, "nodeB")
		}
	case <-time.After(time.Second):
		t.Fatal("A did not receive B's batch within 1s")
	}

	// Cut the pair. Sends on the cached connections must error and
	// the handlers must not see the batch.
	matrix.Cut("nodeA", "nodeB")
	beforeA := aRecvCount.Load()
	beforeB := bRecvCount.Load()
	if err := connAB.SendMessageBatch(raftpb.MessageBatch{SourceAddress: "nodeA"}); !errors.Is(err, errPartitioned) {
		t.Fatalf("A->B send after Cut: err = %v, want errPartitioned", err)
	}
	if err := connBA.SendMessageBatch(raftpb.MessageBatch{SourceAddress: "nodeB"}); !errors.Is(err, errPartitioned) {
		t.Fatalf("B->A send after Cut: err = %v, want errPartitioned", err)
	}
	// New GetConnection calls during the partition must also fail.
	if _, err := tA.GetConnection(ctx, "nodeB"); !errors.Is(err, errPartitioned) {
		t.Fatalf("A.GetConnection(B) after Cut: err = %v, want errPartitioned", err)
	}
	// Give any wayward delivery a moment to show up.
	time.Sleep(50 * time.Millisecond)
	if aRecvCount.Load() != beforeA {
		t.Fatalf("A received a batch during partition (count %d -> %d)", beforeA, aRecvCount.Load())
	}
	if bRecvCount.Load() != beforeB {
		t.Fatalf("B received a batch during partition (count %d -> %d)", beforeB, bRecvCount.Load())
	}

	// Heal restores delivery on a fresh connection.
	matrix.Heal("nodeA", "nodeB")
	connAB2, err := tA.GetConnection(ctx, "nodeB")
	if err != nil {
		t.Fatalf("A.GetConnection(B) after Heal: %v", err)
	}
	defer connAB2.Close()
	if err := connAB2.SendMessageBatch(raftpb.MessageBatch{SourceAddress: "nodeA"}); err != nil {
		t.Fatalf("send A->B after Heal: %v", err)
	}
	select {
	case <-bRecvCh:
	case <-time.After(time.Second):
		t.Fatal("B did not receive A's post-heal batch within 1s")
	}
}

// TestBufconnTransport_PeerDown verifies that dialing a target with
// no registered listener returns errPeerDown (distinct from
// errPartitioned so chaos tests can tell kill from partition apart).
func TestBufconnTransport_PeerDown(t *testing.T) {
	hub := NewBufconnHub()
	matrix := NewPartitionMatrix()
	factory := NewBufconnTransportFactory(hub, matrix)
	tA := factory.Create(
		config.NodeHostConfig{RaftAddress: "nodeA"},
		func(raftpb.MessageBatch) {},
		func(raftpb.Chunk) bool { return true },
	)
	if err := tA.Start(); err != nil {
		t.Fatalf("tA.Start: %v", err)
	}
	defer tA.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := tA.GetConnection(ctx, "ghost")
	if !errors.Is(err, errPeerDown) {
		t.Fatalf("GetConnection(ghost): err = %v, want errPeerDown", err)
	}
}

// Compile-time check that the factory satisfies the dragonboat
// surface — guards against silent dragonboat API drift.
var _ config.TransportFactory = (*BufconnTransportFactory)(nil)
var _ raftio.ITransport = (*bufconnTransport)(nil)
