package cluster

import "testing"

func TestTableNotifier_BumpIsNonBlocking(t *testing.T) {
	n := NewTableNotifier()
	// Buffer is 1; first Bump goes in, second drops.
	for range 5 {
		n.Bump() // must never block
	}
	select {
	case <-n.Subscribe():
		// drained the single coalesced signal
	default:
		t.Fatal("expected one pending signal in subscriber channel")
	}
	// Channel should now be empty — the extra Bumps were dropped.
	select {
	case <-n.Subscribe():
		t.Fatal("expected no additional signals after drain")
	default:
	}
}

func TestTableNotifier_NilSafe(t *testing.T) {
	var n *TableNotifier
	n.Bump() // must not panic
	if ch := n.Subscribe(); ch != nil {
		t.Fatal("nil notifier Subscribe should return nil channel")
	}
}
