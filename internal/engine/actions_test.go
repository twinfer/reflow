package engine

import "testing"

func TestActionCollector_PushDrainClear(t *testing.T) {
	var c ActionCollector
	if c.Len() != 0 {
		t.Fatal("new collector should be empty")
	}
	c.Push(ActInvoke{})
	c.Push(ActRegisterTimer{FireAtMs: 1})
	if c.Len() != 2 {
		t.Fatalf("len after push = %d; want 2", c.Len())
	}

	got := c.Drain()
	if len(got) != 2 {
		t.Errorf("drain returned %d; want 2", len(got))
	}
	if c.Len() != 0 {
		t.Errorf("drain didn't reset collector; len = %d", c.Len())
	}

	c.Push(ActAbortInvocation{})
	c.Clear()
	if c.Len() != 0 {
		t.Errorf("clear didn't reset; len = %d", c.Len())
	}
}
