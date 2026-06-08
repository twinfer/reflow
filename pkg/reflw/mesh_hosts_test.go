package reflw

import (
	"slices"
	"testing"
)

func TestMeshLeafHosts(t *testing.T) {
	var cfg Config
	cfg.Node.RaftAdvertisedAddr = "node1.example:9091"
	cfg.Node.RaftAddr = "0.0.0.0:9091"
	cfg.Node.DeliveryAddr = "node1.example:9100" // same host as raft advertised → dedup
	cfg.Admin.Addr = "0.0.0.0:8082"              // wildcard bind → dropped
	cfg.ClusterCA.LeafHosts = []string{"lb.example", "127.0.0.1"}

	got := meshLeafHosts(cfg)

	// node1.example must appear exactly once despite two sources.
	if n := count(got, "node1.example"); n != 1 {
		t.Errorf("node1.example appears %d times; want 1 (got %v)", n, got)
	}
	for _, want := range []string{"node1.example", "lb.example", "127.0.0.1"} {
		if !slices.Contains(got, want) {
			t.Errorf("meshLeafHosts missing %q (got %v)", want, got)
		}
	}
	if slices.Contains(got, "0.0.0.0") {
		t.Errorf("wildcard bind 0.0.0.0 must be dropped (got %v)", got)
	}
}

func TestHostOnly(t *testing.T) {
	cases := map[string]string{
		"node1:9091":     "node1",
		"127.0.0.1:8082": "127.0.0.1",
		"node1.internal": "node1.internal", // no port
		"":               "",
		"[::1]:9091":     "::1",
	}
	for in, want := range cases {
		if got := hostOnly(in); got != want {
			t.Errorf("hostOnly(%q) = %q; want %q", in, got, want)
		}
	}
}

func count(s []string, v string) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}
