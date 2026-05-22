package engine

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lni/dragonboat/v4/config"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// TestPeerResolvedNodeHostID confirms the deterministic
// NodeHostID derivation is identical for an empty override and the
// explicit derived form. Every node in the cluster runs the same code,
// so a static peer list bootstraps without any out-of-band ID exchange.
func TestPeerResolvedNodeHostID(t *testing.T) {
	tests := []struct {
		name string
		in   Peer
		want string
	}{
		{"derived", Peer{NodeID: 7}, "00000000-0000-0000-0000-000000000007"},
		{"override", Peer{NodeID: 7, NodeHostID: "custom-id"}, "custom-id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.resolvedNodeHostID(); got != tc.want {
				t.Errorf("resolvedNodeHostID() = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestApplyMultiNodeConfig_OK packs three peers (self + two
// others) into a NodeHostConfig and asserts every gossip field is set:
// DefaultNodeRegistryEnabled, NodeHostID for self, BindAddress,
// AdvertiseAddress, Seed (excluding self), and a Meta blob carrying the
// expected NodeHostMeta.GrpcEndpoint.
func TestApplyMultiNodeConfig_OK(t *testing.T) {
	cfg := HostConfig{
		NodeID:         2,
		RaftAddr:       "10.0.0.2:9091",
		GossipBindAddr: "10.0.0.2:9101",
		GossipAdvAddr:  "10.0.0.2:9101",
		GrpcEndpoint:   "10.0.0.2:8081",
		Peers: []Peer{
			{NodeID: 1, RaftAddr: "10.0.0.1:9091", GossipAddr: "10.0.0.1:9101"},
			{NodeID: 2, RaftAddr: "10.0.0.2:9091", GossipAddr: "10.0.0.2:9101"},
			{NodeID: 3, RaftAddr: "10.0.0.3:9091", GossipAddr: "10.0.0.3:9101"},
		},
	}
	var nh config.NodeHostConfig
	if err := applyMultiNodeConfig(&nh, &cfg); err != nil {
		t.Fatalf("applyMultiNodeConfig: %v", err)
	}
	if !nh.DefaultNodeRegistryEnabled {
		t.Error("DefaultNodeRegistryEnabled = false; want true")
	}
	if nh.NodeHostID != "00000000-0000-0000-0000-000000000002" {
		t.Errorf("NodeHostID = %q; want UUID for node 2", nh.NodeHostID)
	}
	if nh.Gossip.BindAddress != "10.0.0.2:9101" {
		t.Errorf("Gossip.BindAddress = %q", nh.Gossip.BindAddress)
	}
	if nh.Gossip.AdvertiseAddress != "10.0.0.2:9101" {
		t.Errorf("Gossip.AdvertiseAddress = %q", nh.Gossip.AdvertiseAddress)
	}
	wantSeeds := map[string]struct{}{
		"10.0.0.1:9101": {},
		"10.0.0.3:9101": {},
	}
	if len(nh.Gossip.Seed) != 2 {
		t.Fatalf("Gossip.Seed = %v; want 2 entries", nh.Gossip.Seed)
	}
	for _, s := range nh.Gossip.Seed {
		if _, ok := wantSeeds[s]; !ok {
			t.Errorf("Gossip.Seed contains unexpected %q", s)
		}
	}
	var meta enginev1.NodeHostMeta
	if err := proto.Unmarshal(nh.Gossip.Meta, &meta); err != nil {
		t.Fatalf("unmarshal Meta: %v", err)
	}
	if meta.GetGrpcEndpoint() != "10.0.0.2:8081" {
		t.Errorf("Meta.GrpcEndpoint = %q", meta.GetGrpcEndpoint())
	}
}

// TestApplyMultiNodeConfig_GossipAdvFallback verifies that
// GossipAdvAddr defaults to GossipBindAddr when left empty — a common
// single-network deployment shorthand.
func TestApplyMultiNodeConfig_GossipAdvFallback(t *testing.T) {
	cfg := HostConfig{
		NodeID:         1,
		RaftAddr:       "10.0.0.1:9091",
		GossipBindAddr: "10.0.0.1:9101",
		GrpcEndpoint:   "10.0.0.1:8081",
		Peers: []Peer{
			{NodeID: 1, GossipAddr: "10.0.0.1:9101"},
			{NodeID: 2, GossipAddr: "10.0.0.2:9101"},
		},
	}
	var nh config.NodeHostConfig
	if err := applyMultiNodeConfig(&nh, &cfg); err != nil {
		t.Fatalf("applyMultiNodeConfig: %v", err)
	}
	if nh.Gossip.AdvertiseAddress != "10.0.0.1:9101" {
		t.Errorf("AdvertiseAddress fallback = %q; want 10.0.0.1:9101", nh.Gossip.AdvertiseAddress)
	}
}

// TestApplyMultiNodeConfig_RejectsIfSelfMissing rejects a
// peer list that does not contain this node's own NodeID. The contract
// is that every node holds a fully-symmetric Peers slice.
func TestApplyMultiNodeConfig_RejectsIfSelfMissing(t *testing.T) {
	cfg := HostConfig{
		NodeID:         9,
		RaftAddr:       "10.0.0.9:9091",
		GossipBindAddr: "10.0.0.9:9101",
		GrpcEndpoint:   "10.0.0.9:8081",
		Peers: []Peer{
			{NodeID: 1, GossipAddr: "10.0.0.1:9101"},
			{NodeID: 2, GossipAddr: "10.0.0.2:9101"},
		},
	}
	var nh config.NodeHostConfig
	err := applyMultiNodeConfig(&nh, &cfg)
	if err == nil || !strings.Contains(err.Error(), "not present in Peers") {
		t.Fatalf("err = %v; want 'not present in Peers'", err)
	}
}

// TestApplyMultiNodeConfig_RejectsMissingBindOrEndpoint
// confirms the two required fields are guarded.
func TestApplyMultiNodeConfig_RejectsMissingBindOrEndpoint(t *testing.T) {
	base := HostConfig{
		NodeID:   1,
		RaftAddr: "10.0.0.1:9091",
		Peers:    []Peer{{NodeID: 1}},
	}
	cases := map[string]func(c *HostConfig){
		"missing GossipBindAddr": func(c *HostConfig) {
			c.GrpcEndpoint = "10.0.0.1:8081"
		},
		"missing GrpcEndpoint": func(c *HostConfig) {
			c.GossipBindAddr = "10.0.0.1:9101"
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := base
			mut(&cfg)
			var nh config.NodeHostConfig
			if err := applyMultiNodeConfig(&nh, &cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", name)
			}
		})
	}
}

// TestApplyMultiNodeConfig_RejectsInvalidUUIDOverride catches a
// peer with a NodeHostID override that doesn't parse as RFC 4122 — the
// same check dragonboat applies inside NewNodeHost, raised here so the
// error attributes the offending peer.
func TestApplyMultiNodeConfig_RejectsInvalidUUIDOverride(t *testing.T) {
	cfg := HostConfig{
		NodeID:         1,
		RaftAddr:       "10.0.0.1:9091",
		GossipBindAddr: "10.0.0.1:9101",
		GrpcEndpoint:   "10.0.0.1:8081",
		Peers: []Peer{
			{NodeID: 1, GossipAddr: "10.0.0.1:9101"},
			{NodeID: 2, GossipAddr: "10.0.0.2:9101", NodeHostID: "not-a-uuid"},
		},
	}
	var nh config.NodeHostConfig
	err := applyMultiNodeConfig(&nh, &cfg)
	if err == nil || !strings.Contains(err.Error(), "not a valid UUID") {
		t.Fatalf("err = %v; want 'not a valid UUID'", err)
	}
	if !strings.Contains(err.Error(), "NodeID=2") {
		t.Errorf("err = %v; want peer NodeID=2 attribution", err)
	}
}

// TestDerivedNodeHostID_IsValidUUID guards the derived form
// against silent drift — dragonboat's NodeHost constructor will reject
// any non-UUID NodeHostID, and we want that signaled at the resolver.
func TestDerivedNodeHostID_IsValidUUID(t *testing.T) {
	for _, id := range []uint64{1, 2, 7, 999, 1<<32 + 1} {
		p := Peer{NodeID: id}
		if _, err := uuid.Parse(p.resolvedNodeHostID()); err != nil {
			t.Errorf("resolvedNodeHostID for NodeID=%d = %q; not a UUID: %v",
				id, p.resolvedNodeHostID(), err)
		}
	}
}

// TestInitialMembers_FromPeers verifies StartPartition will
// receive a NodeHostID-keyed initialMembers map when Peers is populated.
// Single-node fallback is exercised by every existing single-node test.
func TestInitialMembers_FromPeers(t *testing.T) {
	h := &Host{cfg: HostConfig{
		NodeID: 2,
		Peers: []Peer{
			{NodeID: 1, RaftAddr: "10.0.0.1:9091"},
			{NodeID: 2, RaftAddr: "10.0.0.2:9091"},
			{NodeID: 3, RaftAddr: "10.0.0.3:9091", NodeHostID: "custom-3"},
		},
	}}
	got := h.initialMembers()
	want := map[uint64]string{
		1: "00000000-0000-0000-0000-000000000001",
		2: "00000000-0000-0000-0000-000000000002",
		3: "custom-3",
	}
	if len(got) != len(want) {
		t.Fatalf("len(initialMembers) = %d; want %d", len(got), len(want))
	}
	for id, target := range got {
		if string(target) != want[id] {
			t.Errorf("initialMembers[%d] = %q; want %q", id, target, want[id])
		}
	}
}

// TestRaftBindAndAdvertise exercises the bind-vs-advertise resolver
// directly. RaftAdvertisedAddr being empty preserves today's combined
// behavior (advertise == RaftAddr, no ListenAddress override); a
// non-empty override flips the gossiped value and pins the listener at
// RaftAddr — the shape the e2e harness needs for Toxiproxy fronting.
func TestRaftBindAndAdvertise(t *testing.T) {
	tests := []struct {
		name               string
		bind               string
		advertised         string
		wantAdvertise      string
		wantListenOverride string
	}{
		{
			name:               "empty advertised falls back to bind",
			bind:               "127.0.0.1:9001",
			advertised:         "",
			wantAdvertise:      "127.0.0.1:9001",
			wantListenOverride: "",
		},
		{
			name:               "advertised equal to bind clears override",
			bind:               "127.0.0.1:9001",
			advertised:         "127.0.0.1:9001",
			wantAdvertise:      "127.0.0.1:9001",
			wantListenOverride: "",
		},
		{
			name:               "advertised differs splits bind from advertise",
			bind:               "0.0.0.0:9001",
			advertised:         "toxiproxy:21001",
			wantAdvertise:      "toxiproxy:21001",
			wantListenOverride: "0.0.0.0:9001",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := HostConfig{RaftAddr: tc.bind, RaftAdvertisedAddr: tc.advertised}
			adv, lo := raftBindAndAdvertise(&cfg)
			if adv != tc.wantAdvertise {
				t.Errorf("advertise = %q; want %q", adv, tc.wantAdvertise)
			}
			if lo != tc.wantListenOverride {
				t.Errorf("listenOverride = %q; want %q", lo, tc.wantListenOverride)
			}
		})
	}
}

// TestApplyMultiNodeConfig_AdvertisedAddr_GossipMeta confirms that
// when a node sets RaftAdvertisedAddr, the value flows through Peers
// (each peer's RaftAddr is what other peers see and is propagated by
// dragonboat's NodeHostRegistry). The advertise/bind split itself is
// covered by TestRaftBindAndAdvertise; this test is the integration with
// the gossip-meta packing code so a regression in either path surfaces.
func TestApplyMultiNodeConfig_AdvertisedAddr_GossipMeta(t *testing.T) {
	cfg := HostConfig{
		NodeID:             2,
		RaftAddr:           "0.0.0.0:9001",
		RaftAdvertisedAddr: "toxiproxy:21002",
		GossipBindAddr:     "0.0.0.0:9002",
		GossipAdvAddr:      "reflowd-node2:9002",
		GrpcEndpoint:       "toxiproxy:21012",
		Peers: []Peer{
			{NodeID: 1, RaftAddr: "toxiproxy:21001", GossipAddr: "reflowd-node1:9002"},
			{NodeID: 2, RaftAddr: "toxiproxy:21002", GossipAddr: "reflowd-node2:9002"},
			{NodeID: 3, RaftAddr: "toxiproxy:21003", GossipAddr: "reflowd-node3:9002"},
		},
	}
	var nh config.NodeHostConfig
	if err := applyMultiNodeConfig(&nh, &cfg); err != nil {
		t.Fatalf("applyMultiNodeConfig: %v", err)
	}
	// applyMultiNodeConfig itself doesn't touch RaftAddress/ListenAddress
	// (NewHost handles those); the contract it owns is the gossip Meta
	// blob and Seed list, both of which should be unaffected by the
	// advertised-addr split.
	if nh.Gossip.AdvertiseAddress != "reflowd-node2:9002" {
		t.Errorf("Gossip.AdvertiseAddress = %q; want reflowd-node2:9002", nh.Gossip.AdvertiseAddress)
	}
	var meta enginev1.NodeHostMeta
	if err := proto.Unmarshal(nh.Gossip.Meta, &meta); err != nil {
		t.Fatalf("unmarshal Meta: %v", err)
	}
	if meta.GetGrpcEndpoint() != "toxiproxy:21012" {
		t.Errorf("Meta.GrpcEndpoint = %q; want toxiproxy:21012", meta.GetGrpcEndpoint())
	}
}
