//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// clusterSubnetOctet is the middle octet picked once per test process
// (e.g. 10.<octet>.0.0/16). Each go-test binary runs in its own process,
// so two parallel e2e packages get independent values — eliminating the
// "Pool overlaps with other one on this address space" failure mode
// that fixed-subnet allocation produced under `go test ./internal/e2e/...`.
// Range [50, 250] avoids common bridge/docker defaults at the edges.
//
// Mutable under processSubnetMu so newDockerNetwork can re-roll on a
// genuine collision (rare: another process happens to choose the same
// octet, or a stale network from a killed run survived).
var (
	processSubnetMu sync.Mutex
	clusterOctet    = byte(50 + rand.IntN(201))
)

// clusterSubnet returns the current /16 (e.g. "10.137.0.0/16").
func clusterSubnet() string {
	processSubnetMu.Lock()
	defer processSubnetMu.Unlock()
	return fmt.Sprintf("10.%d.0.0/16", clusterOctet)
}

// clusterGateway returns the .1 gateway IP for the current subnet.
func clusterGateway() string {
	processSubnetMu.Lock()
	defer processSubnetMu.Unlock()
	return fmt.Sprintf("10.%d.0.1", clusterOctet)
}

// reRollSubnet picks a new octet. Called when network.New reports a
// pool overlap so the next attempt has a fresh chance.
func reRollSubnet() {
	processSubnetMu.Lock()
	defer processSubnetMu.Unlock()
	clusterOctet = byte(50 + rand.IntN(201))
}

// newDockerNetwork creates a user-defined bridge network with a fixed
// subnet. Per-container IPs are pre-allocated so dragonboat's gossip
// advertise (which requires an IPv4 literal; the memberlist library
// behind it doesn't accept hostnames — see dragonboat
// config.isValidAdvertiseAddress) can use the cluster-known IPs without
// runtime resolution.
//
// Each container is attached with both a stable DNS alias (reflowd-node1,
// loadhandler, ...) and a fixed IPv4 address. Raft + delivery use DNS
// (they accept hostnames); gossip uses the IP form.
//
// On Docker "Pool overlaps with other one on this address space" the
// helper re-rolls the per-process octet and retries up to maxRetries
// times — that error indicates either a stale network or a concurrent
// process picked the same octet.
func newDockerNetwork(t testing.TB) *testcontainers.DockerNetwork {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const maxRetries = 8
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		ipam := &mobynet.IPAM{
			Driver: "default",
			Config: []mobynet.IPAMConfig{
				{
					Subnet:  netip.MustParsePrefix(clusterSubnet()),
					Gateway: netip.MustParseAddr(clusterGateway()),
				},
			},
		}
		nw, err := network.New(ctx, network.WithIPAM(ipam))
		if err == nil {
			t.Cleanup(func() {
				// Use a fresh background context — t.Context() is already
				// cancelled when Cleanup runs.
				shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_ = nw.Remove(shutdown)
			})
			return nw
		}
		lastErr = err
		if !strings.Contains(err.Error(), "Pool overlaps") {
			break
		}
		reRollSubnet()
	}
	t.Fatalf("e2e: create docker network: %v", lastErr)
	return nil
}

// nodeIP returns the static IPv4 address assigned to reflowd-node<N>
// within the cluster's docker network. Per-cluster octet ranges:
// .11..99 reserved for reflowd nodes; .100+ for sidecars.
func nodeIP(nodeID uint64) string {
	processSubnetMu.Lock()
	defer processSubnetMu.Unlock()
	return fmt.Sprintf("10.%d.0.%d", clusterOctet, 10+nodeID)
}
