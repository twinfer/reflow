//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	mobynet "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// clusterSubnet is the /16 every cluster's docker network uses. Per-node
// IPs are derived as <subnet>.10+nodeID so the test process and any
// debug tool can predict them. The subnet is namespaced (not the docker
// default bridge range) to keep collisions unlikely across parallel test
// runs.
const (
	clusterSubnet  = "10.42.0.0/16"
	clusterGateway = "10.42.0.1"
)

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
func newDockerNetwork(t testing.TB) *testcontainers.DockerNetwork {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ipam := &mobynet.IPAM{
		Driver: "default",
		Config: []mobynet.IPAMConfig{
			{
				Subnet:  netip.MustParsePrefix(clusterSubnet),
				Gateway: netip.MustParseAddr(clusterGateway),
			},
		},
	}
	nw, err := network.New(ctx, network.WithIPAM(ipam))
	if err != nil {
		t.Fatalf("e2e: create docker network: %v", err)
	}
	t.Cleanup(func() {
		// Use a fresh background context — t.Context() is already
		// cancelled when Cleanup runs.
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = nw.Remove(shutdown)
	})
	return nw
}

// nodeIP returns the static IPv4 address assigned to reflowd-node<N>
// within the cluster's docker network. Reserved range: 10.42.0.11
// through 10.42.0.99 for reflowd nodes; 10.42.0.100+ for sidecars.
func nodeIP(nodeID uint64) string {
	return fmt.Sprintf("10.42.0.%d", 10+nodeID)
}
