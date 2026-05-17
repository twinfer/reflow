// Command reflow-cluster is the admin CLI for a reflow cluster.
//
// PKI subcommands (offline, no cluster contact needed):
//
//	reflow-cluster init-ca        --out=DIR
//	reflow-cluster issue-cert     --kind=node --node-id=N --hostname=H --ca-dir=DIR --out=DIR [--trust-domain=reflow.local]
//	reflow-cluster issue-operator --name=NAME --ca-dir=DIR --out=DIR [--trust-domain=reflow.local]
//
// init-ca writes ca.crt + ca.key. Every leaf is signed by that single
// CA and carries a SPIFFE URI SAN (spiffe://<trust-domain>/node/<id>
// or spiffe://<trust-domain>/operator/<name>) that the reflow TLS layer
// matches against the listener's expected role.
//
// Cluster subcommands (mTLS-authenticated against the Admin gRPC port).
// --admin may point at ANY cluster node — mutating commands follow the
// LeaderHint detail attached to codes.Unavailable to redirect to the
// metadata leader automatically:
//
//	reflow-cluster add-node            --admin=ANY:PORT --node-id=N --raft-addr=... --gossip-addr=... --grpc-endpoint=... [--node-host-id=ID]
//	reflow-cluster remove-node         --admin=ANY:PORT --node-id=N
//	reflow-cluster nodes list          --admin=ANY:PORT
//	reflow-cluster partitions list     --admin=ANY:PORT
//	reflow-cluster snapshot create     --admin=ANY:PORT --shard=N
//	reflow-cluster snapshot list       --admin=ANY:PORT --shard=N
//	reflow-cluster register-deployment --admin=ANY:PORT --url=http://HANDLER:PORT
//
// Every cluster subcommand needs the operator's TLS flags (or matching
// env vars):
//
//	--client-cert   $REFLOW_CLIENT_CERT
//	--client-key    $REFLOW_CLIENT_KEY
//	--ca            $REFLOW_CA_CERT
//	--trust-domain  $REFLOW_TRUST_DOMAIN  (defaults to "reflow.local")
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "init-ca":
		err = cmdInitCA(args)
	case "issue-cert":
		err = cmdIssueCert(args)
	case "issue-operator":
		err = cmdIssueOperator(args)
	case "add-node":
		err = cmdAddNode(ctx, args)
	case "remove-node":
		err = cmdRemoveNode(ctx, args)
	case "nodes":
		err = cmdNodes(ctx, args)
	case "partitions":
		err = cmdPartitions(ctx, args)
	case "snapshot":
		err = cmdSnapshot(ctx, args)
	case "register-deployment":
		err = cmdRegisterDeployment(ctx, args)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return
	default:
		fmt.Fprintf(os.Stderr, "reflow-cluster: unknown subcommand %q\n\n", cmd)
		usage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "reflow-cluster: %v\n", err)
		os.Exit(1)
	}
}

func usage(w *os.File) {
	fmt.Fprint(w, `reflow-cluster — admin CLI for a reflow cluster

PKI:
  init-ca           Create node + operator CAs.
  issue-cert        Issue a node or generic cert against a CA.
  issue-operator    Issue an operator client cert.

Cluster:
  add-node             Register a new peer and start the rebalance.
  remove-node          Mark a peer evicted.
  nodes list           List current membership.
  partitions list      Print the partition table.
  snapshot create      Trigger an exported snapshot of one partition shard.
  snapshot list        List archived snapshots.
  register-deployment  Register a handler deployment URL with the cluster.

Run any subcommand with --help for its specific flags.
`)
}
