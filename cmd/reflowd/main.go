// Command reflowd is the production reflow binary. It exposes three
// top-level subcommands:
//
//	reflowd run                 # start the engine
//	reflowd pki <subcmd>        # offline CA + leaf issuance
//	reflowd cluster <subcmd>    # mTLS-authenticated admin RPCs
//
// PKI subcommands (no cluster contact needed):
//
//	reflowd pki init-ca        --out=DIR
//	reflowd pki issue-cert     --kind=node --node-id=N --hostname=H --ca-dir=DIR --out=DIR [--trust-domain=reflow.local]
//	reflowd pki issue-operator --name=NAME --ca-dir=DIR --out=DIR [--trust-domain=reflow.local]
//
// init-ca writes ca.crt + ca.key. Every leaf is signed by that single CA
// and carries a SPIFFE URI SAN (spiffe://<trust-domain>/node/<id> or
// spiffe://<trust-domain>/operator/<name>) that the reflow TLS layer
// matches against the listener's expected role.
//
// Cluster subcommands (mTLS-authenticated against the Admin Connect
// port). --admin may point at ANY cluster node — mutating commands
// follow the LeaderHint detail attached to connect.CodeUnavailable to
// redirect to the metadata leader automatically:
//
//	reflowd cluster add-node            --admin=ANY:PORT --node-id=N --raft-addr=... --gossip-addr=... --grpc-endpoint=... [--node-host-id=ID]
//	reflowd cluster remove-node         --admin=ANY:PORT --node-id=N
//	reflowd cluster nodes list          --admin=ANY:PORT
//	reflowd cluster partitions list     --admin=ANY:PORT
//	reflowd cluster snapshot create     --admin=ANY:PORT --shard=N
//	reflowd cluster snapshot list       --admin=ANY:PORT --shard=N
//	reflowd cluster snapshot delete     --admin=ANY:PORT --shard=N --index=I
//	reflowd cluster register-deployment --admin=ANY:PORT --url=http://HANDLER:PORT
//
// Cluster subcommands need the operator TLS flags (or matching env vars):
//
//	--client-cert   $REFLOW_CLIENT_CERT
//	--client-key    $REFLOW_CLIENT_KEY
//	--ca            $REFLOW_CA_CERT
//	--trust-domain  $REFLOW_TRUST_DOMAIN  (defaults to "reflow.local")
//
// `reflowd run` reads layered configuration sources (later overrides
// earlier):
//
//  1. Built-in defaults (single-node, shard 1, sensible ports).
//  2. Optional config file from $REFLOW_CONFIG (YAML or JSON).
//  3. REFLOW_* environment variables.
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
	case "run":
		err = cmdRun(args)
	case "pki":
		err = dispatchPKI(args)
	case "cluster":
		err = dispatchCluster(ctx, args)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return
	default:
		fmt.Fprintf(os.Stderr, "reflowd: unknown subcommand %q\n\n", cmd)
		usage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "reflowd: %v\n", err)
		os.Exit(1)
	}
}

// dispatchPKI routes "reflowd pki <subcmd> ..." to the right handler.
func dispatchPKI(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflowd pki {init-ca|issue-cert|issue-operator} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "init-ca":
		return cmdInitCA(rest)
	case "issue-cert":
		return cmdIssueCert(rest)
	case "issue-operator":
		return cmdIssueOperator(rest)
	default:
		return fmt.Errorf("reflowd pki: unknown subcommand %q", sub)
	}
}

// dispatchCluster routes "reflowd cluster <subcmd> ..." to the right
// handler.
func dispatchCluster(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflowd cluster {add-node|remove-node|nodes|partitions|snapshot|register-deployment|eventsources|webhooks|apply|export|get} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "add-node":
		return cmdAddNode(ctx, rest)
	case "remove-node":
		return cmdRemoveNode(ctx, rest)
	case "nodes":
		return cmdNodes(ctx, rest)
	case "partitions":
		return cmdPartitions(ctx, rest)
	case "snapshot":
		return cmdSnapshot(ctx, rest)
	case "register-deployment":
		return cmdRegisterDeployment(ctx, rest)
	case "eventsources":
		return cmdEventSources(ctx, rest)
	case "webhooks":
		return cmdWebhooks(ctx, rest)
	case "apply":
		return cmdApply(ctx, rest)
	case "export":
		return cmdExport(ctx, rest)
	case "get":
		return cmdGet(ctx, rest)
	default:
		return fmt.Errorf("reflowd cluster: unknown subcommand %q", sub)
	}
}

func usage(w *os.File) {
	fmt.Fprint(w, `reflowd — reflow engine + admin CLI

Engine:
  run                  Start the engine. Reads layered config:
                         defaults → $REFLOW_CONFIG file → REFLOW_* env.

PKI (offline, no cluster contact):
  pki init-ca            Create the cluster CA.
  pki issue-cert         Issue a node leaf cert.
  pki issue-operator     Issue an operator client cert.

Cluster (Connect RPC; mTLS-authenticated; --admin can be ANY node):
  cluster add-node              Register a new peer and start rebalance.
  cluster remove-node           Mark a peer evicted.
  cluster nodes list            List current membership.
  cluster partitions list       Print the partition table.
  cluster snapshot create       Trigger an exported snapshot of one shard.
  cluster snapshot list         List archived snapshots.
  cluster snapshot delete       Remove an archived snapshot.
  cluster register-deployment   Register a handler deployment URL.
  cluster eventsources list     List configured event sources.
  cluster eventsources delete   Delete an event source by name.
  cluster webhooks list         List configured webhook sources.
  cluster webhooks delete       Delete a webhook source by name.
  cluster apply -f <file>       Apply a multi-doc YAML file
                                (kinds: EventSource, WebhookSource).
  cluster export --kind=<k>     Dump a kind (or 'all') as multi-doc YAML.
  cluster get <kind> <name>     Fetch one record as YAML.

Run any subcommand with --help for its specific flags.
`)
}
