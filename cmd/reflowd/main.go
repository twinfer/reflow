// Command reflowd is the production reflow binary. It exposes four
// top-level subcommands:
//
//	reflowd run                 # start the engine
//	reflowd pki <subcmd>        # offline CA + leaf issuance
//	reflowd cluster <subcmd>    # mTLS-authenticated ClusterCtl RPCs
//	                            # (fleet ops: membership, partitions,
//	                            # snapshots, LP transfers)
//	reflowd config <subcmd>     # mTLS-authenticated Config RPCs
//	                            # (app config: deployments, event
//	                            # sources, webhooks, secrets)
//
// PKI subcommands (no cluster contact needed):
//
//	reflowd pki init-ca        --out=DIR
//	reflowd pki issue-cert     --kind=node --node-id=N --hostname=H --ca-dir=DIR --out=DIR
//	reflowd pki issue-operator --name=NAME --ca-dir=DIR --out=DIR
//
// init-ca writes ca.crt + ca.key. Every leaf is signed by that single CA
// and carries the principal Raw form in its CN (e.g. "node/1",
// "operator/alice") that the reflow TLS layer matches against the
// listener's expected role.
//
// Cluster and config subcommands talk to the admin Connect listener via
// mTLS. --admin may point at ANY cluster node — mutating commands follow
// the LeaderHint detail attached to connect.CodeUnavailable to redirect
// to the metadata leader automatically:
//
//	reflowd cluster add-node            --admin=ANY:PORT --node-id=N --raft-addr=... --gossip-addr=... --grpc-endpoint=... [--node-host-id=ID]
//	reflowd cluster remove-node         --admin=ANY:PORT --node-id=N
//	reflowd cluster nodes list          --admin=ANY:PORT
//	reflowd cluster partitions list     --admin=ANY:PORT
//	reflowd cluster snapshot create     --admin=ANY:PORT --shard=N
//	reflowd cluster snapshot list       --admin=ANY:PORT --shard=N
//	reflowd cluster snapshot delete     --admin=ANY:PORT --shard=N --index=I
//	reflowd cluster transfer-lp         --admin=ANY:PORT --lp=N --to-shard=M
//	reflowd cluster list-lp-transfers   --admin=ANY:PORT
//	reflowd cluster rebalance-advise    --admin=ANY:PORT
//	reflowd cluster rebalance-drain     --admin=ANY:PORT --shard=N [--stop]
//
//	reflowd config register-deployment  --admin=ANY:PORT --url=http://HANDLER:PORT
//	reflowd config list-deployments     --admin=ANY:PORT
//	reflowd config describe-deployment  --admin=ANY:PORT --id=DEPLOYMENT_ID
//	reflowd config delete-deployment    --admin=ANY:PORT --id=DEPLOYMENT_ID --force
//	reflowd config eventsources list    --admin=ANY:PORT
//	reflowd config eventsources delete  --admin=ANY:PORT --name=NAME
//	reflowd config webhooks list        --admin=ANY:PORT
//	reflowd config webhooks delete      --admin=ANY:PORT --name=NAME
//	reflowd config apply -f <file>      --admin=ANY:PORT
//	reflowd config export --kind=K      --admin=ANY:PORT
//	reflowd config get <kind> <name>    --admin=ANY:PORT
//	reflowd config init-kek             --blob-uri=...
//	reflowd config create-secret        --admin=ANY:PORT --name=N --kek-uri=... --blob-uri=...
//	reflowd config delete-secret        --admin=ANY:PORT --name=N
//	reflowd config list-secrets         --admin=ANY:PORT
//	reflowd config decrypt-secret       --name=N --kek-uri=... --blob-uri=...
//	reflowd config upsert-webhook       --admin=ANY:PORT --name=N --path=... --verifier=... --secret=N --service=... --handler=...
//
// Cluster and config subcommands need the operator TLS flags (or
// matching env vars):
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
	case "config":
		err = dispatchConfig(ctx, args)
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

// dispatchCluster routes "reflowd cluster <subcmd> ..." to the
// ClusterCtl-service handlers (fleet ops: membership, partitions,
// snapshots, LP transfers). App-config subcommands moved to
// `reflowd config`.
func dispatchCluster(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflowd cluster {add-node|remove-node|nodes|partitions|snapshot|transfer-lp|list-lp-transfers|tenants|tenant-deks} [flags]")
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
	case "transfer-lp":
		return cmdTransferLP(ctx, rest)
	case "list-lp-transfers":
		return cmdListLPTransfers(ctx, rest)
	case "rebalance-advise":
		return cmdRebalanceAdvise(ctx, rest)
	case "rebalance-drain":
		return cmdRebalanceDrain(ctx, rest)
	case "tenants":
		return cmdTenants(ctx, rest)
	case "tenant-deks":
		return cmdTenantDEKs(ctx, rest)
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

Cluster (ClusterCtl RPCs; fleet ops; --admin can be ANY node):
  cluster add-node              Register a new peer and start rebalance.
  cluster remove-node           Mark a peer evicted.
  cluster nodes list            List current membership.
  cluster partitions list       Print the partition table.
  cluster snapshot create       Trigger an exported snapshot of one shard.
  cluster snapshot list         List archived snapshots.
  cluster snapshot delete       Remove an archived snapshot.
  cluster transfer-lp           Move one LP to a different partition shard.
  cluster list-lp-transfers     List in-flight LP transfer records.
  cluster rebalance-advise      Dump the autonomous rebalancer's intent
                                (skew, drained shards, would-transfer set).
  cluster rebalance-drain       Mark a partition shard drained (or undrain
                                via --stop).
  cluster tenants create        Create a tenant (--name, --max-concurrent).
  cluster tenants update        Update a tenant's quotas by name.
  cluster tenants delete        Delete a tenant by name.
  cluster tenants list          List every TenantRecord.
  cluster tenants describe      Describe one TenantRecord by --id.

Config (Config RPCs; app config; --admin can be ANY node):
  config register-deployment    Register a handler deployment URL.
  config list-deployments       List every DeploymentRecord.
  config describe-deployment    Describe one DeploymentRecord by id.
  config delete-deployment      Delete a DeploymentRecord (requires --force).
  config eventsources list      List configured event sources.
  config eventsources delete    Delete an event source by name.
  config webhooks list          List configured webhook sources.
  config webhooks delete        Delete a webhook source by name.
  config apply -f <file>        Apply a multi-doc YAML file
                                (kinds: EventSource, WebhookSource).
  config export --kind=<k>      Dump a kind (or 'all') as multi-doc YAML.
  config get <kind> <name>      Fetch one record as YAML.
  config init-kek               Create a fresh BlobKMS KEK blob.
  config create-secret          Encrypt + write blob + UpsertSecret in
                                shard 0's SecretTable in one command.
                                Webhook (and future) records reference
                                the resulting row by --name.
  config delete-secret          Remove a SecretRecord from shard 0.
  config list-secrets           List SecretRecords (no plaintext).
  config decrypt-secret         Decrypt a secret blob to stdout
                                (operator self-verification only).
  config upsert-webhook         Register a webhook source referencing
                                an existing secret by --secret=NAME.
  config ca init                Generate a cluster CA, KEK-wrap the key
                                to a blob, and register both in shard 0
                                (UpsertSecret then UpsertCARoot).
  config ca list                List CARootTable rows (no signing keys).
  config ca delete              Remove one CARootTable row by name.

Run any subcommand with --help for its specific flags.
`)
}
