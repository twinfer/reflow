// Command reflwd is the production reflw binary. It exposes these
// top-level subcommands:
//
//	reflwd run                 # start the engine
//	reflwd cluster <subcmd>    # fleet ops: membership, partitions,
//	                            # snapshots, LP transfers, rebalance
//	reflwd config <subcmd>     # app config: deployments, models,
//	                            # secrets, cluster authz policy; plus
//	                            # local PKI (ca init, issue-operator)
//	reflwd purge-invocation    # operator: purge a completed invocation
//
// cluster and config both dispatch to the single reflw.admin.v1.Admin
// Connect service on the mTLS admin listener (the former config +
// clusterctl services were merged into one Admin service; the CLI keeps
// the two command groups for UX). --admin may point at ANY cluster node —
// mutating commands follow the LeaderHint detail attached to
// connect.CodeUnavailable to redirect to the metadata leader automatically.
//
// PKI: the cluster CA is config + KMS, not a shard-0 table — a public CA
// cert distributed via cluster_ca.ca_cert_file plus a KMS-wrapped signing
// key (cluster_ca.key_blob_uri + key_kek_uri). `reflwd config ca init`
// mints the CA and seals its key; `reflwd config issue-operator` mints an
// operator/<name> leaf from it — both LOCAL commands (no RPC, no cluster
// connection): whoever can unwrap the KMS key can mint a cert. Each node
// self-issues its own node/<id> mesh leaf at startup, so there is no
// central issuer, no join token, and no bootstrap port. A new node joins
// via `reflwd cluster add-node` (operator-driven) or `reflwd run` with
// cluster.join_existing=true. Every leaf carries the principal Raw form
// (e.g. "node/1", "operator/alice") in its CN, which the reflw mTLS layer
// and the Cedar authz policy match against.
//
//	reflwd cluster add-node            --admin=ANY:PORT --node-id=N --raft-addr=... --gossip-addr=... --grpc-endpoint=... [--node-host-id=ID]
//	reflwd cluster remove-node         --admin=ANY:PORT --node-id=N
//	reflwd cluster nodes list          --admin=ANY:PORT
//	reflwd cluster partitions list     --admin=ANY:PORT
//	reflwd cluster snapshot create     --admin=ANY:PORT --shard=N
//	reflwd cluster snapshot list       --admin=ANY:PORT --shard=N
//	reflwd cluster snapshot delete     --admin=ANY:PORT --shard=N --index=I
//	reflwd cluster transfer-lp         --admin=ANY:PORT --lp=N --to-shard=M
//	reflwd cluster list-lp-transfers   --admin=ANY:PORT
//	reflwd cluster rebalance-advise    --admin=ANY:PORT
//	reflwd cluster rebalance-drain     --admin=ANY:PORT --shard=N [--stop]
//
//	reflwd config register-deployment         --admin=ANY:PORT --url=http://HANDLER:PORT
//	reflwd config list-deployments            --admin=ANY:PORT
//	reflwd config describe-deployment         --admin=ANY:PORT --id=DEPLOYMENT_ID
//	reflwd config delete-deployment           --admin=ANY:PORT --id=DEPLOYMENT_ID --force
//	reflwd config register-model              --admin=ANY:PORT --file=M.bpmn --kind=bpmn --name=N --version=V  (or --manifest=set.json)
//	reflwd config list-models                 --admin=ANY:PORT
//	reflwd config describe-model              --admin=ANY:PORT --kind=bpmn --name=N [--version=V]
//	reflwd config delete-model                --admin=ANY:PORT --kind=bpmn --name=N
//	reflwd config create-secret               --admin=ANY:PORT --name=N --kek-uri=... --blob-uri=...
//	reflwd config delete-secret               --admin=ANY:PORT --name=N
//	reflwd config list-secrets                --admin=ANY:PORT
//	reflwd config upsert-cluster-authz-policy --admin=ANY:PORT -f policy.cedar [--if-revision=R]
//	reflwd config get-cluster-authz-policy    --admin=ANY:PORT
//	reflwd config ca init                     --kek-uri=... --key-blob-uri=... [--ca-cert-out=ca.crt]   # LOCAL
//	reflwd config issue-operator              --name=N --ca-cert-file=... --key-blob-uri=... --key-kek-uri=... [--out=.]  # LOCAL
//	reflwd config init-kek                    --blob-uri=...   # LOCAL
//	reflwd config decrypt-secret              --name=N --kek-uri=... --blob-uri=...   # LOCAL
//
// The Admin RPCs (everything above except the LOCAL PKI/KEK commands)
// need the operator TLS flags (or matching env vars):
//
//	--client-cert   $REFLW_CLIENT_CERT
//	--client-key    $REFLW_CLIENT_KEY
//	--ca            $REFLW_CA_CERT
//
// `reflwd run` reads layered configuration sources (later overrides
// earlier):
//
//  1. Built-in defaults (single-node, shard 1, sensible ports).
//  2. Optional config file from $REFLW_CONFIG (YAML or JSON).
//  3. REFLW_* environment variables.
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
	case "cluster":
		err = dispatchCluster(ctx, args)
	case "config":
		err = dispatchConfig(ctx, args)
	case "purge-invocation":
		err = cmdPurgeInvocation(ctx, args)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return
	default:
		fmt.Fprintf(os.Stderr, "reflwd: unknown subcommand %q\n\n", cmd)
		usage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "reflwd: %v\n", err)
		os.Exit(1)
	}
}

// dispatchCluster routes "reflwd cluster <subcmd> ..." to the
// ClusterCtl-service handlers (fleet ops: membership, partitions,
// snapshots, LP transfers). App-config subcommands moved to
// `reflwd config`.
func dispatchCluster(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflwd cluster {add-node|remove-node|nodes|partitions|snapshot|transfer-lp|list-lp-transfers} [flags]")
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
	default:
		return fmt.Errorf("reflwd cluster: unknown subcommand %q", sub)
	}
}

func usage(w *os.File) {
	fmt.Fprint(w, `reflwd — reflw engine + admin CLI

Engine:
  run                  Start the engine. Reads layered config:
                         defaults → $REFLW_CONFIG file → REFLW_* env.

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

Config (Config RPCs; app config; --admin can be ANY node):
  config register-deployment    Register a handler deployment URL.
  config list-deployments       List every DeploymentRecord.
  config describe-deployment    Describe one DeploymentRecord by id.
  config delete-deployment      Delete a DeploymentRecord (requires --force).
  config init-kek               Create a fresh BlobKMS KEK blob.
  config create-secret          Encrypt + write blob + UpsertSecret in
                                shard 0's SecretTable in one command.
                                Consumers reference the resulting row
                                by --name.
  config delete-secret          Remove a SecretRecord from shard 0.
  config list-secrets           List SecretRecords (no plaintext).
  config decrypt-secret         Decrypt a secret blob to stdout
                                (operator self-verification only).
  config ca init                Mint a cluster CA locally: KEK-wrap the
                                signing key to a blob, write the public
                                cert, and print the cluster_ca config
                                block for every node. No cluster needed.
  config issue-operator         Mint an operator client cert locally from
                                the cluster CA (--name, --ca-cert-file,
                                --key-blob-uri, --key-kek-uri).

Maintenance (Ingress RPC; operator-only; --ingress targets a node hosting
the invocation's shard):
  purge-invocation              Immediately delete a Completed invocation's
                                durable rows (status, journal, signals)
                                instead of waiting for the retention reaper.

Run any subcommand with --help for its specific flags.
`)
}
