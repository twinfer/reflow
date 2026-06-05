package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/twinfer/reflw/internal/bootstrap"
	"github.com/twinfer/reflw/pkg/reflw"
	"github.com/twinfer/reflw/pkg/reflw/config"
)

// cmdRun is the "reflwd run" subcommand: load layered config and start
// the engine until SIGINT/SIGTERM. Configuration sources (later overrides
// earlier):
//
//  1. Built-in defaults (single-node, shard 1, sensible ports).
//  2. Optional config file from $REFLW_CONFIG (YAML or JSON).
//  3. REFLW_* environment variables.
//
// Joiner flags:
//
//	--join=<addr>                Joiner mode: exchange --join-token with
//	--join-token=<tok>           the bootstrap server, persist the
//	--root-cert-pin=sha256:<fpr> signed leaf to <data-dir>/bootstrap/,
//	                             then run normally.
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	joinAddr := fs.String("join", "", "joiner mode: bootstrap listener address (host:port)")
	joinToken := fs.String("join-token", "", "joiner mode: plaintext join token from `reflwd config create-join-token`")
	rootCertPin := fs.String("root-cert-pin", "", "joiner mode: optional SPKI pin (sha256:<hex>) the joiner verifies before sending the token")
	extraHosts := fs.String("join-hostname", "", "joiner mode: comma-separated extra DNS/IP SANs to embed in the CSR")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *joinAddr != "" && *joinToken == "" {
		return fmt.Errorf("--join requires --join-token")
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	if *joinAddr != "" {
		if err := runJoinerPreflight(*joinAddr, *joinToken, *rootCertPin, *extraHosts, cfg); err != nil {
			return fmt.Errorf("reflwd: --join failed: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	host, err := reflw.Run(ctx, cfg)
	if err != nil {
		return err
	}

	<-ctx.Done()
	slog.Default().Info("reflwd: shutting down")
	return host.Close()
}

// runJoinerPreflight dials the bootstrap listener, exchanges the token
// for a signed leaf, and writes the leaf+key+ca-chain into a sibling
// directory under cfg.Storage.DataDir. The on-disk layout (leaf.crt,
// leaf.key, ca.crt) is what operators wire into cfg.Admin.Creds /
// cfg.Delivery.Creds / cfg.Ingress.Creds via CertFile/KeyFile/CAFile so
// the subsequent reflw.Run pick up the freshly-issued material.
//
// The function exits without starting the engine if it fails — the
// joiner is expected to retry with corrected flags, not partially boot.
func runJoinerPreflight(addr, token, pin, extraHostsCSV string, cfg reflw.Config) error {
	dir := filepath.Join(cfg.Storage.DataDir, "bootstrap")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	kind := "node"
	name := "auto"
	if cfg.Node.ID != 0 {
		// Operator already chose a node_id in config; embed it in the
		// CSR so the bootstrap server enforces the alignment between
		// the join token's requested_name (often "auto") and the
		// joiner's stated identity.
		name = strconv.FormatUint(cfg.Node.ID, 10)
	}
	var extra []string
	if extraHostsCSV != "" {
		for _, h := range splitCSV(extraHostsCSV) {
			if h != "" {
				extra = append(extra, h)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := bootstrap.Join(ctx, bootstrap.JoinOptions{
		Addr:          addr,
		Token:         token,
		Kind:          kind,
		RequestedName: name,
		RootCertPin:   pin,
		ExtraHosts:    extra,
	})
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "leaf.crt"), res.CertPEM, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "leaf.key"), res.KeyPEM, 0o600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), res.CAChainPEM, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "reflwd: joiner credentials written to %s\n", dir)
	fmt.Fprintf(os.Stderr, "reflwd:   leaf.crt — signed leaf (CN=node/%d)\n", res.AssignedNodeID)
	fmt.Fprintf(os.Stderr, "reflwd:   leaf.key — private key (0600)\n")
	fmt.Fprintf(os.Stderr, "reflwd:   ca.crt   — cluster CA chain (pin %s)\n", res.CAFingerprint)
	fmt.Fprintf(os.Stderr, "reflwd: point cfg.{admin,delivery,ingress}.creds.tls at these files and restart without --join.\n")
	return nil
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// loadConfig layers built-in defaults, an optional config file, and
// REFLW_* env vars (in that order — later sources win).
func loadConfig() (reflw.Config, error) {
	sources := []config.Source{
		config.FromMap(defaultValues()),
	}
	if path := os.Getenv("REFLW_CONFIG"); path != "" {
		sources = append(sources, config.FromFile(path))
	}
	sources = append(sources, config.FromEnv())

	cfg, _, err := config.Load(sources...)
	return cfg, err
}

// defaultValues are the baked-in defaults. Picked so `reflwd run`
// works out of the box on a developer machine. Multi-node fields
// (node.gossip_bind_addr, node.delivery_addr, cluster.peers) are
// left empty by default — single-node bootstrap when they are unset.
func defaultValues() map[string]any {
	return map[string]any{
		"node.id":          uint64(1),
		"node.raft_addr":   "127.0.0.1:9091",
		"storage.data_dir": "./data",
		// Ingress is the user-facing API; reflw.Run starts it
		// unconditionally and applies this same default if the operator
		// leaves Addr empty. Surfaced here so users can see the canonical
		// port without reading library code.
		"ingress.addr":  ":8080",
		"metrics.addr":  ":9090",
		"logging.level": "INFO",
		// Admin + snapshot defaults. The admin server starts when
		// Admin.Addr is set, so leaving it populated is safe for
		// single-node out of the box. The snapshot producer is disabled
		// by default (Interval=0); operators opt in via REFLW_SNAPSHOT_
		// INTERVAL once they have a sustained DR plan.
		"admin.addr":           ":8082",
		"snapshot.retain":      24,
		"snapshot.interval":    "0s",
		"snapshot.scratch_dir": "",
	}
}
