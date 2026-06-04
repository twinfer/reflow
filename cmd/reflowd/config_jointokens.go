package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// cmdCreateJoinToken mints a one-time bootstrap credential and prints
// the plaintext exactly once. Operators forward the plaintext to the
// joiner; subsequent List calls show only the hash.
func cmdCreateJoinToken(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("create-join-token", flag.ContinueOnError)
	tlsFlags := registerTLSFlags(fs)
	kind := fs.String("kind", "node", "node | operator")
	name := fs.String("name", "auto", "requested_name; for kind=node use 'auto' to let the bootstrap server allocate a node_id")
	ttl := fs.Duration("ttl", 10*time.Minute, "token TTL")
	singleUse := fs.Bool("single-use", true, "true = first redemption marks the row spent; false = reusable until expiry")
	if err := fs.Parse(args); err != nil {
		return err
	}
	tokenKind, err := parseTokenKind(*kind)
	if err != nil {
		return err
	}
	return tlsFlags.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.CreateJoinToken(rctx, connect.NewRequest(&configv1.CreateJoinTokenRequest{
			Kind:          tokenKind,
			RequestedName: *name,
			TtlSeconds:    uint64(ttl.Seconds()),
			SingleUse:     *singleUse,
		}))
		if err != nil {
			return err
		}
		// Token plaintext goes to stdout exactly once. The hint goes to
		// stderr so callers piping the token into a flag get just the
		// secret on stdout.
		fmt.Println(resp.Msg.GetToken())
		fmt.Fprintf(os.Stderr, "join token created (kind=%s, name=%s, ttl=%s, single_use=%t, hash=%s, table_revision=%d)\n",
			*kind, *name, *ttl, *singleUse,
			hex.EncodeToString(resp.Msg.GetTokenHash()),
			resp.Msg.GetTableRevision())
		fmt.Fprintf(os.Stderr, "redeem on the joiner: reflowd run --join=<this-node>:<bootstrap-port> --join-token=<token>\n")
		return nil
	})
}

// cmdListJoinTokens prints every JoinTokenRecord as JSON. Only the
// hash is visible — plaintext was emitted at create time.
func cmdListJoinTokens(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-join-tokens", flag.ContinueOnError)
	tlsFlags := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tlsFlags.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Config.ListJoinTokens(ctx, connect.NewRequest(&configv1.ListJoinTokensRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"records":        resp.Msg.GetRecords(),
		})
	})
}

// cmdDeleteJoinToken removes a token by hash. Useful for revoking a
// pending-but-not-yet-redeemed token.
func cmdDeleteJoinToken(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete-join-token", flag.ContinueOnError)
	tlsFlags := registerTLSFlags(fs)
	hexHash := fs.String("hash", "", "hex-encoded token hash (from `list-join-tokens`)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *hexHash == "" {
		return errors.New("--hash is required")
	}
	hash, err := hex.DecodeString(*hexHash)
	if err != nil {
		return fmt.Errorf("decode --hash: %w", err)
	}
	return tlsFlags.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		list, err := cli.Config.ListJoinTokens(rctx, connect.NewRequest(&configv1.ListJoinTokensRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.Config.DeleteJoinToken(rctx, connect.NewRequest(&configv1.DeleteJoinTokenRequest{
			TokenHash:         hash,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "join token deleted (hash=%s, table_revision=%d)\n",
			*hexHash, resp.Msg.GetTableRevision())
		return nil
	})
}

// cmdIssueOperator generates an ECDSA-P256 keypair locally, sends a
// CSR with CN=operator/<name> to the cluster, and writes the signed
// leaf + key + CA chain into --out. Replaces the deleted
// `reflowd pki issue-operator` flow.
func cmdIssueOperator(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("issue-operator", flag.ContinueOnError)
	tlsFlags := registerTLSFlags(fs)
	name := fs.String("name", "", "operator name (required); becomes CN=operator/<name>")
	out := fs.String("out", "", "output directory for operator-<name>.{crt,key,ca.crt}; default: ~/.reflow/operator-<name>")
	validity := fs.Duration("validity", 30*24*time.Hour, "leaf validity (clamped by server)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	outDir := *out
	if outDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("resolve home dir: %w", err)
		}
		outDir = filepath.Join(home, ".reflow", "operator-"+*name)
	}
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return fmt.Errorf("create %s: %w", outDir, err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate key: %w", err)
	}
	csrTpl := &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "operator/" + *name},
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, csrTpl, key)
	if err != nil {
		return fmt.Errorf("build CSR: %w", err)
	}

	return tlsFlags.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.IssueOperator(rctx, connect.NewRequest(&configv1.IssueOperatorRequest{
			CsrDer:          csrDER,
			ValiditySeconds: uint64(validity.Seconds()),
		}))
		if err != nil {
			return err
		}
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			return err
		}
		certPath := filepath.Join(outDir, "operator-"+*name+".crt")
		keyPath := filepath.Join(outDir, "operator-"+*name+".key")
		caPath := filepath.Join(outDir, "ca.crt")
		if err := os.WriteFile(certPath, resp.Msg.GetCertPem(), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(keyPath,
			pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
			0o600); err != nil {
			return err
		}
		if err := os.WriteFile(caPath, resp.Msg.GetCaChainPem(), 0o644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "operator credential written:\n")
		fmt.Fprintf(os.Stderr, "  %s\n", certPath)
		fmt.Fprintf(os.Stderr, "  %s\n", keyPath)
		fmt.Fprintf(os.Stderr, "  %s\n", caPath)
		fmt.Fprintf(os.Stderr, "trust anchor pin: %s\n", resp.Msg.GetCaFingerprint())
		return nil
	})
}

func parseTokenKind(s string) (enginev1.JoinTokenKind, error) {
	switch s {
	case "node":
		return enginev1.JoinTokenKind_JOIN_TOKEN_KIND_NODE, nil
	case "operator":
		return enginev1.JoinTokenKind_JOIN_TOKEN_KIND_OPERATOR, nil
	default:
		return 0, fmt.Errorf("unknown kind %q (want node or operator)", s)
	}
}
