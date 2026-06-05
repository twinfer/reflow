package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/internal/certmgr"
	"github.com/twinfer/reflw/pkg/reflwclient"
	configv1 "github.com/twinfer/reflw/proto/configv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// cmdCAInit generates a fresh cluster CA, encrypts the signing key
// under --kek-uri (AAD = --secret-name), writes the ciphertext to
// --key-blob-uri, then proposes Config.UpsertSecret followed by
// Config.UpsertCARoot. After this returns, every node's
// certmgr.ClusterIssuer picks up the new active CA on its next
// reconcile.
//
// Idempotent on --row-name: re-running overwrites the existing row's
// cert + key_secret_name. The signing key blob is independent — point
// at a fresh --key-blob-uri or pass --force to overwrite.
func cmdCAInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ca init", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	rowName := fs.String("row-name", "active", "CARootTable row name (default: active)")
	secretName := fs.String("secret-name", "ca/root/active", "SecretTable row name for the signing key")
	kekURI := fs.String("kek-uri", "", "Tink KMS URI for wrapping the signing key (required)")
	keyBlobURI := fs.String("key-blob-uri", "", "ciphertext destination for the wrapped signing key (gocloud.dev/blob; required)")
	cn := fs.String("ca-cn", "reflw-cluster-ca", "CA subject CommonName")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := checkRequired(map[string]string{
		"--kek-uri":      *kekURI,
		"--key-blob-uri": *keyBlobURI,
	}); err != nil {
		return err
	}

	ca, err := certmgr.MintCA(*cn)
	if err != nil {
		return fmt.Errorf("generate CA: %w", err)
	}
	if err := encryptToBlob(ctx, *secretName, *kekURI, *keyBlobURI, ca.KeyPEM); err != nil {
		return err
	}
	fingerprint := spkiFingerprint(ca.Cert.RawSubjectPublicKeyInfo)

	secretRec := &enginev1.SecretRecord{
		Name: *secretName,
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: *keyBlobURI,
				KekUri:  *kekURI,
			},
		},
	}
	rootRec := &enginev1.CARootRecord{
		Name:          *rowName,
		CertPem:       ca.CertPEM,
		KeySecretName: *secretName,
		Fingerprint:   fingerprint,
		RotationEpoch: uint32(time.Now().Unix()),
		CreatedAtMs:   uint64(time.Now().UnixMilli()),
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflwclient.Client) error {
		sec, err := cli.Config.ListSecrets(rctx, connect.NewRequest(&configv1.ListSecretsRequest{}))
		if err != nil {
			return fmt.Errorf("read secret revision: %w", err)
		}
		if _, err := cli.Config.UpsertSecret(rctx, connect.NewRequest(&configv1.UpsertSecretRequest{
			Record:            secretRec,
			IfTableRevisionEq: sec.Msg.GetTableRevision(),
		})); err != nil {
			return fmt.Errorf("UpsertSecret: %w", err)
		}
		fmt.Fprintf(os.Stderr, "signing key registered (secret_name=%s)\n", *secretName)

		roots, err := cli.Config.ListCARoots(rctx, connect.NewRequest(&configv1.ListCARootsRequest{}))
		if err != nil {
			return fmt.Errorf("read caroot revision: %w", err)
		}
		resp, err := cli.Config.UpsertCARoot(rctx, connect.NewRequest(&configv1.UpsertCARootRequest{
			Record:            rootRec,
			IfTableRevisionEq: roots.Msg.GetTableRevision(),
		}))
		if err != nil {
			return fmt.Errorf("UpsertCARoot: %w", err)
		}
		fmt.Fprintf(os.Stderr, "CA root registered (row=%s, fingerprint=%s, table_revision=%d)\n",
			*rowName, fingerprint, resp.Msg.GetTableRevision())
		return nil
	})
}

// cmdCAList prints every CARootRecord in shard 0 as JSON. The cert
// PEM is included; the signing key never is.
func cmdCAList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ca list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Config.ListCARoots(ctx, connect.NewRequest(&configv1.ListCARootsRequest{}))
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

// cmdCADelete removes a CARootRecord by name. Does NOT cascade to the
// referenced SecretTable row; operators delete the secret separately if
// rotation is complete.
func cmdCADelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ca delete", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "CARootTable row name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflwclient.Client) error {
		list, err := cli.Config.ListCARoots(rctx, connect.NewRequest(&configv1.ListCARootsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.Config.DeleteCARoot(rctx, connect.NewRequest(&configv1.DeleteCARootRequest{
			Name:              *name,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "CA root deleted (name=%s, table_revision=%d)\n",
			*name, resp.Msg.GetTableRevision())
		return nil
	})
}

// dispatchCA routes "reflwd config ca <subcmd>" to the right handler.
func dispatchCA(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflwd config ca {init|list|delete} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "init":
		return cmdCAInit(ctx, rest)
	case "list":
		return cmdCAList(ctx, rest)
	case "delete":
		return cmdCADelete(ctx, rest)
	default:
		return fmt.Errorf("reflwd config ca: unknown subcommand %q", sub)
	}
}

// spkiFingerprint is the operator-facing trust-anchor pin format used
// across creds + the bootstrap CLI: sha256:<lowercase-hex>(SPKI).
// Matches pkg/reflw/creds.SPKIFingerprint exactly.
func spkiFingerprint(spki []byte) string {
	sum := sha256.Sum256(spki)
	return "sha256:" + hex.EncodeToString(sum[:])
}
