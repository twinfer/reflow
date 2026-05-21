package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	connect "connectrpc.com/connect"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"gocloud.dev/blob"

	// Always-linked KMS providers — the CLI shares the same Tink
	// registry as the engine, so any URI scheme accepted by `reflowd
	// run` is also accepted here.
	_ "github.com/twinfer/reflow/pkg/kms/awskms"
	tinkkmsblob "github.com/twinfer/reflow/pkg/kms/blob"
	_ "github.com/twinfer/reflow/pkg/kms/gcpkms"

	"github.com/twinfer/reflow/pkg/adminclient"
	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// cmdInitKEK creates a fresh BlobKMS KEK blob (boot key + encrypted
// Tink AEAD keyset) at the configured blob URI.
//
// One-time setup per cluster. Operators run this once, then point
// every create-secret invocation at the resulting blobkms+<uri>.
func cmdInitKEK(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("init-kek", flag.ContinueOnError)
	blobURI := fs.String("blob-uri", "", "gocloud.dev/blob URI where the KEK blob lands (required)")
	force := fs.Bool("force", false, "overwrite an existing blob at --blob-uri")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *blobURI == "" {
		return errors.New("--blob-uri is required")
	}
	bucketURI, key, err := parseBlobURI(*blobURI)
	if err != nil {
		return err
	}
	bkt, err := blob.OpenBucket(ctx, bucketURI)
	if err != nil {
		return fmt.Errorf("open bucket %q: %w", bucketURI, err)
	}
	defer bkt.Close()
	if !*force {
		exists, err := bkt.Exists(ctx, key)
		if err != nil {
			return fmt.Errorf("check existing blob: %w", err)
		}
		if exists {
			return fmt.Errorf("blob %q already exists at %s; pass --force to overwrite", key, bucketURI)
		}
	}
	raw, err := tinkkmsblob.InitKEK()
	if err != nil {
		return fmt.Errorf("InitKEK: %w", err)
	}
	if err := bkt.WriteAll(ctx, key, raw, nil); err != nil {
		return fmt.Errorf("write KEK blob: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote KEK to %s (size=%d)\n", *blobURI, len(raw))
	fmt.Fprintf(os.Stderr, "kek_uri to pass to create-secret: %s%s\n", tinkkmsblob.URIPrefix, *blobURI)
	return nil
}

// cmdCreateSecret reads plaintext from stdin (or --input), encrypts it
// under the KEK at --kek-uri with AAD=--name, writes the ciphertext to
// --blob-uri, and then proposes Admin.UpsertSecret so every node's
// SecretStore Resolver picks the new ciphertext up on the next
// reconcile.
//
// One command, three actions: encrypt → write blob → register in
// shard 0. Subsequent webhook (or future) records reference the
// secret by name (--name argument).
func cmdCreateSecret(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("create-secret", flag.ContinueOnError)
	name := fs.String("name", "", "secret name (used as AAD and as the SecretTable key; required)")
	kekURI := fs.String("kek-uri", "", "Tink KMS URI (e.g. blobkms+file:///etc/reflow/kek.bin) (required)")
	blobURI := fs.String("blob-uri", "", "ciphertext destination URI (gocloud.dev/blob) (required)")
	input := fs.String("input", "", "read plaintext from this file (default: stdin)")
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := checkRequired(map[string]string{
		"--name":     *name,
		"--kek-uri":  *kekURI,
		"--blob-uri": *blobURI,
	}); err != nil {
		return err
	}

	pt, err := readPlaintext(*input)
	if err != nil {
		return err
	}
	if len(pt) == 0 {
		return errors.New("plaintext is empty (stdin / --input read 0 bytes)")
	}
	if err := encryptToBlob(ctx, *name, *kekURI, *blobURI, pt); err != nil {
		return err
	}

	rec := &enginev1.SecretRecord{
		Name: *name,
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: *blobURI,
				KekUri:  *kekURI,
			},
		},
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1connect.AdminClient) error {
		list, err := cli.ListSecrets(rctx, connect.NewRequest(&adminv1.ListSecretsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.UpsertSecret(rctx, connect.NewRequest(&adminv1.UpsertSecretRequest{
			Record:            rec,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return fmt.Errorf("UpsertSecret: %w", err)
		}
		fmt.Fprintf(os.Stderr, "secret upserted (name=%s, table_revision=%d)\n",
			rec.GetName(), resp.Msg.GetTableRevision())
		return nil
	})
}

// cmdDeleteSecret removes the named SecretRecord. Does NOT
// cascade-validate consumer references — webhook (and future) records
// that still name this secret will fail to resolve on next reconcile
// and the consumer's preserve-prev-on-error semantics will keep them
// serving until operators clean up.
func cmdDeleteSecret(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete-secret", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "secret name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1connect.AdminClient) error {
		list, err := cli.ListSecrets(rctx, connect.NewRequest(&adminv1.ListSecretsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.DeleteSecret(rctx, connect.NewRequest(&adminv1.DeleteSecretRequest{
			Name:              *name,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "secret deleted (name=%s, table_revision=%d)\n",
			*name, resp.Msg.GetTableRevision())
		return nil
	})
}

// cmdListSecrets prints every SecretRecord in shard 0 as JSON. No
// plaintext, no decrypt — just the persisted blob_uri / kek_uri
// pointers.
func cmdListSecrets(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-secrets", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *adminclient.Client) error {
		resp, err := cli.Admin.ListSecrets(ctx, connect.NewRequest(&adminv1.ListSecretsRequest{}))
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

// cmdDecryptSecret reads ciphertext from --blob-uri, decrypts it under
// the KEK at --kek-uri with AAD=--name, and writes the plaintext to
// stdout. Operator self-verification only — refuses to write to a TTY
// unless --allow-tty is set, so the plaintext doesn't end up in
// scrollback by accident.
func cmdDecryptSecret(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("decrypt-secret", flag.ContinueOnError)
	name := fs.String("name", "", "secret name (used as AAD; required)")
	kekURI := fs.String("kek-uri", "", "Tink KMS URI (required)")
	blobURI := fs.String("blob-uri", "", "ciphertext source URI (gocloud.dev/blob; required)")
	allowTTY := fs.Bool("allow-tty", false, "write plaintext to terminal scrollback (off by default)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := checkRequired(map[string]string{
		"--name":     *name,
		"--kek-uri":  *kekURI,
		"--blob-uri": *blobURI,
	}); err != nil {
		return err
	}
	if !*allowTTY && isTerminal(os.Stdout) {
		return errors.New("refusing to write plaintext to a terminal; pipe stdout or pass --allow-tty")
	}

	bucketURI, key, err := parseBlobURI(*blobURI)
	if err != nil {
		return err
	}
	bkt, err := blob.OpenBucket(ctx, bucketURI)
	if err != nil {
		return fmt.Errorf("open bucket %q: %w", bucketURI, err)
	}
	defer bkt.Close()
	ct, err := bkt.ReadAll(ctx, key)
	if err != nil {
		return fmt.Errorf("read ciphertext: %w", err)
	}

	kc, err := registry.GetKMSClient(*kekURI)
	if err != nil {
		return fmt.Errorf("GetKMSClient(%q): %w", *kekURI, err)
	}
	aead, err := kc.GetAEAD(*kekURI)
	if err != nil {
		return fmt.Errorf("GetAEAD(%q): %w", *kekURI, err)
	}
	pt, err := aead.Decrypt(ct, []byte(*name))
	if err != nil {
		return fmt.Errorf("AEAD.Decrypt: %w", err)
	}
	if _, err := os.Stdout.Write(pt); err != nil {
		return fmt.Errorf("write stdout: %w", err)
	}
	return nil
}

// cmdUpsertWebhook proposes Admin.UpsertWebhookSource for a webhook
// record that references an existing SecretRecord by name. The secret
// itself is created separately via `cluster create-secret`.
func cmdUpsertWebhook(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("upsert-webhook", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "webhook name (required)")
	path := fs.String("path", "", "webhook URL path (required)")
	verifier := fs.String("verifier", "", "verifier name (required)")
	secretName := fs.String("secret", "", "SecretTable row name to resolve the HMAC secret from (required)")
	service := fs.String("service", "", "target service (required)")
	handler := fs.String("handler", "", "target handler (required)")
	objectKey := fs.String("object-key", "", "target object key (optional)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := checkRequired(map[string]string{
		"--name":     *name,
		"--path":     *path,
		"--verifier": *verifier,
		"--secret":   *secretName,
		"--service":  *service,
		"--handler":  *handler,
	}); err != nil {
		return err
	}
	rec := &enginev1.WebhookSourceRecord{
		Name:       *name,
		Path:       *path,
		Verifier:   *verifier,
		SecretName: *secretName,
		Service:    *service,
		Handler:    *handler,
		ObjectKey:  *objectKey,
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1connect.AdminClient) error {
		list, err := cli.ListWebhookSources(rctx, connect.NewRequest(&adminv1.ListWebhookSourcesRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.UpsertWebhookSource(rctx, connect.NewRequest(&adminv1.UpsertWebhookSourceRequest{
			Record:            rec,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return fmt.Errorf("UpsertWebhookSource: %w", err)
		}
		fmt.Fprintf(os.Stderr, "webhook upserted (name=%s, table_revision=%d)\n",
			rec.GetName(), resp.Msg.GetTableRevision())
		return nil
	})
}

// encryptToBlob encrypts plaintext with the KEK at kekURI (AAD = name)
// and writes the ciphertext to blobURI via gocloud.dev/blob. Split out
// of cmdCreateSecret so tests can exercise the file ops in isolation,
// without needing a live admin endpoint to call UpsertSecret.
func encryptToBlob(ctx context.Context, name, kekURI, blobURI string, plaintext []byte) error {
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		return fmt.Errorf("GetKMSClient(%q): %w", kekURI, err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		return fmt.Errorf("GetAEAD(%q): %w", kekURI, err)
	}
	ct, err := aead.Encrypt(plaintext, []byte(name))
	if err != nil {
		return fmt.Errorf("AEAD.Encrypt: %w", err)
	}
	bucketURI, key, err := parseBlobURI(blobURI)
	if err != nil {
		return err
	}
	bkt, err := blob.OpenBucket(ctx, bucketURI)
	if err != nil {
		return fmt.Errorf("open bucket %q: %w", bucketURI, err)
	}
	if err := bkt.WriteAll(ctx, key, ct, nil); err != nil {
		_ = bkt.Close()
		return fmt.Errorf("write ciphertext: %w", err)
	}
	if err := bkt.Close(); err != nil {
		return fmt.Errorf("close bucket: %w", err)
	}
	fmt.Fprintf(os.Stderr, "wrote ciphertext to %s (name=%q, plaintext=%d B, ciphertext=%d B)\n",
		blobURI, name, len(plaintext), len(ct))
	return nil
}

// readPlaintext reads from a file path, or from stdin when path is "".
func readPlaintext(path string) ([]byte, error) {
	if path == "" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

// parseBlobURI splits a gocloud.dev/blob URI into (bucketURI, key).
// Mirrors the BlobKMS splitter shape — same parsing rules so operators
// don't have to learn two URI conventions.
func parseBlobURI(uri string) (bucketURI, key string, err error) {
	schemeEnd := strings.Index(uri, "://")
	if schemeEnd < 0 {
		return "", "", fmt.Errorf("URI %q missing scheme://", uri)
	}
	pathStart := schemeEnd + len("://")
	slash := strings.LastIndex(uri[pathStart:], "/")
	if slash < 0 {
		return "", "", fmt.Errorf("URI %q missing object key (no '/' after authority)", uri)
	}
	cut := pathStart + slash
	bucketURI = uri[:cut]
	key = uri[cut+1:]
	if key == "" {
		return "", "", fmt.Errorf("URI %q has empty object key", uri)
	}
	return bucketURI, key, nil
}

func checkRequired(flags map[string]string) error {
	missing := make([]string, 0, len(flags))
	for k, v := range flags {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required flags: %s", strings.Join(missing, ", "))
	}
	return nil
}

// isTerminal returns true when f is connected to a tty. Best-effort:
// Stat lets us distinguish character devices from pipes/files without
// importing golang.org/x/term.
func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
