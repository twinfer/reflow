package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/twinfer/reflw/internal/certmgr"
	"github.com/twinfer/reflw/internal/secretstore"
	"github.com/twinfer/reflw/pkg/reflw"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// cmdCAInit mints a fresh cluster CA locally, seals the signing key under
// --kek-uri (AAD = reflw.ClusterCAKeyAAD) to --key-blob-uri, and writes
// the public CA cert to --ca-cert-out. It then prints the cluster_ca
// config block for operators to copy into every node's config.
//
// This is a fully local command — it talks to the KMS + blob store but
// never to the cluster. The cluster CA is config + KMS (public cert in
// config, KMS-wrapped key at a blob URI); each node self-issues its own
// mesh leaf from it at startup, so there is no shard-0 CA row, no join
// token, and no central issuer.
func cmdCAInit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("ca init", flag.ContinueOnError)
	kekURI := fs.String("kek-uri", "", "Tink KMS URI for wrapping the signing key (required)")
	keyBlobURI := fs.String("key-blob-uri", "", "ciphertext destination for the wrapped signing key (gocloud.dev/blob; required)")
	caCertOut := fs.String("ca-cert-out", "ca.crt", "path to write the public CA certificate PEM")
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
	// Seal with the fixed cluster-CA AAD so this ciphertext can't be
	// resolved as some other (name-AAD) secret and vice versa.
	if err := encryptToBlob(ctx, reflw.ClusterCAKeyAAD, *kekURI, *keyBlobURI, ca.KeyPEM); err != nil {
		return err
	}
	if err := os.WriteFile(*caCertOut, ca.CertPEM, 0o644); err != nil {
		return fmt.Errorf("write CA cert %s: %w", *caCertOut, err)
	}
	fmt.Fprintf(os.Stderr, "cluster CA minted: cert=%s, signing key sealed to %s\n", *caCertOut, *keyBlobURI)
	fmt.Fprintf(os.Stderr, "\nAdd to every node's config so each node self-issues its mesh leaf:\n\n")
	fmt.Printf("cluster_ca:\n")
	fmt.Printf("  ca_cert_file: %s\n", *caCertOut)
	fmt.Printf("  key_blob_uri: %s\n", *keyBlobURI)
	fmt.Printf("  key_kek_uri:  %s\n", *kekURI)
	return nil
}

// cmdIssueOperator mints an operator client cert locally from the cluster
// CA (public cert from --ca-cert-file, key KMS-unwrapped from
// --key-blob-uri). Whoever can unwrap the CA key can mint a cert — the
// same authority every node already has — so issuance needs no cluster
// round-trip. Writes operator.crt / operator.key / ca.crt to --out.
func cmdIssueOperator(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("issue-operator", flag.ContinueOnError)
	name := fs.String("name", "", "operator name → CN operator/<name> (required)")
	caCertFile := fs.String("ca-cert-file", "", "cluster CA certificate PEM (required)")
	keyBlobURI := fs.String("key-blob-uri", "", "wrapped CA key blob URI (required)")
	kekURI := fs.String("key-kek-uri", "", "KEK URI to unwrap the CA key (required)")
	outDir := fs.String("out", ".", "directory to write operator.crt / operator.key / ca.crt")
	validity := fs.Duration("validity", 30*24*time.Hour, "leaf validity")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := checkRequired(map[string]string{
		"--name":         *name,
		"--ca-cert-file": *caCertFile,
		"--key-blob-uri": *keyBlobURI,
		"--key-kek-uri":  *kekURI,
	}); err != nil {
		return err
	}

	certPEM, err := os.ReadFile(*caCertFile)
	if err != nil {
		return fmt.Errorf("read CA cert: %w", err)
	}
	resolveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	keyPEM, err := secretstore.ResolveRemoteEncrypted(resolveCtx, &enginev1.RemoteEncryptedSecret{
		BlobUri: *keyBlobURI,
		KekUri:  *kekURI,
	}, []byte(reflw.ClusterCAKeyAAD), nil)
	if err != nil {
		return fmt.Errorf("unwrap CA key: %w", err)
	}
	ca, err := certmgr.ParseCA(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("load CA: %w", err)
	}
	leafCert, leafKey, err := ca.IssueLeaf(certmgr.IssueLeafOptions{
		Kind:     certmgr.CALeafOperator,
		Name:     *name,
		Validity: *validity,
	})
	if err != nil {
		return fmt.Errorf("issue operator leaf: %w", err)
	}
	if _, _, err := certmgr.WriteLeaf(*outDir, "operator", leafCert, leafKey); err != nil {
		return fmt.Errorf("write operator leaf: %w", err)
	}
	if err := os.WriteFile(filepath.Join(*outDir, "ca.crt"), ca.CertPEM, 0o644); err != nil {
		return fmt.Errorf("write ca.crt: %w", err)
	}
	fmt.Fprintf(os.Stderr, "operator cert issued: CN=operator/%s → %s/{operator.crt,operator.key,ca.crt}\n", *name, *outDir)
	return nil
}

// dispatchCA routes "reflwd config ca <subcmd>".
func dispatchCA(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflwd config ca init [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "init":
		return cmdCAInit(ctx, rest)
	default:
		return fmt.Errorf("reflwd config ca: unknown subcommand %q", sub)
	}
}
