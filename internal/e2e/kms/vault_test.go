//go:build e2e

package kms_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	vaultapi "github.com/hashicorp/vault/api"
	hcvaulttink "github.com/tink-crypto/tink-go-hcvault/v2/integration/hcvault"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/twinfer/reflw/internal/e2e"
	"github.com/twinfer/reflw/internal/secretstore"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

const (
	vaultImage     = "hashicorp/vault:1.18"
	vaultDevToken  = "root"
	vaultTransitKey = "reflw-test"
)

// vaultBackend bundles the dev-mode container's host endpoint
// (host:port form, sans scheme) plus the token. Tests build a Tink
// hcvault client against this endpoint and register it at a host-
// specific URI prefix so each test's vault container routes to its
// own client without sync.Once / registry collisions.
type vaultBackend struct {
	hostport string // 127.0.0.1:55321
	token    string
}

// addr returns the http://host:port the vault api.Client takes.
func (vb *vaultBackend) addr() string { return "http://" + vb.hostport }

// uriPrefix returns the host-pinned Tink prefix this backend claims.
// Each test gets a different prefix (different port), so re-registering
// in Tink's process-global registry is safe across many test runs.
func (vb *vaultBackend) uriPrefix() string { return "hcvault://" + vb.hostport + "/" }

// uriFor returns the full hcvault URI for the standard transit key.
func (vb *vaultBackend) uriFor(keyName string) string {
	return vb.uriPrefix() + "transit/keys/" + keyName
}

// startVaultDev spins up `hashicorp/vault` in dev mode (single-node,
// in-memory storage, root token = "root"), then enables the transit
// secrets engine and creates one key for the test.
//
// Cleanup: container is terminated via t.Cleanup. The registered Tink
// client persists in the global registry (no unregister API), which
// is fine because the prefix is host:port-pinned and dead.
func startVaultDev(t *testing.T) *vaultBackend {
	t.Helper()
	e2e.SkipUnlessDocker(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        vaultImage,
		ExposedPorts: []string{"8200/tcp"},
		Env: map[string]string{
			"VAULT_DEV_ROOT_TOKEN_ID":  vaultDevToken,
			"VAULT_DEV_LISTEN_ADDRESS": "0.0.0.0:8200",
		},
		// The vault image's docker-entrypoint.sh detects
		// VAULT_DEV_ROOT_TOKEN_ID and invokes `vault server -dev`.
		// Explicit cmd would override the entrypoint logic.
		// IPC_LOCK is required for mlock(); without it vault logs
		// a warning and runs in degraded-but-functional mode, which
		// is fine for tests.
		CapAdd: []string{"IPC_LOCK"},
		// /v1/sys/health returns 200 when unsealed + active. In dev
		// mode this happens essentially immediately, but the http
		// listener startup still takes a moment.
		WaitingFor: wait.ForHTTP("/v1/sys/health").
			WithPort("8200/tcp").
			WithStatusCodeMatcher(func(s int) bool {
				return s == http.StatusOK ||
					s == http.StatusTooManyRequests // 429: standby, still alive
			}).
			WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("vault container unavailable: %v", err)
	}
	t.Cleanup(func() {
		_ = testcontainers.TerminateContainer(c)
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("vault host: %v", err)
	}
	port, err := c.MappedPort(ctx, "8200/tcp")
	if err != nil {
		t.Fatalf("vault mapped port: %v", err)
	}
	vb := &vaultBackend{
		hostport: fmt.Sprintf("%s:%s", host, port.Port()),
		token:    vaultDevToken,
	}
	vb.enableTransit(t)
	vb.createTransitKey(t, vaultTransitKey)
	return vb
}

// enableTransit POSTs /v1/sys/mounts/transit so the transit secrets
// engine is available. Dev mode does NOT enable transit by default —
// the test must opt in explicitly.
func (vb *vaultBackend) enableTransit(t *testing.T) {
	t.Helper()
	body := bytes.NewReader([]byte(`{"type":"transit"}`))
	vb.doVaultAPI(t, http.MethodPost, "/v1/sys/mounts/transit", body, http.StatusNoContent)
}

// createTransitKey POSTs /v1/transit/keys/<name>. Vault accepts an
// empty body and uses sensible defaults (aes256-gcm96, derivation
// disabled). 204 + 200 are both success returns depending on whether
// the key already exists.
func (vb *vaultBackend) createTransitKey(t *testing.T, name string) {
	t.Helper()
	vb.doVaultAPI(t, http.MethodPost, "/v1/transit/keys/"+name, nil, http.StatusNoContent)
}

// doVaultAPI issues a Vault HTTP request with the root token attached
// and asserts the response status. Helper so the bring-up keeps a
// single error-handling shape.
func (vb *vaultBackend) doVaultAPI(t *testing.T, method, path string, body io.Reader, wantStatus int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, vb.addr()+path, body)
	if err != nil {
		t.Fatalf("vault req: %v", err)
	}
	req.Header.Set("X-Vault-Token", vb.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("vault %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus && !(method == http.MethodPost && resp.StatusCode == http.StatusOK) {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("vault %s %s: status=%d want=%d body=%s", method, path, resp.StatusCode, wantStatus, raw)
	}
	// Drain body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
}

// registerVaultClient builds a Tink hcvault KMSClient pointed at this
// dev-mode vault and registers it in the Tink process-global registry
// under vb.uriPrefix(). The host-pinned prefix avoids collisions with
// any other registered vault client (production init + parallel test
// containers).
//
// NewClientWithAEADOptions is the test seam — the production-style
// `NewClient(uriPrefix, tlsCfg, token)` hardcodes `https://` for the
// vault address, which doesn't fit dev mode (http).
func (vb *vaultBackend) registerVaultClient(t *testing.T) {
	t.Helper()
	cfg := vaultapi.DefaultConfig()
	cfg.Address = vb.addr()
	vc, err := vaultapi.NewClient(cfg)
	if err != nil {
		t.Fatalf("vault api client: %v", err)
	}
	vc.SetToken(vb.token)

	kc, err := hcvaulttink.NewClientWithAEADOptions(vb.uriPrefix(), vc.Logical())
	if err != nil {
		t.Fatalf("tink hcvault client: %v", err)
	}
	registry.RegisterKMSClient(kc)
}

// TestVault_Transit_RoundTrip exercises the Tink hcvault integration
// against a real dev-mode Vault: encrypt → ciphertext goes through
// `vault transit encrypt` → decrypt round-trips. This is the
// canonical "is Vault Transit wired up correctly?" smoke.
func TestVault_Transit_RoundTrip(t *testing.T) {
	vb := startVaultDev(t)
	vb.registerVaultClient(t)

	kekURI := vb.uriFor(vaultTransitKey)
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		t.Fatalf("GetKMSClient: %v", err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	pt := []byte("hmac-secret-payload")
	// AAD must be either empty (derived=false) or non-empty
	// (derived=true). Our transit key is derived=false so the AAD
	// is ignored by Vault — we still pass it so the call path
	// matches secretstore's AAD-binding pattern.
	aad := []byte("not-actually-bound-with-derived-false")
	ct, err := aead.Encrypt(pt, aad)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	got, err := aead.Decrypt(ct, aad)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("round-trip mismatch: got %q want %q", got, pt)
	}
}

// TestVault_Transit_Secretstore wires secretstore.Resolver to a
// Vault-encrypted ciphertext on local disk. KEK lives in vault
// transit; ciphertext is a tmpfile pointed at by SecretRecord.blob_uri.
// This validates the full path that production webhook ingress takes
// when KMS = Vault.
func TestVault_Transit_Secretstore(t *testing.T) {
	vb := startVaultDev(t)
	vb.registerVaultClient(t)

	kekURI := vb.uriFor(vaultTransitKey)
	kc, err := registry.GetKMSClient(kekURI)
	if err != nil {
		t.Fatalf("GetKMSClient: %v", err)
	}
	aead, err := kc.GetAEAD(kekURI)
	if err != nil {
		t.Fatalf("GetAEAD: %v", err)
	}
	const name = "vault-secret"
	pt := []byte("ghs_payload_via_vault")
	ct, err := aead.Encrypt(pt, []byte(name))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	dir := t.TempDir()
	ctPath := filepath.Join(dir, "ct.bin")
	if err := os.WriteFile(ctPath, ct, 0o600); err != nil {
		t.Fatalf("write ct: %v", err)
	}

	rec := &enginev1.SecretRecord{
		Name: name,
		Source: &enginev1.SecretRecord_RemoteEncrypted{
			RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
				BlobUri: "file://" + ctPath,
				KekUri:  kekURI,
			},
		},
	}
	resolver := secretstore.New(nil, nil)
	if err := resolver.Reconcile(context.Background(), []*enginev1.SecretRecord{rec}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, ok := resolver.Lookup(name)
	if !ok {
		t.Fatal("Lookup returned false; expected resolved bytes")
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("Lookup = %q; want %q", got, pt)
	}
}

