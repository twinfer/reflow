// Package hcvault wires the Tink HashiCorp Vault Transit integration
// into Reflow's process-global KMS registry.
//
// Unlike awskms / gcpkms, Vault requires an explicit token (and
// optionally a TLS config) to construct the client, so this package
// does NOT self-register from init(). Operators opt in by populating
// cfg.KMS.Vault.TokenFile (and optionally Address); pkg/reflw.Run
// invokes Register at startup if the token file is set.
//
// URI form follows Tink's convention:
//
//	hcvault://<vault-host>:<port>/transit/keys/<key-name>
//
// The URI carries the Vault server address; the registered client
// only carries the token + TLS config. Address (when set in cfg)
// narrows the registered prefix so a leaked token can't be aimed at
// an unrelated Vault.
package hcvault

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	hcvault "github.com/tink-crypto/tink-go-hcvault/v2/integration/hcvault"
	"github.com/tink-crypto/tink-go/v2/core/registry"
)

// DefaultURIPrefix matches any hcvault:// URI. Operators who want to
// pin a specific Vault server pass a longer prefix to Register
// (e.g. "hcvault://vault.prod:8200") so a different Vault host is
// rejected at GetAEAD time.
const DefaultURIPrefix = "hcvault://"

var registerOnce sync.Once
var registerErr error

// Register installs the Vault Transit KMSClient in Tink's process-
// global KMS registry. uriPrefix narrows the URIs this client claims
// — empty defaults to DefaultURIPrefix. tokenFile is required (Vault
// can't authenticate without it); tlsCfg is optional.
//
// Idempotent via sync.Once: only the first call's tokenFile / prefix
// take effect; subsequent calls are no-ops and the original error
// (if any) is returned.
func Register(uriPrefix, tokenFile string, tlsCfg *tls.Config) error {
	registerOnce.Do(func() {
		if tokenFile == "" {
			registerErr = errors.New("hcvault: token_file is required")
			return
		}
		raw, err := os.ReadFile(tokenFile)
		if err != nil {
			registerErr = fmt.Errorf("hcvault: read token file %q: %w", tokenFile, err)
			return
		}
		token := strings.TrimSpace(string(raw))
		if token == "" {
			registerErr = fmt.Errorf("hcvault: token file %q is empty", tokenFile)
			return
		}
		prefix := uriPrefix
		if prefix == "" {
			prefix = DefaultURIPrefix
		}
		client, err := hcvault.NewClient(prefix, tlsCfg, token)
		if err != nil {
			registerErr = fmt.Errorf("hcvault: new client: %w", err)
			return
		}
		registry.RegisterKMSClient(client)
	})
	return registerErr
}
