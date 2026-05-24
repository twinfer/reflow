package certmgr

import (
	"context"
	"crypto"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// RemoteSignerFactory returns a crypto.Signer backed by a remote KMS.
// The signer's Public() must return the same public key the cluster CA
// cert was issued against — ClusterIssuer enforces that invariant on
// every Refresh. The signer's Sign() makes the network round-trip to
// the KMS; callers MUST treat it as fallible and slow.
//
// Implementations register themselves at package init() time via
// RegisterRemoteSigner, keyed by URI scheme prefix. The pattern
// mirrors Tink's KMSClient registration (pkg/kms/{awskms,gcpkms,
// hcvault,blob}) so operators wire kms_remote signing the same way
// they wire envelope encryption today.
type RemoteSignerFactory func(ctx context.Context, uri string) (crypto.Signer, error)

var (
	remoteSignerMu        sync.RWMutex
	remoteSignerFactories = map[string]RemoteSignerFactory{}
)

// RegisterRemoteSigner installs factory for any URI whose prefix
// matches one of the registered keys. Registration order does not
// matter; ResolveRemoteSigner picks the longest matching prefix so
// "aws-kms://acct.foo/" can override the broader "aws-kms://" if an
// operator wants per-account routing.
//
// Re-registering the same prefix overwrites silently — operators
// override built-in factories the same way they override any other
// registry entry.
func RegisterRemoteSigner(prefix string, factory RemoteSignerFactory) {
	if prefix == "" {
		panic("certmgr: RegisterRemoteSigner requires non-empty prefix")
	}
	if factory == nil {
		panic("certmgr: RegisterRemoteSigner requires non-nil factory")
	}
	remoteSignerMu.Lock()
	defer remoteSignerMu.Unlock()
	remoteSignerFactories[prefix] = factory
}

// UnregisterRemoteSigner removes the factory keyed by prefix. Returns
// true when something was removed. Test-only helper — production code
// never unregisters.
func UnregisterRemoteSigner(prefix string) bool {
	remoteSignerMu.Lock()
	defer remoteSignerMu.Unlock()
	_, ok := remoteSignerFactories[prefix]
	delete(remoteSignerFactories, prefix)
	return ok
}

// ResolveRemoteSigner finds the registered factory whose prefix
// matches the longest leading substring of uri and invokes it. Returns
// an error when no prefix matches.
func ResolveRemoteSigner(ctx context.Context, uri string) (crypto.Signer, error) {
	if uri == "" {
		return nil, errors.New("certmgr: ResolveRemoteSigner requires non-empty uri")
	}
	remoteSignerMu.RLock()
	var bestPrefix string
	var bestFactory RemoteSignerFactory
	for prefix, f := range remoteSignerFactories {
		if !strings.HasPrefix(uri, prefix) {
			continue
		}
		if len(prefix) > len(bestPrefix) {
			bestPrefix = prefix
			bestFactory = f
		}
	}
	remoteSignerMu.RUnlock()
	if bestFactory == nil {
		return nil, fmt.Errorf("certmgr: no remote-signer factory registered for uri %q", uri)
	}
	signer, err := bestFactory(ctx, uri)
	if err != nil {
		return nil, fmt.Errorf("certmgr: remote-signer factory %q: %w", bestPrefix, err)
	}
	if signer == nil {
		return nil, fmt.Errorf("certmgr: remote-signer factory %q returned nil signer", bestPrefix)
	}
	return signer, nil
}

// registeredRemoteSignerPrefixes returns the registered prefixes,
// sorted ascending. Test-only; lets the test assert registry contents
// without exposing the internal map directly.
func registeredRemoteSignerPrefixes() []string {
	remoteSignerMu.RLock()
	defer remoteSignerMu.RUnlock()
	out := make([]string, 0, len(remoteSignerFactories))
	for k := range remoteSignerFactories {
		out = append(out, k)
	}
	return out
}
