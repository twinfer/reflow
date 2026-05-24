package certmgr

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"
)

// fakeRemoteSigner wraps an in-memory ECDSA key as if it lived in a
// remote KMS. Production factories return a wrapper that proxies Sign
// over the wire; for tests an in-memory key + a Sign() that defers to
// ecdsa.Sign is faithful to the contract.
type fakeRemoteSigner struct {
	priv *ecdsa.PrivateKey
}

func (f *fakeRemoteSigner) Public() crypto.PublicKey {
	return &f.priv.PublicKey
}

func (f *fakeRemoteSigner) Sign(rand io.Reader, digest []byte, opts crypto.SignerOpts) ([]byte, error) {
	return f.priv.Sign(rand, digest, opts)
}

func newFakeRemoteSigner(t *testing.T) *fakeRemoteSigner {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeRemoteSigner{priv: priv}
}

func TestRemoteSignerRegistry_RegisterResolveRoundTrip(t *testing.T) {
	const prefix = "test-kms-reg://"
	t.Cleanup(func() { UnregisterRemoteSigner(prefix) })

	signer := newFakeRemoteSigner(t)
	RegisterRemoteSigner(prefix, func(_ context.Context, _ string) (crypto.Signer, error) {
		return signer, nil
	})

	got, err := ResolveRemoteSigner(context.Background(), prefix+"my-key")
	if err != nil {
		t.Fatalf("ResolveRemoteSigner: %v", err)
	}
	if got != signer {
		t.Errorf("ResolveRemoteSigner returned different signer than factory produced")
	}
}

func TestRemoteSignerRegistry_LongestPrefixWins(t *testing.T) {
	const (
		broad  = "test-kms-prefix://"
		narrow = "test-kms-prefix://acct.foo/"
	)
	t.Cleanup(func() {
		UnregisterRemoteSigner(broad)
		UnregisterRemoteSigner(narrow)
	})

	broadSigner := newFakeRemoteSigner(t)
	narrowSigner := newFakeRemoteSigner(t)

	RegisterRemoteSigner(broad, func(context.Context, string) (crypto.Signer, error) {
		return broadSigner, nil
	})
	RegisterRemoteSigner(narrow, func(context.Context, string) (crypto.Signer, error) {
		return narrowSigner, nil
	})

	gotNarrow, err := ResolveRemoteSigner(context.Background(), narrow+"key1")
	if err != nil {
		t.Fatalf("Resolve narrow: %v", err)
	}
	if gotNarrow != narrowSigner {
		t.Errorf("expected narrow factory to win for matching uri")
	}

	gotBroad, err := ResolveRemoteSigner(context.Background(), broad+"acct.bar/key2")
	if err != nil {
		t.Fatalf("Resolve broad: %v", err)
	}
	if gotBroad != broadSigner {
		t.Errorf("expected broad factory to win when narrow prefix doesn't match")
	}
}

func TestRemoteSignerRegistry_NoFactoryError(t *testing.T) {
	_, err := ResolveRemoteSigner(context.Background(), "no-such-scheme://x")
	if err == nil {
		t.Fatal("expected error for unregistered prefix")
	}
	if !strings.Contains(err.Error(), "no remote-signer factory") {
		t.Errorf("error message lacks diagnostic substring: %v", err)
	}
}

func TestRemoteSignerRegistry_EmptyURIError(t *testing.T) {
	if _, err := ResolveRemoteSigner(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty uri")
	}
}

func TestRemoteSignerRegistry_FactoryErrorPropagates(t *testing.T) {
	const prefix = "test-kms-err://"
	t.Cleanup(func() { UnregisterRemoteSigner(prefix) })

	sentinel := errors.New("kms unreachable")
	RegisterRemoteSigner(prefix, func(context.Context, string) (crypto.Signer, error) {
		return nil, sentinel
	})

	_, err := ResolveRemoteSigner(context.Background(), prefix+"k")
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v; want wrap of sentinel", err)
	}
}

func TestRemoteSignerRegistry_NilSignerRejected(t *testing.T) {
	const prefix = "test-kms-nil://"
	t.Cleanup(func() { UnregisterRemoteSigner(prefix) })

	RegisterRemoteSigner(prefix, func(context.Context, string) (crypto.Signer, error) {
		return nil, nil
	})

	if _, err := ResolveRemoteSigner(context.Background(), prefix+"k"); err == nil {
		t.Fatal("expected nil-signer rejection")
	}
}

func TestRemoteSignerRegistry_RegisterPanicsOnEmptyPrefix(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty prefix")
		}
	}()
	RegisterRemoteSigner("", func(context.Context, string) (crypto.Signer, error) { return nil, nil })
}

func TestRemoteSignerRegistry_RegisterPanicsOnNilFactory(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil factory")
		}
	}()
	RegisterRemoteSigner("test-kms-nilf://", nil)
}

func TestRemoteSignerRegistry_UnregisterReturnsFoundFlag(t *testing.T) {
	const prefix = "test-kms-unreg://"
	RegisterRemoteSigner(prefix, func(context.Context, string) (crypto.Signer, error) {
		return newFakeRemoteSigner(t), nil
	})
	if !UnregisterRemoteSigner(prefix) {
		t.Error("Unregister returned false for registered prefix")
	}
	if UnregisterRemoteSigner(prefix) {
		t.Error("Unregister returned true after the prefix was already removed")
	}
	prefixes := registeredRemoteSignerPrefixes()
	sort.Strings(prefixes)
	for _, p := range prefixes {
		if p == prefix {
			t.Errorf("prefix %q still listed after Unregister", prefix)
		}
	}
}
