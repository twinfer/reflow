package creds

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/tls/certprovider"
)

// CertProviderSpec drives TLS terminated via grpc-go's certprovider
// plug-in framework. The operator registers their provider (SPIRE
// workload API, cert-manager file watcher, …) at process start; this
// spec names which builder to call and what JSON config to feed it.
//
// Identity holds the local leaf+key material. Roots holds the trust
// bundle. The two halves are deliberately separate so a workload-API
// provider can rotate the leaf independently of the trust bundle.
type CertProviderSpec struct {
	// Identity is the provider name registered via certprovider.Register
	// (e.g. "file_watcher", "spire_agent").
	Identity string `koanf:"identity"`
	// IdentityConfig is the opaque JSON config handed to the identity
	// provider's builder.
	IdentityConfig string `koanf:"identity_config"`
	// Roots is the provider name for the trust bundle. Empty disables
	// peer verification (one-way TLS).
	Roots string `koanf:"roots"`
	// RootsConfig is the opaque JSON config for the roots provider.
	RootsConfig string `koanf:"roots_config"`
	// MeshCAFingerprint, when non-empty, pins the SPKI fingerprint of
	// the CA cert at the root of every verified chain. See TLSSpec.
	MeshCAFingerprint string `koanf:"mesh_ca_fingerprint"`
	// ClientAuth, when true (default), enforces mTLS on the server side.
	ClientAuth *bool `koanf:"client_auth"`
}

func (s CertProviderSpec) clientAuth() bool {
	if s.ClientAuth == nil {
		return true
	}
	return *s.ClientAuth
}

func buildCertProvider(s *CertProviderSpec, _ *slog.Logger) (*ListenerCreds, error) {
	if s == nil {
		return nil, errMissingSpec(DriverCertProvider)
	}
	if s.Identity == "" {
		return nil, errEmptyField(DriverCertProvider, "identity")
	}

	identityCfg, err := certprovider.ParseConfig(s.Identity, []byte(s.IdentityConfig))
	if err != nil {
		return nil, fmt.Errorf("reflw/creds: parse identity config: %w", err)
	}
	identity, err := identityCfg.Build(certprovider.BuildOptions{})
	if err != nil {
		return nil, fmt.Errorf("reflw/creds: build identity provider: %w", err)
	}

	var roots certprovider.Provider
	if s.Roots != "" {
		rootsCfg, perr := certprovider.ParseConfig(s.Roots, []byte(s.RootsConfig))
		if perr != nil {
			identity.Close()
			return nil, fmt.Errorf("reflw/creds: parse roots config: %w", perr)
		}
		roots, err = rootsCfg.Build(certprovider.BuildOptions{})
		if err != nil {
			identity.Close()
			return nil, fmt.Errorf("reflw/creds: build roots provider: %w", err)
		}
	}

	verify := verifyMeshIdentity(s.MeshCAFingerprint)

	getCert := func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		km, err := identity.KeyMaterial(context.Background())
		if err != nil {
			return nil, err
		}
		if len(km.Certs) == 0 {
			return nil, errors.New("reflw/creds: identity provider returned no certs")
		}
		return &km.Certs[0], nil
	}
	getClientCert := func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return getCert(nil)
	}
	verifyRoots := func() (*tls.Config, *tls.Config, error) {
		serverCfg := &tls.Config{
			MinVersion:            tls.VersionTLS13,
			GetCertificate:        getCert,
			VerifyPeerCertificate: verify,
		}
		clientCfg := &tls.Config{
			MinVersion:            tls.VersionTLS13,
			GetClientCertificate:  getClientCert,
			VerifyPeerCertificate: verify,
		}
		if roots != nil {
			km, kerr := roots.KeyMaterial(context.Background())
			if kerr != nil {
				return nil, nil, fmt.Errorf("reflw/creds: roots KeyMaterial: %w", kerr)
			}
			if s.clientAuth() {
				serverCfg.ClientAuth = tls.RequireAndVerifyClientCert
				serverCfg.ClientCAs = km.Roots
			}
			clientCfg.RootCAs = km.Roots
		}
		return serverCfg, clientCfg, nil
	}
	serverCfg, clientCfg, err := verifyRoots()
	if err != nil {
		identity.Close()
		if roots != nil {
			roots.Close()
		}
		return nil, err
	}

	closer := func() error {
		identity.Close()
		if roots != nil {
			roots.Close()
		}
		return nil
	}

	return &ListenerCreds{
		ServerTLSConfig: serverCfg,
		ClientTLSConfig: clientCfg,
		Driver:          DriverCertProvider,
		SecurityLevel:   credentials.PrivacyAndIntegrity,
		Close:           closer,
	}, nil
}

// BuildSigner constructs a JWT Signer that pulls cert material from the
// same certprovider builder a DriverCertProvider listener would use. The
// Signer owns its own identity Provider (the listener's provider lives
// inside buildCertProvider's closer) so handler-hop signing and inter-
// node mTLS observe rotations independently against the same source.
// Returns nil-result-no-error when spec is nil so callers can opt out
// without branching.
func BuildSigner(spec *CertProviderSpec, _ *slog.Logger) (*Signer, error) {
	if spec == nil {
		return nil, nil
	}
	if spec.Identity == "" {
		return nil, errEmptyField(DriverCertProvider, "identity")
	}
	identityCfg, err := certprovider.ParseConfig(spec.Identity, []byte(spec.IdentityConfig))
	if err != nil {
		return nil, fmt.Errorf("reflw/creds: parse identity config: %w", err)
	}
	identity, err := identityCfg.Build(certprovider.BuildOptions{})
	if err != nil {
		return nil, fmt.Errorf("reflw/creds: build identity provider: %w", err)
	}
	return NewSigner(identity), nil
}
