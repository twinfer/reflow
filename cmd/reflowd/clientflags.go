package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/twinfer/reflow/pkg/reflow/creds"
	"github.com/twinfer/reflow/pkg/reflowclient"
)

// tlsFlags installs --client-cert / --client-key / --ca / --trust-domain
// with env fallbacks. Shared by every `reflowd cluster` and `reflowd
// config` subcommand that dials the admin Connect port; cluster RPCs go
// through cli.Cluster.X (ClusterCtl), config RPCs through cli.Config.Y
// (Config), both served on the same mTLS listener.
type tlsFlags struct {
	clientCert  string
	clientKey   string
	ca          string
	addr        string
	trustDomain string
}

func registerTLSFlags(fs *flag.FlagSet) *tlsFlags {
	f := &tlsFlags{}
	fs.StringVar(&f.clientCert, "client-cert", os.Getenv("REFLOW_CLIENT_CERT"), "operator cert PEM (env REFLOW_CLIENT_CERT)")
	fs.StringVar(&f.clientKey, "client-key", os.Getenv("REFLOW_CLIENT_KEY"), "operator key PEM (env REFLOW_CLIENT_KEY)")
	fs.StringVar(&f.ca, "ca", os.Getenv("REFLOW_CA_CERT"), "cluster CA PEM (env REFLOW_CA_CERT)")
	fs.StringVar(&f.addr, "admin", os.Getenv("REFLOW_ADMIN_ADDR"), "admin host:port of any cluster node — mutating RPCs follow LeaderHint redirects (env REFLOW_ADMIN_ADDR)")
	fs.StringVar(&f.trustDomain, "trust-domain", envOrDefault("REFLOW_TRUST_DOMAIN", "reflow.local"), "SPIFFE trust domain (env REFLOW_TRUST_DOMAIN)")
	return f
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (t *tlsFlags) validate() error {
	if t.addr == "" || t.clientCert == "" || t.clientKey == "" || t.ca == "" {
		return errors.New("--admin, --client-cert, --client-key, and --ca are required (or set the matching env vars)")
	}
	return nil
}

func (t *tlsFlags) dialOpts() reflowclient.DialOptions {
	return reflowclient.DialOptions{
		Addr: t.addr,
		Creds: creds.Spec{
			Driver: creds.DriverTLS,
			TLS: &creds.TLSSpec{
				CAFile:      t.ca,
				CertFile:    t.clientCert,
				KeyFile:     t.clientKey,
				TrustDomain: t.trustDomain,
			},
		},
	}
}

func (t *tlsFlags) dial(ctx context.Context) (*reflowclient.Client, error) {
	return reflowclient.Dial(ctx, t.dialOpts())
}

// withClient validates the registered TLS flags, dials the admin
// endpoint, and invokes fn with the live client. Used by read-only
// subcommands where any node can answer.
func (t *tlsFlags) withClient(ctx context.Context, fn func(*reflowclient.Client) error) error {
	if err := t.validate(); err != nil {
		return err
	}
	cli, err := t.dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	return fn(cli)
}

// withLeaderRedirect validates the registered TLS flags and invokes fn
// inside reflowclient.CallWithLeaderRedirect. fn receives the full
// *reflowclient.Client wrapper; pick cli.Cluster.X for ClusterCtl RPCs
// or cli.Config.Y for Config RPCs.
func (t *tlsFlags) withLeaderRedirect(
	ctx context.Context,
	fn func(context.Context, *reflowclient.Client) error,
) error {
	if err := t.validate(); err != nil {
		return err
	}
	return reflowclient.CallWithLeaderRedirect(ctx, t.dialOpts(), 3, fn)
}
