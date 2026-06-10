package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/twinfer/reflw/pkg/reflw/creds"
	"github.com/twinfer/reflw/pkg/reflwclient"
)

// tlsFlags installs --client-cert / --client-key / --ca with env
// fallbacks. Shared by every `reflwd cluster` and `reflwd config`
// subcommand that dials the admin Connect port; cluster RPCs go
// through cli.Admin.X (ClusterCtl), config RPCs through cli.Admin.Y
// (Config), both served on the same mTLS listener.
type tlsFlags struct {
	clientCert string
	clientKey  string
	ca         string
	addr       string
	addrName   string // the addr flag's name ("admin"/"ingress") for error text
}

func registerTLSFlags(fs *flag.FlagSet) *tlsFlags {
	f := &tlsFlags{addrName: "admin"}
	fs.StringVar(&f.clientCert, "client-cert", os.Getenv("REFLW_CLIENT_CERT"), "operator cert PEM (env REFLW_CLIENT_CERT)")
	fs.StringVar(&f.clientKey, "client-key", os.Getenv("REFLW_CLIENT_KEY"), "operator key PEM (env REFLW_CLIENT_KEY)")
	fs.StringVar(&f.ca, "ca", os.Getenv("REFLW_CA_CERT"), "cluster CA PEM (env REFLW_CA_CERT)")
	fs.StringVar(&f.addr, "admin", os.Getenv("REFLW_ADMIN_ADDR"), "admin host:port of any cluster node — mutating RPCs follow LeaderHint redirects (env REFLW_ADMIN_ADDR)")
	return f
}

// registerIngressTLSFlags is registerTLSFlags for commands that dial the
// ingress listener instead of admin (the addr flag is --ingress). Same
// operator cert/key/ca — the ingress listener verifies the operator leaf
// and the authz interceptor gates operator-only procedures (e.g. purge).
func registerIngressTLSFlags(fs *flag.FlagSet) *tlsFlags {
	f := &tlsFlags{addrName: "ingress"}
	fs.StringVar(&f.clientCert, "client-cert", os.Getenv("REFLW_CLIENT_CERT"), "operator cert PEM (env REFLW_CLIENT_CERT)")
	fs.StringVar(&f.clientKey, "client-key", os.Getenv("REFLW_CLIENT_KEY"), "operator key PEM (env REFLW_CLIENT_KEY)")
	fs.StringVar(&f.ca, "ca", os.Getenv("REFLW_CA_CERT"), "cluster CA PEM (env REFLW_CA_CERT)")
	fs.StringVar(&f.addr, "ingress", os.Getenv("REFLW_INGRESS_ADDR"), "ingress host:port of a node hosting the target partition shard (env REFLW_INGRESS_ADDR)")
	return f
}

func (t *tlsFlags) validate() error {
	if t.addr == "" || t.clientCert == "" || t.clientKey == "" || t.ca == "" {
		name := t.addrName
		if name == "" {
			name = "admin"
		}
		return fmt.Errorf("--%s, --client-cert, --client-key, and --ca are required (or set the matching env vars)", name)
	}
	return nil
}

func (t *tlsFlags) dialOpts() reflwclient.DialOptions {
	return reflwclient.DialOptions{
		Addr: t.addr,
		Creds: creds.Spec{
			Driver: creds.DriverTLS,
			TLS: &creds.TLSSpec{
				CAFile:   t.ca,
				CertFile: t.clientCert,
				KeyFile:  t.clientKey,
			},
		},
	}
}

func (t *tlsFlags) dial(ctx context.Context) (*reflwclient.Client, error) {
	return reflwclient.Dial(ctx, t.dialOpts())
}

// withClient validates the registered TLS flags, dials the admin
// endpoint, and invokes fn with the live client. Used by read-only
// subcommands where any node can answer.
func (t *tlsFlags) withClient(ctx context.Context, fn func(*reflwclient.Client) error) error {
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
// inside reflwclient.CallWithLeaderRedirect. fn receives the full
// *reflwclient.Client wrapper; pick cli.Admin.X for ClusterCtl RPCs
// or cli.Admin.Y for Config RPCs.
func (t *tlsFlags) withLeaderRedirect(
	ctx context.Context,
	fn func(context.Context, *reflwclient.Client) error,
) error {
	if err := t.validate(); err != nil {
		return err
	}
	return reflwclient.CallWithLeaderRedirect(ctx, t.dialOpts(), 3, fn)
}
