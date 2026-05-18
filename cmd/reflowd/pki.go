package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/twinfer/reflow/internal/pki"
)

// cmdInitCA generates a single cluster CA in --out as ca.crt + ca.key.
func cmdInitCA(args []string) error {
	fs := flag.NewFlagSet("init-ca", flag.ContinueOnError)
	out := fs.String("out", "", "output directory for the CA files (required)")
	cn := fs.String("cn", "reflow-ca", "Common Name for the cluster CA")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("--out is required")
	}

	ca, err := pki.NewCA(*cn)
	if err != nil {
		return err
	}
	cp, kp, err := ca.WriteSingle(*out)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", cp)
	fmt.Printf("wrote %s\n", kp)
	return nil
}

// cmdIssueCert issues a node leaf cert signed by the cluster CA. The leaf
// carries a SPIFFE URI SAN of the form spiffe://<trust-domain>/node/<id>.
func cmdIssueCert(args []string) error {
	fs := flag.NewFlagSet("issue-cert", flag.ContinueOnError)
	kind := fs.String("kind", "node", "cert kind (only 'node' supported here)")
	nodeID := fs.Uint64("node-id", 0, "node ID; used in the URI SAN and filename (required for kind=node)")
	hostnames := fs.String("hostname", "", "comma-separated DNS / IP SANs (required for kind=node)")
	caDir := fs.String("ca-dir", "", "directory containing ca.crt + ca.key (required)")
	out := fs.String("out", "", "output directory for the leaf cert (required)")
	trustDomain := fs.String("trust-domain", "reflow.local", "SPIFFE trust domain")
	validity := fs.Duration("validity", pki.DefaultLeafValidity, "leaf cert lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kind != "node" {
		return fmt.Errorf("--kind must be 'node' (got %q)", *kind)
	}
	if *nodeID == 0 || *hostnames == "" || *caDir == "" || *out == "" {
		return errors.New("--node-id, --hostname, --ca-dir, and --out are required")
	}
	ca, err := pki.LoadCA(filepath.Join(*caDir, "ca.crt"), filepath.Join(*caDir, "ca.key"))
	if err != nil {
		return err
	}
	uri, err := pki.BuildSPIFFEID(*trustDomain, "node", strconv.FormatUint(*nodeID, 10))
	if err != nil {
		return err
	}
	hosts := strings.Split(*hostnames, ",")
	name := fmt.Sprintf("node-%d", *nodeID)
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind:     pki.LeafNode,
		Name:     name,
		Hosts:    hosts,
		URIs:     []*url.URL{uri},
		Validity: *validity,
	})
	if err != nil {
		return err
	}
	cp, kp, err := pki.WriteMaterial(*out, name, leaf)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", cp)
	fmt.Printf("wrote %s (valid for %s, uri=%s)\n", kp, *validity, uri.String())
	return nil
}

// cmdIssueOperator issues an operator client cert signed by the cluster
// CA. The leaf carries a SPIFFE URI SAN of the form
// spiffe://<trust-domain>/operator/<name>.
func cmdIssueOperator(args []string) error {
	fs := flag.NewFlagSet("issue-operator", flag.ContinueOnError)
	name := fs.String("name", "", "operator name; becomes the cert CN and SPIFFE path segment (required)")
	caDir := fs.String("ca-dir", "", "directory containing ca.crt + ca.key (required)")
	out := fs.String("out", "", "output directory for the operator cert (required)")
	trustDomain := fs.String("trust-domain", "reflow.local", "SPIFFE trust domain")
	validity := fs.Duration("validity", 30*24*time.Hour, "operator cert lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *caDir == "" || *out == "" {
		return errors.New("--name, --ca-dir, and --out are required")
	}
	ca, err := pki.LoadCA(filepath.Join(*caDir, "ca.crt"), filepath.Join(*caDir, "ca.key"))
	if err != nil {
		return err
	}
	uri, err := pki.BuildSPIFFEID(*trustDomain, "operator", *name)
	if err != nil {
		return err
	}
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind:     pki.LeafOperator,
		Name:     *name,
		URIs:     []*url.URL{uri},
		Validity: *validity,
	})
	if err != nil {
		return err
	}
	cp, kp, err := pki.WriteMaterial(*out, "operator-"+*name, leaf)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", cp)
	fmt.Printf("wrote %s (valid for %s, uri=%s)\n", kp, *validity, uri.String())
	return nil
}
