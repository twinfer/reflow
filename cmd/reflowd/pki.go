package main

import (
	"errors"
	"flag"
	"fmt"
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

// cmdIssueCert issues a node leaf cert signed by the cluster CA. The
// leaf's CN is "node/<id>" — the principal Raw form the auth layer
// reads at peer-verify time.
func cmdIssueCert(args []string) error {
	fs := flag.NewFlagSet("issue-cert", flag.ContinueOnError)
	kind := fs.String("kind", "node", "cert kind (only 'node' supported here)")
	nodeID := fs.Uint64("node-id", 0, "node ID; used in the CN and filename (required for kind=node)")
	hostnames := fs.String("hostname", "", "comma-separated DNS / IP SANs (required for kind=node)")
	caDir := fs.String("ca-dir", "", "directory containing ca.crt + ca.key (required)")
	out := fs.String("out", "", "output directory for the leaf cert (required)")
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
	name := strconv.FormatUint(*nodeID, 10)
	filenamePrefix := "node-" + name
	hosts := strings.Split(*hostnames, ",")
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind:     pki.LeafNode,
		Name:     name,
		Hosts:    hosts,
		Validity: *validity,
	})
	if err != nil {
		return err
	}
	cp, kp, err := pki.WriteMaterial(*out, filenamePrefix, leaf)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", cp)
	fmt.Printf("wrote %s (valid for %s, CN=node/%s)\n", kp, *validity, name)
	return nil
}

// cmdIssueOperator issues an operator client cert signed by the cluster
// CA. The leaf's CN is "operator/<name>".
func cmdIssueOperator(args []string) error {
	fs := flag.NewFlagSet("issue-operator", flag.ContinueOnError)
	name := fs.String("name", "", "operator name; becomes the cert CN suffix (required)")
	caDir := fs.String("ca-dir", "", "directory containing ca.crt + ca.key (required)")
	out := fs.String("out", "", "output directory for the operator cert (required)")
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
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind:     pki.LeafOperator,
		Name:     *name,
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
	fmt.Printf("wrote %s (valid for %s, CN=operator/%s)\n", kp, *validity, *name)
	return nil
}
