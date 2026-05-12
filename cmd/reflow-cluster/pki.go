package main

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/twinfer/reflow/internal/pki"
)

// cmdInitCA generates node-ca + operator-ca pairs in --out.
func cmdInitCA(args []string) error {
	fs := flag.NewFlagSet("init-ca", flag.ContinueOnError)
	out := fs.String("out", "", "output directory for the CA files (required)")
	nodeCN := fs.String("node-cn", "reflow-node-ca", "Common Name for the node CA")
	operatorCN := fs.String("operator-cn", "reflow-operator-ca", "Common Name for the operator CA")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("--out is required")
	}

	nodeCA, err := pki.NewCA(*nodeCN)
	if err != nil {
		return err
	}
	opCA, err := pki.NewCA(*operatorCN)
	if err != nil {
		return err
	}
	nc, nk, err := nodeCA.Write(*out, "node")
	if err != nil {
		return err
	}
	oc, ok, err := opCA.Write(*out, "operator")
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", nc)
	fmt.Printf("wrote %s\n", nk)
	fmt.Printf("wrote %s\n", oc)
	fmt.Printf("wrote %s\n", ok)
	return nil
}

// cmdIssueCert issues a node leaf cert signed by the node CA in --ca-dir.
func cmdIssueCert(args []string) error {
	fs := flag.NewFlagSet("issue-cert", flag.ContinueOnError)
	kind := fs.String("kind", "node", "cert kind (only 'node' supported in 4.2)")
	nodeID := fs.Uint64("node-id", 0, "node ID; used to derive the cert filename (required for kind=node)")
	hostnames := fs.String("hostname", "", "comma-separated DNS / IP SANs (required for kind=node)")
	caDir := fs.String("ca-dir", "", "directory containing node-ca.crt + node-ca.key (required)")
	out := fs.String("out", "", "output directory for the leaf cert (required)")
	validity := fs.Duration("validity", pki.DefaultLeafValidity, "leaf cert lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kind != "node" {
		return fmt.Errorf("--kind must be 'node' in Phase 4.2 (got %q)", *kind)
	}
	if *nodeID == 0 || *hostnames == "" || *caDir == "" || *out == "" {
		return errors.New("--node-id, --hostname, --ca-dir, and --out are required")
	}
	ca, err := pki.LoadCA(filepath.Join(*caDir, "node-ca.crt"), filepath.Join(*caDir, "node-ca.key"))
	if err != nil {
		return err
	}
	hosts := strings.Split(*hostnames, ",")
	leaf, err := ca.Issue(pki.LeafOptions{
		Kind:     pki.LeafNode,
		Name:     fmt.Sprintf("node-%d", *nodeID),
		Hosts:    hosts,
		Validity: *validity,
	})
	if err != nil {
		return err
	}
	name := fmt.Sprintf("node-%d", *nodeID)
	cp, kp, err := pki.WriteMaterial(*out, name, leaf)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %s\n", cp)
	fmt.Printf("wrote %s (valid for %s)\n", kp, *validity)
	return nil
}

// cmdIssueOperator issues an operator client cert signed by the operator CA.
func cmdIssueOperator(args []string) error {
	fs := flag.NewFlagSet("issue-operator", flag.ContinueOnError)
	name := fs.String("name", "", "operator name; becomes the cert CN (required)")
	caDir := fs.String("ca-dir", "", "directory containing operator-ca.crt + operator-ca.key (required)")
	out := fs.String("out", "", "output directory for the operator cert (required)")
	validity := fs.Duration("validity", 30*24*time.Hour, "operator cert lifetime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *caDir == "" || *out == "" {
		return errors.New("--name, --ca-dir, and --out are required")
	}
	ca, err := pki.LoadCA(filepath.Join(*caDir, "operator-ca.crt"), filepath.Join(*caDir, "operator-ca.key"))
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
	fmt.Printf("wrote %s (valid for %s)\n", kp, *validity)
	return nil
}
