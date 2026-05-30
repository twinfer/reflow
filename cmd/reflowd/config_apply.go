package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	connect "connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"sigs.k8s.io/yaml"

	"github.com/twinfer/reflow/pkg/reflowclient"
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// resourceDoc is the kubectl-style envelope each YAML document carries.
// Spec is left as raw bytes so the discriminator on kind can route to
// the right typed proto-unmarshal path.
type resourceDoc struct {
	Kind     string           `json:"kind"`
	Metadata resourceMetadata `json:"metadata"`
	Spec     map[string]any   `json:"spec"`
}

type resourceMetadata struct {
	Name string `json:"name"`
}

// cmdApply reads a multi-doc YAML file (or stdin when -f -) and
// dispatches each document to the matching Config RPC. CAS revisions
// are pulled fresh from the server immediately before each Upsert so
// applying the same file twice in a row succeeds without manual
// revision tracking — the second apply just no-ops on equal-shape rows
// (per Reconcile's sourceConfigsEqual check) but still bumps the
// revision because the FSM bumps unconditionally on Upsert.
//
// Supported kinds: WebhookSource, Tenant.
func cmdApply(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("apply", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	file := fs.String("f", "", "YAML file path, or '-' for stdin (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("-f is required (use '-' for stdin)")
	}
	raw, err := readApplyInput(*file)
	if err != nil {
		return err
	}
	docs, err := splitYAMLDocs(raw)
	if err != nil {
		return fmt.Errorf("parse YAML: %w", err)
	}
	if len(docs) == 0 {
		return errors.New("apply: no resources in input")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		for i, doc := range docs {
			if err := applyOneDoc(rctx, cli, doc); err != nil {
				return fmt.Errorf("doc[%d] kind=%q name=%q: %w",
					i, doc.Kind, doc.Metadata.Name, err)
			}
		}
		return nil
	})
}

func applyOneDoc(ctx context.Context, cli *reflowclient.Client, doc resourceDoc) error {
	switch doc.Kind {
	case "Tenant":
		return applyTenant(ctx, cli, doc)
	case "":
		return errors.New("missing kind")
	default:
		return fmt.Errorf("unknown kind %q (supported: Tenant)", doc.Kind)
	}
}

// applyTenant decodes a Tenant doc and round-trips through
// ClusterCtl.UpsertTenant (tenants live on the platform-admin
// surface). metadata.name is the canonical key; the spec carries
// quotas + per-tenant OIDC issuers. The server resolves create-vs-
// update by name, so applying the same file twice in a row is
// idempotent (the second apply reuses the existing id).
func applyTenant(ctx context.Context, cli *reflowclient.Client, doc resourceDoc) error {
	if doc.Metadata.Name == "" {
		return errors.New("metadata.name is required")
	}
	rec, err := decodeTenantSpec(doc.Spec)
	if err != nil {
		return err
	}
	rec.Name = doc.Metadata.Name
	resp, err := cli.Cluster.UpsertTenant(ctx, connect.NewRequest(&clusterctlv1.UpsertTenantRequest{
		Record: rec,
	}))
	if err != nil {
		return err
	}
	fmt.Printf("tenant upserted (name=%s, tenant_id=%d, table_revision=%d)\n",
		rec.GetName(), resp.Msg.GetTenantId(), resp.Msg.GetTableRevision())
	return nil
}

// decodeTenantSpec round-trips spec → JSON → protojson into a
// TenantRecord. protojson handles nested OIDCIssuerConfig and the
// numeric quota fields uniformly.
func decodeTenantSpec(spec map[string]any) (*enginev1.TenantRecord, error) {
	if spec == nil {
		return nil, errors.New("spec is required")
	}
	jsonBytes, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshal spec: %w", err)
	}
	var rec enginev1.TenantRecord
	opts := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := opts.Unmarshal(jsonBytes, &rec); err != nil {
		return nil, fmt.Errorf("decode Tenant spec: %w", err)
	}
	return &rec, nil
}

func readApplyInput(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

// splitYAMLDocs splits a multi-document YAML stream on the standard
// `---` separator and parses each chunk into a resourceDoc. Empty
// chunks (leading separator, trailing whitespace) are skipped.
func splitYAMLDocs(raw []byte) ([]resourceDoc, error) {
	parts := strings.Split(string(raw), "\n---")
	out := make([]resourceDoc, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var doc resourceDoc
		if err := yaml.Unmarshal([]byte(p), &doc); err != nil {
			return nil, err
		}
		// All-empty document (e.g. a stray separator); skip.
		if doc.Kind == "" && doc.Metadata.Name == "" && len(doc.Spec) == 0 {
			continue
		}
		out = append(out, doc)
	}
	return out, nil
}
