package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflw/pkg/reflwclient"
	configv1 "github.com/twinfer/reflw/proto/configv1"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// cmdRegisterModel registers a model — or a dependency-closed set of models —
// into shard 0's ModelTable via RegisterModelSet. The server parses every entry,
// derives each model's bundle (decisions / children / imports), validates the set
// ∪ existing table is dependency-closed and cycle-free, and writes all rows under
// one atomic CAS'd proposal. Each node's processengine TableResolver reconciles
// the rows into parsed graphs + decision/import resolvers on the next notifier
// wake. Bundles are computed server-side — there is no --bundle flag.
//
// Single model:
//
//	reflwd config register-model --file=order.bpmn --kind=bpmn --name=Order --version=v1 [--admin=ADDR]
//
// A set (a model + its imported DMNs / referenced decisions / child processes),
// via a JSON manifest of [{"file","kind","name","version"}, ...]:
//
//	reflwd config register-model --manifest=order.set.json [--admin=ADDR]
func cmdRegisterModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("register-model", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	file := fs.String("file", "", "path to a single BPMN/CMMN/DMN model XML file")
	kind := fs.String("kind", "bpmn", "model kind for --file: bpmn, cmmn or dmn")
	name := fs.String("name", "", "model name for --file")
	version := fs.String("version", "", "model version for --file")
	manifest := fs.String("manifest", "", "path to a JSON manifest [{file,kind,name,version},...] registering a model set")
	ifRev := fs.Uint64("if-revision", 0, "CAS guard: only apply if the model-table revision equals this (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	entries, err := buildModelSetEntries(*file, *kind, *name, *version, *manifest)
	if err != nil {
		return err
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflwclient.Client) error {
		resp, err := cli.Config.RegisterModelSet(rctx, connect.NewRequest(&configv1.RegisterModelSetRequest{
			Entries:           entries,
			IfTableRevisionEq: *ifRev,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("RegisterModelSet ok (%d model(s), table_revision=%d)\n", len(entries), resp.Msg.GetTableRevision())
		return nil
	})
}

// manifestEntry is one row of a --manifest file.
type manifestEntry struct {
	File    string `json:"file"`
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

// buildModelSetEntries assembles RegisterModelSet entries from either a single
// --file (+ --kind/--name/--version) or a --manifest list. Exactly one of the two
// must be supplied.
func buildModelSetEntries(file, kind, name, version, manifest string) ([]*configv1.ModelSetEntry, error) {
	switch {
	case manifest != "" && file != "":
		return nil, errors.New("pass either --file or --manifest, not both")
	case manifest != "":
		raw, err := os.ReadFile(manifest)
		if err != nil {
			return nil, fmt.Errorf("read manifest: %w", err)
		}
		var rows []manifestEntry
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, fmt.Errorf("parse manifest json: %w", err)
		}
		if len(rows) == 0 {
			return nil, errors.New("manifest is empty")
		}
		entries := make([]*configv1.ModelSetEntry, 0, len(rows))
		for i, row := range rows {
			if row.File == "" || row.Name == "" {
				return nil, fmt.Errorf("manifest entry %d: file and name are required", i)
			}
			xmlBytes, err := os.ReadFile(row.File)
			if err != nil {
				return nil, fmt.Errorf("read manifest entry %d (%s): %w", i, row.File, err)
			}
			k := row.Kind
			if k == "" {
				k = "bpmn"
			}
			entries = append(entries, &configv1.ModelSetEntry{
				ModelRef: &enginev1.ModelRef{Kind: k, Name: row.Name, Version: row.Version},
				Xml:      xmlBytes,
			})
		}
		return entries, nil
	case file != "":
		if name == "" {
			return nil, errors.New("--name is required with --file")
		}
		xmlBytes, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read model file: %w", err)
		}
		return []*configv1.ModelSetEntry{{
			ModelRef: &enginev1.ModelRef{Kind: kind, Name: name, Version: version},
			Xml:      xmlBytes,
		}}, nil
	default:
		return nil, errors.New("pass --file (single model) or --manifest (model set)")
	}
}

// cmdListModels invokes Config/ListModels and prints each model's ref +
// registered_at + XML size (not the raw XML) plus the table CAS revision as
// indented JSON. Read-only — any peer can answer.
func cmdListModels(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-models", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Config.ListModels(ctx, connect.NewRequest(&configv1.ListModelsRequest{}))
		if err != nil {
			return err
		}
		models := make([]map[string]any, 0, len(resp.Msg.GetRecords()))
		for _, m := range resp.Msg.GetRecords() {
			models = append(models, map[string]any{
				"model_ref":        m.GetModelRef(),
				"registered_at_ms": m.GetRegisteredAtMs(),
				"xml_bytes":        len(m.GetXml()),
			})
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"models":         models,
		})
	})
}

// cmdDescribeModel invokes Config/DescribeModel for one model_ref and prints the
// record (XML rendered as text) as JSON. Read-only.
//
//	reflwd config describe-model --kind=bpmn --name=Order --version=v1 [--admin=ADDR]
func cmdDescribeModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("describe-model", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	kind := fs.String("kind", "bpmn", "model kind: bpmn or cmmn")
	name := fs.String("name", "", "model name (required)")
	version := fs.String("version", "", "model version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Config.DescribeModel(ctx, connect.NewRequest(&configv1.DescribeModelRequest{
			ModelRef: &enginev1.ModelRef{Kind: *kind, Name: *name, Version: *version},
		}))
		if err != nil {
			return err
		}
		rec := resp.Msg.GetRecord()
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"model_ref":        rec.GetModelRef(),
			"registered_at_ms": rec.GetRegisteredAtMs(),
			"bundle":           rec.GetBundle(),
			"xml":              string(rec.GetXml()),
		})
	})
}

// cmdDeleteModel invokes Config/DeleteModel for one model_ref. The CAS
// round-trip reads the current table_revision and passes it as
// if_table_revision_eq so a concurrent operator-edit reproducibly conflicts.
//
//	reflwd config delete-model --kind=bpmn --name=Order --version=v1 [--admin=ADDR]
func cmdDeleteModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete-model", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	kind := fs.String("kind", "bpmn", "model kind: bpmn or cmmn")
	name := fs.String("name", "", "model name (required)")
	version := fs.String("version", "", "model version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflwclient.Client) error {
		list, err := cli.Config.ListModels(rctx, connect.NewRequest(&configv1.ListModelsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.Config.DeleteModel(rctx, connect.NewRequest(&configv1.DeleteModelRequest{
			ModelRef:          &enginev1.ModelRef{Kind: *kind, Name: *name, Version: *version},
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Printf("model deleted (%s/%s/%s, table_revision=%d)\n", *kind, *name, *version, resp.Msg.GetTableRevision())
		return nil
	})
}
