package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// cmdRegisterModel uploads a BPMN/CMMN model file into shard 0's ModelTable.
// The server validates kind + XML well-formedness before proposing; each node's
// iflowengine TableResolver reconciles the row into a parsed graph +
// historyTimeToLive on the next notifier wake.
//
//	reflowd config register-model --file=order.bpmn --kind=bpmn --name=Order --version=v1 [--admin=ADDR]
func cmdRegisterModel(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("register-model", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	file := fs.String("file", "", "path to a BPMN/CMMN model XML file (required)")
	kind := fs.String("kind", "bpmn", "model kind: bpmn or cmmn")
	name := fs.String("name", "", "model name (required)")
	version := fs.String("version", "", "model version")
	ifRev := fs.Uint64("if-revision", 0, "CAS guard: only apply if the model-table revision equals this (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("--file is required")
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	xmlBytes, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("read model file: %w", err)
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.UpsertModel(rctx, connect.NewRequest(&configv1.UpsertModelRequest{
			ModelRef:          &enginev1.ModelRef{Kind: *kind, Name: *name, Version: *version},
			Xml:               xmlBytes,
			IfTableRevisionEq: *ifRev,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("RegisterModel ok (%s/%s/%s, table_revision=%d)\n", *kind, *name, *version, resp.Msg.GetTableRevision())
		return nil
	})
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
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
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
//	reflowd config describe-model --kind=bpmn --name=Order --version=v1 [--admin=ADDR]
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
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
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
			"xml":              string(rec.GetXml()),
		})
	})
}

// cmdDeleteModel invokes Config/DeleteModel for one model_ref. The CAS
// round-trip reads the current table_revision and passes it as
// if_table_revision_eq so a concurrent operator-edit reproducibly conflicts.
//
//	reflowd config delete-model --kind=bpmn --name=Order --version=v1 [--admin=ADDR]
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
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
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
