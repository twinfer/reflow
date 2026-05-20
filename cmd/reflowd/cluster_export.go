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

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"

	"github.com/twinfer/reflow/pkg/adminclient"
)

// cmdExport dumps the configured cluster-managed tables as a multi-doc
// YAML stream on stdout. The output is shape-compatible with `cluster
// apply -f` so `cluster export | cluster apply -f -` is a round-trip
// (modulo the table-revision field which apply re-reads).
//
// --kind defaults to "EventSource" since that's the only kind in PR1.
// Phase C adds "WebhookSource"; values like "all" become a convenience
// then.
func cmdExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	kind := fs.String("kind", "EventSource", "resource kind to export (EventSource)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *adminclient.Client) error {
		switch *kind {
		case "EventSource":
			return exportEventSources(ctx, cli, os.Stdout)
		default:
			return fmt.Errorf("unknown kind %q (supported: EventSource)", *kind)
		}
	})
}

func exportEventSources(ctx context.Context, cli *adminclient.Client, w io.Writer) error {
	resp, err := cli.Admin.ListEventSources(ctx, connect.NewRequest(&adminv1.ListEventSourcesRequest{}))
	if err != nil {
		return err
	}
	first := true
	for _, rec := range resp.Msg.GetSources() {
		if !first {
			if _, err := fmt.Fprintln(w, "---"); err != nil {
				return err
			}
		}
		first = false
		if err := writeEventSourceDoc(w, rec); err != nil {
			return err
		}
	}
	if first {
		return errors.New("no event sources configured")
	}
	return nil
}

func writeEventSourceDoc(w io.Writer, rec *enginev1.EventSourceRecord) error {
	specJSON, err := protojson.MarshalOptions{
		EmitUnpopulated: false,
		UseProtoNames:   true,
	}.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	var specMap map[string]any
	if err := json.Unmarshal(specJSON, &specMap); err != nil {
		return fmt.Errorf("decode record json: %w", err)
	}
	// The "name" field lives on metadata.name in the export envelope,
	// not on spec — strip it out of spec to avoid duplication after a
	// round-trip apply.
	delete(specMap, "name")
	doc := map[string]any{
		"kind":     "EventSource",
		"metadata": map[string]any{"name": rec.GetName()},
		"spec":     specMap,
	}
	out, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}
	// yaml.Marshal already terminates with a newline; trim trailing
	// runs to keep the document separator clean.
	if _, err := io.WriteString(w, strings.TrimRight(string(out), "\n")+"\n"); err != nil {
		return err
	}
	return nil
}
