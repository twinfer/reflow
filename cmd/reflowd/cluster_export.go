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
// YAML stream on stdout. Round-trip safe with `cluster apply -f`:
// the table-revision field is read fresh by apply.
//
// --kind selects one of:
//
//	EventSource    — dumps every EventSourceTable row.
//	WebhookSource  — dumps every WebhookSourceTable row.
//	all            — dumps every cluster-managed table in stable order
//	                 (EventSource then WebhookSource). Good for backup.
func cmdExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	kind := fs.String("kind", "EventSource", "resource kind to export (EventSource|WebhookSource|all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *adminclient.Client) error {
		switch *kind {
		case "EventSource":
			return exportEventSources(ctx, cli, os.Stdout)
		case "WebhookSource":
			return exportWebhookSources(ctx, cli, os.Stdout)
		case "all":
			return exportAllKinds(ctx, cli, os.Stdout)
		default:
			return fmt.Errorf("unknown kind %q (supported: EventSource, WebhookSource, all)", *kind)
		}
	})
}

// exportAllKinds walks every known kind in a stable order and emits a
// single multi-doc YAML stream. Each kind's exporter is responsible for
// its own per-row separator; the stream-level separator between kinds
// is inserted here when both kinds contributed at least one row.
func exportAllKinds(ctx context.Context, cli *adminclient.Client, w io.Writer) error {
	// Buffer per-kind output so we can decide whether to write a
	// separator between kinds (don't emit "---\n" if the prior kind
	// was empty).
	wrote := false
	exporters := []struct {
		name string
		run  func(context.Context, *adminclient.Client, io.Writer) error
	}{
		{"EventSource", exportEventSources},
		{"WebhookSource", exportWebhookSources},
	}
	for _, e := range exporters {
		buf := &captureWriter{}
		err := e.run(ctx, cli, buf)
		switch {
		case err == nil && buf.n > 0:
			if wrote {
				if _, werr := fmt.Fprintln(w, "---"); werr != nil {
					return werr
				}
			}
			if _, werr := w.Write(buf.bytes()); werr != nil {
				return werr
			}
			wrote = true
		case err != nil && errors.Is(err, errEmpty):
			// Empty table — skip silently in --kind=all mode.
		case err != nil:
			return fmt.Errorf("%s: %w", e.name, err)
		}
	}
	if !wrote {
		return errEmpty
	}
	return nil
}

// errEmpty signals "no rows configured" from a per-kind exporter so
// --kind=all can skip empty tables without aborting the run.
var errEmpty = errors.New("no rows configured")

type captureWriter struct {
	buf []byte
	n   int
}

func (c *captureWriter) Write(p []byte) (int, error) {
	c.buf = append(c.buf, p...)
	c.n += len(p)
	return len(p), nil
}
func (c *captureWriter) bytes() []byte { return c.buf }

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
		return errEmpty
	}
	return nil
}

func exportWebhookSources(ctx context.Context, cli *adminclient.Client, w io.Writer) error {
	resp, err := cli.Admin.ListWebhookSources(ctx, connect.NewRequest(&adminv1.ListWebhookSourcesRequest{}))
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
		if err := writeWebhookSourceDoc(w, rec); err != nil {
			return err
		}
	}
	if first {
		return errEmpty
	}
	return nil
}

func writeWebhookSourceDoc(w io.Writer, rec *enginev1.WebhookSourceRecord) error {
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
	delete(specMap, "name")
	doc := map[string]any{
		"kind":     "WebhookSource",
		"metadata": map[string]any{"name": rec.GetName()},
		"spec":     specMap,
	}
	out, err := yaml.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal yaml: %w", err)
	}
	if _, err := io.WriteString(w, strings.TrimRight(string(out), "\n")+"\n"); err != nil {
		return err
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
