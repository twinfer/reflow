package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	connect "connectrpc.com/connect"

	adminv1 "github.com/twinfer/reflow/proto/adminv1"
	"github.com/twinfer/reflow/proto/adminv1/adminv1connect"

	"github.com/twinfer/reflow/pkg/adminclient"
)

// cmdWebhooks routes `reflowd cluster webhooks <subcmd>`. Mirrors
// cmdEventSources — list reads from any peer (SyncRead), delete is
// leader-only with a CAS round-trip.
func cmdWebhooks(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflowd cluster webhooks {list|delete} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list":
		return cmdWebhooksList(ctx, rest)
	case "delete":
		return cmdWebhooksDelete(ctx, rest)
	default:
		return fmt.Errorf("webhooks: unknown subcommand %q", sub)
	}
}

func cmdWebhooksList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("webhooks list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *adminclient.Client) error {
		resp, err := cli.Admin.ListWebhookSources(ctx, connect.NewRequest(&adminv1.ListWebhookSourcesRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"sources":        resp.Msg.GetSources(),
		})
	})
}

func cmdWebhooksDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("webhooks delete", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "webhook source name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1connect.AdminClient) error {
		list, err := cli.ListWebhookSources(rctx, connect.NewRequest(&adminv1.ListWebhookSourcesRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.DeleteWebhookSource(rctx, connect.NewRequest(&adminv1.DeleteWebhookSourceRequest{
			Name:              *name,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Printf("webhook deleted (name=%s, table_revision=%d)\n", *name, resp.Msg.GetTableRevision())
		return nil
	})
}
