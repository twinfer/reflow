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
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Config.ListWebhookSources(ctx, connect.NewRequest(&configv1.ListWebhookSourcesRequest{}))
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
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		list, err := cli.Config.ListWebhookSources(rctx, connect.NewRequest(&configv1.ListWebhookSourcesRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.Config.DeleteWebhookSource(rctx, connect.NewRequest(&configv1.DeleteWebhookSourceRequest{
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
