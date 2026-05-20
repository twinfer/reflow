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

// cmdEventSources routes `reflowd cluster eventsources <subcmd>`.
//
// list   — SyncRead's the EventSourceTable and prints rows as JSON.
//
//	Any node can answer.
//
// delete — leader-only. Reads the current table revision first so the
//
//	CAS guard matches; on a CAS conflict the operator retries
//	(or accepts: their delete proposal landed in either case).
func cmdEventSources(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflowd cluster eventsources {list|delete} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list":
		return cmdEventSourcesList(ctx, rest)
	case "delete":
		return cmdEventSourcesDelete(ctx, rest)
	default:
		return fmt.Errorf("eventsources: unknown subcommand %q", sub)
	}
}

func cmdEventSourcesList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eventsources list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *adminclient.Client) error {
		resp, err := cli.Admin.ListEventSources(ctx, connect.NewRequest(&adminv1.ListEventSourcesRequest{}))
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

func cmdEventSourcesDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("eventsources delete", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "event source name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli adminv1connect.AdminClient) error {
		// Read-then-write CAS round-trip: ListEventSources echoes the
		// current table revision; Delete passes it so a concurrent
		// operator-edit reproducibly conflicts. Same-shard linearizability
		// guarantees this is consistent on the leader.
		list, err := cli.ListEventSources(rctx, connect.NewRequest(&adminv1.ListEventSourcesRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.DeleteEventSource(rctx, connect.NewRequest(&adminv1.DeleteEventSourceRequest{
			Name:              *name,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Printf("eventsource deleted (name=%s, table_revision=%d)\n", *name, resp.Msg.GetTableRevision())
		return nil
	})
}
