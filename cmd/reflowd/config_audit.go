package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// cmdAudit routes `reflowd config audit <subcmd>`.
//
// list  — SyncRead's AuditLogTable with optional filters (--since,
//
//	--until, --tenant, --action, --limit). Any node can answer.
//
// show  — fetch one row by --raft-index.
//
// export — same as list but newline-delimited JSON suitable for
//
//	piping into jq or a log shipper.
func cmdAudit(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflowd config audit {list|show|export} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list":
		return cmdAuditList(ctx, rest)
	case "show":
		return cmdAuditShow(ctx, rest)
	case "export":
		return cmdAuditExport(ctx, rest)
	default:
		return fmt.Errorf("audit: unknown subcommand %q", sub)
	}
}

// auditListFlags installs the shared filter flags on fs and returns
// the parsed request builder. Returned func is called after fs.Parse
// because flag values aren't realized until then.
func auditListFlags(fs *flag.FlagSet) func() (*configv1.ListAuditLogRequest, error) {
	since := fs.String("since", "", "lower bound on ts (RFC3339 or duration like '24h' meaning 'last 24h')")
	until := fs.String("until", "", "upper bound on ts (RFC3339); empty = no upper bound")
	tenant := fs.Uint("tenant", 0, "tenant id filter (0 = all tenants, including engine-self)")
	action := fs.String("action", "", "action_kind filter (e.g. UpsertTenant); empty = all kinds")
	limit := fs.Uint("limit", 0, "max rows (0 = server default)")
	return func() (*configv1.ListAuditLogRequest, error) {
		req := &configv1.ListAuditLogRequest{
			TenantId:   uint32(*tenant),
			ActionKind: *action,
			Limit:      uint32(*limit),
		}
		sinceMs, err := parseAuditTime(*since, true)
		if err != nil {
			return nil, fmt.Errorf("--since: %w", err)
		}
		untilMs, err := parseAuditTime(*until, false)
		if err != nil {
			return nil, fmt.Errorf("--until: %w", err)
		}
		req.SinceMs = sinceMs
		req.UntilMs = untilMs
		return req, nil
	}
}

// parseAuditTime accepts either an RFC3339 absolute timestamp, an
// empty string (= 0 = no bound), or — for the "since" side only — a
// Go duration like "24h" meaning "now - 24h". durationAllowed gates
// the relative-duration form because "--until 24h" would mean "now -
// 24h ago" which is in the past and confusingly always-empty.
func parseAuditTime(s string, durationAllowed bool) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return uint64(t.UnixMilli()), nil
	}
	if durationAllowed {
		if d, err := time.ParseDuration(s); err == nil {
			return uint64(time.Now().Add(-d).UnixMilli()), nil
		}
	}
	return 0, fmt.Errorf("expected RFC3339 timestamp or duration: %q", s)
}

func cmdAuditList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("audit list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	build := auditListFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	req, err := build()
	if err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Config.ListAuditLog(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"records": resp.Msg.GetRecords(),
			"more":    resp.Msg.GetMore(),
		})
	})
}

// cmdAuditExport emits one JSON object per line — Splunk / jq / log-
// shipper friendly. Same filters as list. The Encoder writes each
// record on its own line without indent.
func cmdAuditExport(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("audit export", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	build := auditListFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	req, err := build()
	if err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Config.ListAuditLog(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		for _, rec := range resp.Msg.GetRecords() {
			if err := enc.Encode(rec); err != nil {
				return err
			}
		}
		if resp.Msg.GetMore() {
			fmt.Fprintln(os.Stderr, "audit export: response truncated; narrow filters and re-run")
		}
		return nil
	})
}

func cmdAuditShow(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("audit show", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: reflowd config audit show <raft_index>")
	}
	raftIndex, err := strconv.ParseUint(fs.Arg(0), 10, 64)
	if err != nil {
		return fmt.Errorf("raft_index must be numeric: %w", err)
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		// No dedicated "get by raft_index" RPC — the table is keyed by
		// raft_index so a narrow scan is O(1) after seek. We bracket
		// the scan tight: ts_ms = 0 / 0 means "no time filter", and
		// limit 1 with the right since/until requires a "get" RPC we
		// don't have. So scan the full range and find the matching id
		// client-side. Audit log size is bounded by retention; a few
		// thousand rows scanned for a one-off operator query is fine.
		resp, err := cli.Config.ListAuditLog(ctx, connect.NewRequest(&configv1.ListAuditLogRequest{}))
		if err != nil {
			return err
		}
		var hit *enginev1.AuditLogRecord
		for _, rec := range resp.Msg.GetRecords() {
			if rec.GetRaftIndex() == raftIndex {
				hit = rec
				break
			}
		}
		if hit == nil {
			return fmt.Errorf("audit show: raft_index %d not found", raftIndex)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(hit)
	})
}
