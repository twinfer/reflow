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

// cmdTenants routes `reflowd config tenants <subcmd>`.
//
// create — pre-allocates a fresh tenant_id via a read-then-CAS
//
//	round-trip (UpsertTenant with record.id=0). Re-running with the
//	same --name reuses the existing id (update path).
//
// update — re-applies --name + flag fields against the existing row
//
//	keyed by --name (resolved server-side via the tenant_name_idx
//	lookup). Use when changing quotas on an existing tenant.
//
// delete — leader-only. Reads the table revision for CAS and proposes
//
//	DeleteTenant{id}; on a CAS conflict the operator retries.
//
// list / describe — SyncRead; any node can serve.
func cmdTenants(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflowd config tenants {create|update|delete|list|describe} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "create":
		return cmdTenantsCreate(ctx, rest)
	case "update":
		return cmdTenantsUpdate(ctx, rest)
	case "delete":
		return cmdTenantsDelete(ctx, rest)
	case "list":
		return cmdTenantsList(ctx, rest)
	case "describe":
		return cmdTenantsDescribe(ctx, rest)
	default:
		return fmt.Errorf("tenants: unknown subcommand %q", sub)
	}
}

func cmdTenantsCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenants create", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "tenant name (required)")
	maxConcurrent := fs.Uint("max-concurrent", 0, "max concurrent invocations (0 = unlimited)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	rec := &enginev1.TenantRecord{
		Name:                     *name,
		MaxConcurrentInvocations: uint32(*maxConcurrent),
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.UpsertTenant(rctx, connect.NewRequest(&configv1.UpsertTenantRequest{
			Record: rec,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("tenant upserted (name=%s, tenant_id=%d, table_revision=%d)\n",
			rec.GetName(), resp.Msg.GetTenantId(), resp.Msg.GetTableRevision())
		return nil
	})
}

func cmdTenantsUpdate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenants update", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "tenant name (required)")
	maxConcurrent := fs.Uint("max-concurrent", 0, "max concurrent invocations (0 = unlimited)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	// Update path goes through the same UpsertTenant RPC. The server's
	// pre-allocation lookup will find the existing id by name and reuse
	// it. Operators who want to be explicit can pass --tenant-id (not
	// exposed yet; the lookup is sufficient).
	rec := &enginev1.TenantRecord{
		Name:                     *name,
		MaxConcurrentInvocations: uint32(*maxConcurrent),
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.UpsertTenant(rctx, connect.NewRequest(&configv1.UpsertTenantRequest{
			Record: rec,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("tenant updated (name=%s, tenant_id=%d, table_revision=%d)\n",
			rec.GetName(), resp.Msg.GetTenantId(), resp.Msg.GetTableRevision())
		return nil
	})
}

func cmdTenantsDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenants delete", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	name := fs.String("name", "", "tenant name (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		// Resolve name→id and pull the table_revision in one SyncRead so
		// the CAS guard matches.
		list, err := cli.Config.ListTenants(rctx, connect.NewRequest(&configv1.ListTenantsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		var id uint32
		for _, t := range list.Msg.GetTenants() {
			if t.GetName() == *name {
				id = t.GetId()
				break
			}
		}
		if id == 0 {
			return fmt.Errorf("tenant %q not found", *name)
		}
		resp, err := cli.Config.DeleteTenant(rctx, connect.NewRequest(&configv1.DeleteTenantRequest{
			TenantId:          id,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Printf("tenant deleted (name=%s, tenant_id=%d, table_revision=%d)\n",
			*name, id, resp.Msg.GetTableRevision())
		return nil
	})
}

func cmdTenantsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenants list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Config.ListTenants(ctx, connect.NewRequest(&configv1.ListTenantsRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"tenants":        resp.Msg.GetTenants(),
		})
	})
}

func cmdTenantsDescribe(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenants describe", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	id := fs.Uint("id", 0, "tenant id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == 0 {
		return errors.New("--id is required (0 is the default-tenant sentinel)")
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Config.DescribeTenant(ctx, connect.NewRequest(&configv1.DescribeTenantRequest{
			TenantId: uint32(*id),
		}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.Msg.GetTenant())
	})
}
