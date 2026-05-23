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
	clusterctlv1 "github.com/twinfer/reflow/proto/clusterctlv1"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// cmdTenantDEKs routes `reflowd cluster tenant-deks <subcmd>`. Mirrors
// cmdTenants — same ClusterCtl surface, same leader-redirect pattern,
// same CAS-via-list semantics on delete.
//
// create — proposes UpsertTenantDEK{record}. tenant_id, name, blob_uri
//
//	and kek_uri are required. Existing DEK for the same tenant is
//	overwritten (rotation: write a new record with a new name and a
//	new ciphertext).
//
// delete — leader-only. Reads the table revision for CAS and proposes
//
//	DeleteTenantDEK{tenant_id}. Running this makes the tenant's data
//	permanently unrecoverable.
//
// list — SyncRead; any node can serve.
func cmdTenantDEKs(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: reflowd cluster tenant-deks {create|delete|list} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "create":
		return cmdTenantDEKsCreate(ctx, rest)
	case "delete":
		return cmdTenantDEKsDelete(ctx, rest)
	case "list":
		return cmdTenantDEKsList(ctx, rest)
	default:
		return fmt.Errorf("tenant-deks: unknown subcommand %q", sub)
	}
}

func cmdTenantDEKsCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenant-deks create", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	tenantID := fs.Uint("tenant-id", 0, "tenant id (required; 0 is the default-tenant sentinel)")
	name := fs.String("name", "", "DEK record name — AAD for the KEK→DEK unwrap; rotate by writing a new name (required)")
	blobURI := fs.String("blob-uri", "", "gocloud.dev/blob URI holding the KEK-wrapped DEK ciphertext (required)")
	kekURI := fs.String("kek-uri", "", "Tink KMS URI for the wrapping KEK (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenantID == 0 {
		return errors.New("--tenant-id is required (0 is the default-tenant sentinel)")
	}
	if *name == "" {
		return errors.New("--name is required")
	}
	if *blobURI == "" {
		return errors.New("--blob-uri is required")
	}
	if *kekURI == "" {
		return errors.New("--kek-uri is required")
	}
	rec := &enginev1.TenantDEKRecord{
		TenantId: uint32(*tenantID),
		Name:     *name,
		RemoteEncrypted: &enginev1.RemoteEncryptedSecret{
			BlobUri: *blobURI,
			KekUri:  *kekURI,
		},
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Cluster.UpsertTenantDEK(rctx, connect.NewRequest(&clusterctlv1.UpsertTenantDEKRequest{
			Record: rec,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("tenant_dek upserted (tenant_id=%d, name=%s, table_revision=%d)\n",
			rec.GetTenantId(), rec.GetName(), resp.Msg.GetTableRevision())
		return nil
	})
}

func cmdTenantDEKsDelete(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenant-deks delete", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	tenantID := fs.Uint("tenant-id", 0, "tenant id (required)")
	force := fs.Bool("force", false, "acknowledge that deleting the DEK makes the tenant's data permanently unrecoverable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenantID == 0 {
		return errors.New("--tenant-id is required")
	}
	if !*force {
		return errors.New("--force is required: deleting a tenant DEK makes the tenant's data permanently unrecoverable")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		// Pull the table_revision so the CAS guard matches the read.
		list, err := cli.Cluster.ListTenantDEKs(rctx, connect.NewRequest(&clusterctlv1.ListTenantDEKsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.Cluster.DeleteTenantDEK(rctx, connect.NewRequest(&clusterctlv1.DeleteTenantDEKRequest{
			TenantId:          uint32(*tenantID),
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Printf("tenant_dek deleted (tenant_id=%d, table_revision=%d)\n",
			*tenantID, resp.Msg.GetTableRevision())
		return nil
	})
}

func cmdTenantDEKsList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tenant-deks list", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		resp, err := cli.Cluster.ListTenantDEKs(ctx, connect.NewRequest(&clusterctlv1.ListTenantDEKsRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"tenant_deks":    resp.Msg.GetTenantDeks(),
		})
	})
}
