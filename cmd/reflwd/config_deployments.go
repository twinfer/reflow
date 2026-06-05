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
)

// cmdListDeployments invokes Config/ListDeployments and prints the
// returned records (with the deployment-table CAS revision) as
// indented JSON. Read-only — any peer can answer.
//
//	reflwd config list-deployments [--admin=ADDR]
func cmdListDeployments(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list-deployments", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Config.ListDeployments(ctx, connect.NewRequest(&configv1.ListDeploymentsRequest{}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"table_revision": resp.Msg.GetTableRevision(),
			"deployments":    resp.Msg.GetDeployments(),
		})
	})
}

// cmdDescribeDeployment invokes Config/DescribeDeployment for one id
// and prints the record as JSON. Read-only.
//
//	reflwd config describe-deployment --id=DEPLOYMENT_ID [--admin=ADDR]
func cmdDescribeDeployment(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("describe-deployment", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	id := fs.String("id", "", "deployment id (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	return tls.withClient(ctx, func(cli *reflwclient.Client) error {
		resp, err := cli.Config.DescribeDeployment(ctx, connect.NewRequest(&configv1.DescribeDeploymentRequest{
			DeploymentId: *id,
		}))
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(resp.Msg.GetDeployment())
	})
}

// cmdDeleteDeployment invokes Config/DeleteDeployment for one id.
// --force is required — without it the server refuses, because deleting
// a deployment may break in-flight invocations pinned to its id. The
// CAS round-trip reads the current table_revision and passes it as
// if_table_revision_eq so a concurrent operator-edit reproducibly
// conflicts.
//
//	reflwd config delete-deployment --id=DEPLOYMENT_ID --force [--admin=ADDR]
func cmdDeleteDeployment(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("delete-deployment", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	id := fs.String("id", "", "deployment id (required)")
	force := fs.Bool("force", false, "acknowledge that in-flight invocations may break; required")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("--id is required")
	}
	if !*force {
		return errors.New("--force is required (delete may break in-flight invocations)")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflwclient.Client) error {
		list, err := cli.Config.ListDeployments(rctx, connect.NewRequest(&configv1.ListDeploymentsRequest{}))
		if err != nil {
			return fmt.Errorf("read revision: %w", err)
		}
		resp, err := cli.Config.DeleteDeployment(rctx, connect.NewRequest(&configv1.DeleteDeploymentRequest{
			DeploymentId:      *id,
			Force:             true,
			IfTableRevisionEq: list.Msg.GetTableRevision(),
		}))
		if err != nil {
			return err
		}
		fmt.Printf("deployment deleted (id=%s, table_revision=%d)\n", *id, resp.Msg.GetTableRevision())
		return nil
	})
}
