package main

import (
	"context"
	"errors"
	"flag"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
)

// dispatchConfig routes "reflowd config <subcmd> ..." to the right
// handler. All subcommands target the reflow.config.v1.Config service
// hosted on the same admin Connect listener as ClusterCtl.
func dispatchConfig(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflowd config {register-deployment|list-deployments|describe-deployment|delete-deployment|eventsources|webhooks|tenants|apply|export|get|init-kek|create-secret|delete-secret|list-secrets|decrypt-secret|upsert-webhook} [flags]")
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "register-deployment":
		return cmdRegisterDeployment(ctx, rest)
	case "list-deployments":
		return cmdListDeployments(ctx, rest)
	case "describe-deployment":
		return cmdDescribeDeployment(ctx, rest)
	case "delete-deployment":
		return cmdDeleteDeployment(ctx, rest)
	case "eventsources":
		return cmdEventSources(ctx, rest)
	case "webhooks":
		return cmdWebhooks(ctx, rest)
	case "tenants":
		return cmdTenants(ctx, rest)
	case "apply":
		return cmdApply(ctx, rest)
	case "export":
		return cmdExport(ctx, rest)
	case "get":
		return cmdGet(ctx, rest)
	case "init-kek":
		return cmdInitKEK(ctx, rest)
	case "create-secret":
		return cmdCreateSecret(ctx, rest)
	case "delete-secret":
		return cmdDeleteSecret(ctx, rest)
	case "list-secrets":
		return cmdListSecrets(ctx, rest)
	case "decrypt-secret":
		return cmdDecryptSecret(ctx, rest)
	case "upsert-webhook":
		return cmdUpsertWebhook(ctx, rest)
	default:
		return fmt.Errorf("reflowd config: unknown subcommand %q", sub)
	}
}

func cmdRegisterDeployment(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("register-deployment", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	rawURL := fs.String("url", "", "handler deployment URL (http:// or https://)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rawURL == "" {
		return errors.New("--url is required")
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.RegisterDeployment(rctx, connect.NewRequest(&configv1.RegisterDeploymentRequest{Url: *rawURL}))
		if err != nil {
			return err
		}
		fmt.Printf("RegisterDeployment ok (deployment_id=%s)\n", resp.Msg.GetDeploymentId())
		return nil
	})
}
