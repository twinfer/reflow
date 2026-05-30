package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
)

// dispatchConfig routes "reflowd config <subcmd> ..." to the right
// handler. All subcommands target the reflow.config.v1.Config service
// hosted on the same admin Connect listener as ClusterCtl.
func dispatchConfig(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: reflowd config {register-deployment|list-deployments|describe-deployment|delete-deployment|apply|init-kek|create-secret|delete-secret|list-secrets|decrypt-secret|audit|ca|create-join-token|list-join-tokens|delete-join-token|issue-operator|upsert-cluster-authz-policy|get-cluster-authz-policy} [flags]")
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
	case "apply":
		return cmdApply(ctx, rest)
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
	case "audit":
		return cmdAudit(ctx, rest)
	case "ca":
		return dispatchCA(ctx, rest)
	case "create-join-token":
		return cmdCreateJoinToken(ctx, rest)
	case "list-join-tokens":
		return cmdListJoinTokens(ctx, rest)
	case "delete-join-token":
		return cmdDeleteJoinToken(ctx, rest)
	case "issue-operator":
		return cmdIssueOperator(ctx, rest)
	case "upsert-cluster-authz-policy":
		return cmdUpsertClusterAuthzPolicy(ctx, rest)
	case "get-cluster-authz-policy":
		return cmdGetClusterAuthzPolicy(ctx, rest)
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

// cmdUpsertClusterAuthzPolicy uploads a Cedar policy file as the cluster-wide
// authz policy. The server validates it against the schema before proposing,
// so an invalid policy fails here rather than locking the cluster out.
func cmdUpsertClusterAuthzPolicy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("upsert-cluster-authz-policy", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	file := fs.String("f", "", "path to a Cedar policy file")
	ifRev := fs.Uint64("if-revision", 0, "CAS guard: only apply if the table revision equals this (0 disables)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *file == "" {
		return errors.New("-f <policy.cedar> is required")
	}
	text, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("read policy file: %w", err)
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.UpsertClusterAuthzPolicy(rctx, connect.NewRequest(&configv1.UpsertClusterAuthzPolicyRequest{
			PolicyText:        string(text),
			IfTableRevisionEq: *ifRev,
		}))
		if err != nil {
			return err
		}
		fmt.Printf("UpsertClusterAuthzPolicy ok (table_revision=%d)\n", resp.Msg.GetTableRevision())
		return nil
	})
}

// cmdGetClusterAuthzPolicy prints the current cluster authz policy text and
// its table revision. On a fresh cluster this is the in-binary foundational
// policy (revision 0) — the effective default until an operator overrides it.
func cmdGetClusterAuthzPolicy(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get-cluster-authz-policy", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	return tls.withLeaderRedirect(ctx, func(rctx context.Context, cli *reflowclient.Client) error {
		resp, err := cli.Config.GetClusterAuthzPolicy(rctx, connect.NewRequest(&configv1.GetClusterAuthzPolicyRequest{}))
		if err != nil {
			return err
		}
		fmt.Printf("# table_revision=%d\n%s\n", resp.Msg.GetTableRevision(), resp.Msg.GetPolicyText())
		return nil
	})
}
