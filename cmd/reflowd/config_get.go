package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/reflowclient"
	configv1 "github.com/twinfer/reflow/proto/configv1"
)

// cmdGet fetches a single cluster-managed record and prints it in the
// same kubectl-style YAML envelope `config export` uses. Symmetric to
// export's emit shape so `config get <kind> <name> | config apply -f -`
// round-trips.
//
// Usage:
//
//	reflowd config get <kind> <name>     positional form
//	reflowd config get --kind=<k> --name=<n>
//
// kind is case-insensitive in the positional form to match `kubectl
// get <kind>` ergonomics ("eventsource" → EventSource).
func cmdGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	tls := registerTLSFlags(fs)
	kindFlag := fs.String("kind", "", "resource kind (EventSource|WebhookSource)")
	nameFlag := fs.String("name", "", "resource name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	kind := *kindFlag
	name := *nameFlag
	if kind == "" && len(rest) > 0 {
		kind = normalizeKind(rest[0])
		rest = rest[1:]
	}
	if name == "" && len(rest) > 0 {
		name = rest[0]
	}
	if kind == "" || name == "" {
		return errors.New("usage: reflowd config get <kind> <name>")
	}
	return tls.withClient(ctx, func(cli *reflowclient.Client) error {
		switch kind {
		case "EventSource":
			return getEventSource(ctx, cli, name)
		case "WebhookSource":
			return getWebhookSource(ctx, cli, name)
		default:
			return fmt.Errorf("unknown kind %q (supported: EventSource, WebhookSource)", kind)
		}
	})
}

func normalizeKind(s string) string {
	switch strings.ToLower(s) {
	case "eventsource", "eventsources":
		return "EventSource"
	case "webhooksource", "webhooksources", "webhook", "webhooks":
		return "WebhookSource"
	default:
		return s
	}
}

func getEventSource(ctx context.Context, cli *reflowclient.Client, name string) error {
	resp, err := cli.Config.ListEventSources(ctx, connect.NewRequest(&configv1.ListEventSourcesRequest{}))
	if err != nil {
		return err
	}
	for _, rec := range resp.Msg.GetSources() {
		if rec.GetName() == name {
			return writeEventSourceDoc(os.Stdout, rec)
		}
	}
	return fmt.Errorf("EventSource %q: not found", name)
}

func getWebhookSource(ctx context.Context, cli *reflowclient.Client, name string) error {
	resp, err := cli.Config.ListWebhookSources(ctx, connect.NewRequest(&configv1.ListWebhookSourcesRequest{}))
	if err != nil {
		return err
	}
	for _, rec := range resp.Msg.GetSources() {
		if rec.GetName() == name {
			return writeWebhookSourceDoc(os.Stdout, rec)
		}
	}
	return fmt.Errorf("WebhookSource %q: not found", name)
}
