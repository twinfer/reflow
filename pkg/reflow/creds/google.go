package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/google"
)

// GoogleSpec drives the bundled google credentials path: ALTS on GCE,
// TLS elsewhere, with a Google access token attached per-call. There
// are no operator-tunable knobs today; the struct exists so future
// scopes / quota-project hooks can land without a shape change.
type GoogleSpec struct{}

func buildGoogle(_ *GoogleSpec) (*ListenerCreds, error) {
	bundle := google.NewDefaultCredentials()
	return &ListenerCreds{
		Server:        bundle.TransportCredentials(),
		ClientDial:    []grpc.DialOption{grpc.WithCredentialsBundle(bundle)},
		PerRPC:        bundle.PerRPCCredentials(),
		Driver:        DriverGoogle,
		SecurityLevel: credentials.PrivacyAndIntegrity,
	}, nil
}
