package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// InsecureSpec carries no fields today; presence of the (driver,
// driver-options) pair is enough. The struct exists so future
// debugging knobs (e.g. "warn on every RPC") can land without a
// shape change.
type InsecureSpec struct{}

func buildInsecure(_ *InsecureSpec) (*ListenerCreds, error) {
	server := insecure.NewCredentials()
	return &ListenerCreds{
		Server:        server,
		ClientDial:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Driver:        DriverInsecure,
		SecurityLevel: credentials.NoSecurity,
	}, nil
}
