package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/local"
)

// LocalSpec drives grpc-go's local credentials. The handshake only
// succeeds for UDS or loopback peers; reflow uses this for in-host
// sidecars or test harnesses that want a SecurityLevel above
// NoSecurity without an actual cert.
type LocalSpec struct{}

func buildLocal(_ *LocalSpec) (*ListenerCreds, error) {
	creds := local.NewCredentials()
	return &ListenerCreds{
		Server:        creds,
		ClientDial:    []grpc.DialOption{grpc.WithTransportCredentials(local.NewCredentials())},
		Driver:        DriverLocal,
		SecurityLevel: credentials.PrivacyAndIntegrity,
	}, nil
}
