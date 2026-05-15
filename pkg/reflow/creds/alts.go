package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/alts"
)

// ALTSSpec configures Google ALTS (Application Layer Transport
// Security). Both fields are optional — defaults match grpc-go.
type ALTSSpec struct {
	// TargetServiceAccounts (client-side) restricts which service-account
	// peers the client will accept. Empty allows any peer.
	TargetServiceAccounts []string `koanf:"target_service_accounts"`
	// HandshakerServiceAddress overrides the default ALTS handshaker
	// endpoint (rarely needed outside of tests).
	HandshakerServiceAddress string `koanf:"handshaker_service_address"`
}

func buildALTS(s *ALTSSpec) (*ListenerCreds, error) {
	if s == nil {
		s = &ALTSSpec{}
	}
	serverOpts := &alts.ServerOptions{}
	if s.HandshakerServiceAddress != "" {
		serverOpts.HandshakerServiceAddress = s.HandshakerServiceAddress
	}
	clientOpts := &alts.ClientOptions{
		TargetServiceAccounts: s.TargetServiceAccounts,
	}
	if s.HandshakerServiceAddress != "" {
		clientOpts.HandshakerServiceAddress = s.HandshakerServiceAddress
	}
	return &ListenerCreds{
		Server:        alts.NewServerCreds(serverOpts),
		ClientDial:    []grpc.DialOption{grpc.WithTransportCredentials(alts.NewClientCreds(clientOpts))},
		Driver:        DriverALTS,
		SecurityLevel: credentials.PrivacyAndIntegrity,
	}, nil
}
