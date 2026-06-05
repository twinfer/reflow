package creds

import "errors"

// ALTSSpec carried the Google ALTS configuration when reflw's
// transport was gRPC. ALTS is gRPC-specific with no HTTP/2 equivalent,
// so the Connect-based transport has no ALTS path. The struct stays as
// a koanf-decodable placeholder so existing config files keep parsing;
// selecting this driver now returns an error at startup.
type ALTSSpec struct {
	TargetServiceAccounts    []string `koanf:"target_service_accounts"`
	HandshakerServiceAddress string   `koanf:"handshaker_service_address"`
}

func buildALTS(_ *ALTSSpec) (*ListenerCreds, error) {
	return nil, errors.New("reflw/creds: ALTS driver is not supported on the Connect transport; use tls or tls_certprovider")
}
