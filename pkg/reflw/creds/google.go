package creds

import "errors"

// GoogleSpec carried the Google bundled-credentials configuration when
// reflw's transport was gRPC. The bundle (ALTS on GCE, TLS elsewhere +
// per-RPC Google access token) has no HTTP/2 equivalent in the Connect
// stack. The struct stays as a koanf-decodable placeholder; selecting
// this driver now returns an error at startup.
type GoogleSpec struct{}

func buildGoogle(_ *GoogleSpec) (*ListenerCreds, error) {
	return nil, errors.New("reflw/creds: Google bundled-credentials driver is not supported on the Connect transport; use tls or tls_certprovider")
}
