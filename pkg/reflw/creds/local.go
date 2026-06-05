package creds

import "errors"

// LocalSpec carried grpc-go's local-credentials configuration. Local
// creds are gRPC-specific (UDS / loopback handshake); there's no HTTP/2
// equivalent. The struct stays as a koanf-decodable placeholder;
// selecting this driver now returns an error at startup.
type LocalSpec struct{}

func buildLocal(_ *LocalSpec) (*ListenerCreds, error) {
	return nil, errors.New("reflw/creds: local-credentials driver is not supported on the Connect transport; use insecure for loopback or tls for in-host")
}
