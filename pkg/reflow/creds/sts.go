package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/sts"
)

// STSSpec configures an OAuth 2.0 token-exchange (RFC 8693) flow:
// the client trades a local credential for a server-issued access
// token, and attaches that token per-RPC. See grpc-go's
// credentials/sts package for the field semantics.
type STSSpec struct {
	TokenExchangeServiceURI string   `koanf:"token_exchange_service_uri"`
	Resource                string   `koanf:"resource"`
	Audience                string   `koanf:"audience"`
	Scope                   string   `koanf:"scope"`
	RequestedTokenType      string   `koanf:"requested_token_type"`
	SubjectTokenPath        string   `koanf:"subject_token_path"`
	SubjectTokenType        string   `koanf:"subject_token_type"`
	ActorTokenPath          string   `koanf:"actor_token_path"`
	ActorTokenType          string   `koanf:"actor_token_type"`
	RequestedHeaders        []string `koanf:"requested_headers"`
}

func buildSTS(s *STSSpec) (*ListenerCreds, error) {
	if s == nil {
		return nil, errMissingSpec(DriverSTS)
	}
	if s.TokenExchangeServiceURI == "" {
		return nil, errEmptyField(DriverSTS, "token_exchange_service_uri")
	}
	if s.SubjectTokenPath == "" {
		return nil, errEmptyField(DriverSTS, "subject_token_path")
	}
	if s.SubjectTokenType == "" {
		return nil, errEmptyField(DriverSTS, "subject_token_type")
	}
	perRPC, err := sts.NewCredentials(sts.Options{
		TokenExchangeServiceURI: s.TokenExchangeServiceURI,
		Resource:                s.Resource,
		Audience:                s.Audience,
		Scope:                   s.Scope,
		RequestedTokenType:      s.RequestedTokenType,
		SubjectTokenPath:        s.SubjectTokenPath,
		SubjectTokenType:        s.SubjectTokenType,
		ActorTokenPath:          s.ActorTokenPath,
		ActorTokenType:          s.ActorTokenType,
	})
	if err != nil {
		return nil, err
	}
	return &ListenerCreds{
		Server: insecure.NewCredentials(),
		ClientDial: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(perRPC),
		},
		PerRPC:        perRPC,
		Driver:        DriverSTS,
		SecurityLevel: credentials.NoSecurity,
	}, nil
}
