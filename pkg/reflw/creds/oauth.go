package creds

import (
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

// OAuthSpec configures a client-side OAuth bearer-token attachment.
// Server-side validation of incoming tokens runs in the auth
// middleware and is configured separately.
//
// Exactly one of ServiceAccountFile / StaticToken must be set.
//
// The PerRPC credential populated by this driver is preserved on
// ListenerCreds for forward-compatibility but is not yet consumed by
// the Connect-based transport.
type OAuthSpec struct {
	ServiceAccountFile string   `koanf:"service_account_file"`
	Scopes             []string `koanf:"scopes"`
	StaticToken        string   `koanf:"static_token"`
}

func buildOAuth(s *OAuthSpec) (*ListenerCreds, error) {
	if s == nil {
		return nil, errMissingSpec(DriverOAuth)
	}
	var perRPC credentials.PerRPCCredentials
	switch {
	case s.ServiceAccountFile != "":
		c, err := oauth.NewServiceAccountFromFile(s.ServiceAccountFile, s.Scopes...)
		if err != nil {
			return nil, err
		}
		perRPC = c
	case s.StaticToken != "":
		perRPC = oauth.NewOauthAccess(oauth2Token{AccessToken: s.StaticToken, TokenType: "Bearer"}.toToken())
	default:
		return nil, errEmptyField(DriverOAuth, "service_account_file or static_token")
	}
	return &ListenerCreds{
		PerRPC:        perRPC,
		Driver:        DriverOAuth,
		SecurityLevel: credentials.NoSecurity,
	}, nil
}
