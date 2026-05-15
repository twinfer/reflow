package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/oauth"
)

// OAuthSpec configures a client-side OAuth bearer-token attachment.
// Server-side validation of incoming tokens runs in the auth
// interceptor and is configured separately (it reads metadata, not
// transport state).
//
// Exactly one of ServiceAccountFile / StaticToken must be set.
type OAuthSpec struct {
	// ServiceAccountFile is a Google service-account JSON keyfile.
	// grpc-go's NewServiceAccountFromFile refreshes the access token
	// automatically.
	ServiceAccountFile string `koanf:"service_account_file"`
	// Scopes lists the OAuth scopes requested when ServiceAccountFile
	// is set.
	Scopes []string `koanf:"scopes"`
	// StaticToken is a pre-issued bearer string. No refresh. Useful
	// for tests and short-lived ad-hoc clients.
	StaticToken string `koanf:"static_token"`
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
	// Server-side OAuth has no transport contribution; reflow's setup
	// expects this listener to be composed with a transport-secure
	// driver in the auth/interceptor wiring. For standalone use the
	// floor is insecure.
	server := insecure.NewCredentials()
	return &ListenerCreds{
		Server: server,
		ClientDial: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithPerRPCCredentials(perRPC),
		},
		PerRPC:        perRPC,
		Driver:        DriverOAuth,
		SecurityLevel: credentials.NoSecurity,
	}, nil
}
