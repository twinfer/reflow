package creds

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/jwt"
)

// JWTSpec configures a JWT bearer-token attachment for outbound calls
// via grpc-go's credentials/jwt.NewTokenFileCallCredentials. TokenFile
// is the path to a file holding a JWT (e.g. a k8s service-account
// projected token); grpc-go re-reads it on each call so external
// rotation works without restart.
type JWTSpec struct {
	// TokenFile is the path to the JWT token file.
	TokenFile string `koanf:"token_file"`
}

func buildJWT(s *JWTSpec) (*ListenerCreds, error) {
	if s == nil {
		return nil, errMissingSpec(DriverJWT)
	}
	if s.TokenFile == "" {
		return nil, errEmptyField(DriverJWT, "token_file")
	}
	perRPC, err := jwt.NewTokenFileCallCredentials(s.TokenFile)
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
		Driver:        DriverJWT,
		SecurityLevel: credentials.NoSecurity,
	}, nil
}
