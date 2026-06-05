package creds

import (
	"time"

	"golang.org/x/oauth2"
)

// oauth2Token is a thin koanf-friendly intermediary so the oauth and
// (future) refresh-token paths can build a golang.org/x/oauth2.Token
// without that type leaking into the public Spec surface.
type oauth2Token struct {
	AccessToken  string
	TokenType    string
	RefreshToken string
	Expiry       time.Time
}

func (t oauth2Token) toToken() *oauth2.Token {
	tt := t.TokenType
	if tt == "" {
		tt = "Bearer"
	}
	return &oauth2.Token{
		AccessToken:  t.AccessToken,
		TokenType:    tt,
		RefreshToken: t.RefreshToken,
		Expiry:       t.Expiry,
	}
}
