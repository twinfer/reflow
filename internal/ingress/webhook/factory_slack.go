package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/twinfer/reflow/pkg/webhook"
)

// SlackDefaultTolerance matches Slack's recommended replay window:
// https://api.slack.com/authentication/verifying-requests-from-slack
// Slack documents 5 minutes; we match.
const SlackDefaultTolerance = 5 * time.Minute

// slackVerifier implements webhook.Verifier for Slack v0 signatures.
// Signed string: "v0:" + X-Slack-Request-Timestamp + ":" + body.
// HMAC-SHA256 keyed on the signing secret, lowercase hex, header
// format: X-Slack-Signature: v0=<hex>.
type slackVerifier struct {
	tolerance time.Duration
	now       func() time.Time
}

// NewSlackVerifier returns a verifier with the supplied tolerance.
// Pass 0 to use SlackDefaultTolerance.
func NewSlackVerifier(tolerance time.Duration) webhook.Verifier {
	if tolerance <= 0 {
		tolerance = SlackDefaultTolerance
	}
	return &slackVerifier{tolerance: tolerance, now: time.Now}
}

func init() { webhook.RegisterVerifier(NewSlackVerifier(0)) }

func (s *slackVerifier) Name() string { return "slack" }

func (s *slackVerifier) Verify(_ context.Context, r *http.Request, secret []byte) (*webhook.VerifiedEvent, error) {
	if len(secret) == 0 {
		return nil, errUnauthenticated("slack: empty secret")
	}
	sigHeader := r.Header.Get("X-Slack-Signature")
	tsHeader := r.Header.Get("X-Slack-Request-Timestamp")
	if sigHeader == "" {
		return nil, errUnauthenticated("slack: missing X-Slack-Signature header")
	}
	if tsHeader == "" {
		return nil, errUnauthenticated("slack: missing X-Slack-Request-Timestamp header")
	}
	const prefix = "v0="
	if !strings.HasPrefix(sigHeader, prefix) {
		return nil, errUnauthenticated("slack: signature missing v0= prefix")
	}
	sigHex := sigHeader[len(prefix):]
	got, err := hex.DecodeString(sigHex)
	if err != nil {
		return nil, errUnauthenticated("slack: signature not valid hex")
	}
	ts, err := strconv.ParseInt(tsHeader, 10, 64)
	if err != nil {
		return nil, errUnauthenticated("slack: timestamp not an integer")
	}
	if delta := s.now().Sub(time.Unix(ts, 0)); delta < -s.tolerance || delta > s.tolerance {
		return nil, errUnauthenticated(fmt.Sprintf("slack: timestamp outside tolerance (delta=%s)", delta))
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, defaultMaxBodyBytes))
	if err != nil {
		return nil, errUnauthenticated("slack: body read: " + err.Error())
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte("v0:"))
	mac.Write([]byte(tsHeader))
	mac.Write([]byte(":"))
	mac.Write(body)
	expected := mac.Sum(nil)
	if !hmac.Equal(expected, got) {
		return nil, errUnauthenticated("slack: signature mismatch")
	}
	return &webhook.VerifiedEvent{
		Body: body,
		Metadata: map[string]string{
			webhook.MetadataKeyVendor: "slack",
			"slack_signed_timestamp":  tsHeader,
		},
	}, nil
}
