package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/pkg/webhook"
)

// StripeDefaultTolerance is the default replay window for Stripe
// signatures per https://docs.stripe.com/webhooks/signature. Stripe
// recommends 5 minutes; we match.
const StripeDefaultTolerance = 5 * time.Minute

// MaxBodyBytes caps how much we will buffer from r.Body during
// verification. Per Stripe's published docs the max event size is
// ~256KB; we cap higher than that to leave headroom for unusually
// large connect/event payloads from other vendors that share the
// same default. Webhook sources may override per-source via the
// manager's MaxBodyBytes hook.
const defaultMaxBodyBytes int64 = 1 << 20 // 1 MiB

// stripeVerifier implements webhook.Verifier for Stripe-Signature
// headers of the form: t=<unix>,v1=<hex>,v0=<hex>...
//
// The signed payload is timestamp + "." + body. HMAC-SHA256 keyed on
// the endpoint secret (Stripe calls these "whsec_..."), expressed in
// lowercase hex. Tolerance bounds replay attacks; verifiers ignore
// v0 (the pre-2019 scheme) and any unknown vX prefixes.
type stripeVerifier struct {
	tolerance time.Duration
	now       func() time.Time // injectable for tests
}

// NewStripeVerifier returns a verifier with the supplied tolerance.
// Pass 0 to use StripeDefaultTolerance. The returned verifier is
// stateless and safe for concurrent use.
func NewStripeVerifier(tolerance time.Duration) webhook.Verifier {
	if tolerance <= 0 {
		tolerance = StripeDefaultTolerance
	}
	return &stripeVerifier{tolerance: tolerance, now: time.Now}
}

func init() { webhook.RegisterVerifier(NewStripeVerifier(0)) }

func (s *stripeVerifier) Name() string { return "stripe" }

func (s *stripeVerifier) Verify(_ context.Context, r *http.Request, secret []byte) (*webhook.VerifiedEvent, error) {
	if len(secret) == 0 {
		return nil, errUnauthenticated("stripe: empty secret")
	}
	header := r.Header.Get("Stripe-Signature")
	if header == "" {
		return nil, errUnauthenticated("stripe: missing Stripe-Signature header")
	}
	ts, sigs, err := parseStripeHeader(header)
	if err != nil {
		return nil, errUnauthenticated("stripe: " + err.Error())
	}
	if len(sigs) == 0 {
		return nil, errUnauthenticated("stripe: no v1 signatures in header")
	}
	// Bound the replay window before doing the expensive HMAC: a
	// client supplying a far-future / far-past timestamp shouldn't
	// pay for verification, and we shouldn't extend the cost to the
	// server either.
	signedAt := time.Unix(ts, 0)
	if delta := s.now().Sub(signedAt); delta < -s.tolerance || delta > s.tolerance {
		return nil, errUnauthenticated(fmt.Sprintf("stripe: timestamp outside tolerance (delta=%s)", delta))
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, defaultMaxBodyBytes))
	if err != nil {
		return nil, errUnauthenticated("stripe: body read: " + err.Error())
	}
	// Construct the signed payload exactly as Stripe documents.
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	expected := mac.Sum(nil)
	expectedHex := []byte(hex.EncodeToString(expected))
	matched := false
	for _, sig := range sigs {
		got, err := hex.DecodeString(sig)
		if err != nil {
			continue
		}
		// Compare against the raw bytes (constant-time) and the hex
		// form (defense if a misbehaving client sends odd casing).
		if hmac.Equal(expected, got) || hmac.Equal(expectedHex, []byte(strings.ToLower(sig))) {
			matched = true
			break
		}
	}
	if !matched {
		return nil, errUnauthenticated("stripe: signature mismatch")
	}
	return &webhook.VerifiedEvent{
		Body: body,
		Metadata: map[string]string{
			webhook.MetadataKeyVendor:   "stripe",
			"stripe_signed_timestamp":   strconv.FormatInt(ts, 10),
			"stripe_signature_versions": "v1",
		},
	}, nil
}

// parseStripeHeader splits the Stripe-Signature header into its
// timestamp and v1 signature components. Format:
//
//	t=1614265330,v1=abc...,v1=def...,v0=ignored
//
// Order is not significant; multiple v1 entries are allowed (Stripe
// rotates secrets by overlapping signatures). v0 is the deprecated
// pre-2019 scheme and is ignored.
func parseStripeHeader(h string) (ts int64, v1 []string, err error) {
	parts := strings.SplitSeq(h, ",")
	for p := range parts {
		eq := strings.IndexByte(p, '=')
		if eq <= 0 {
			continue
		}
		key, val := strings.TrimSpace(p[:eq]), strings.TrimSpace(p[eq+1:])
		switch key {
		case "t":
			if ts != 0 {
				return 0, nil, errors.New("duplicate t= in header")
			}
			n, perr := strconv.ParseInt(val, 10, 64)
			if perr != nil {
				return 0, nil, fmt.Errorf("invalid t= value: %w", perr)
			}
			ts = n
		case "v1":
			v1 = append(v1, val)
		}
	}
	if ts == 0 {
		return 0, nil, errors.New("missing t= in header")
	}
	return ts, v1, nil
}

// errUnauthenticated wraps a string error into a Connect-coded 401
// so the manager's response shape is correct across protocols.
func errUnauthenticated(msg string) error {
	return connect.NewError(connect.CodeUnauthenticated, errors.New(msg))
}
