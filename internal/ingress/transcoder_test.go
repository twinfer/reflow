package ingress_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/twinfer/reflw/internal/ingress"
	"github.com/twinfer/reflw/pkg/handler"
	ingressv1 "github.com/twinfer/reflw/proto/ingressv1"
)

// postJSON issues a plain (anonymous, no client cert) HTTP POST of a JSON body
// to the ingress listener — a browser/curl-shaped request against the Vanguard
// REST surface.
func postJSON(t *testing.T, addr, path string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s: %v", path, err)
	}
	return resp
}

// TestTranscoder_REST exercises the Vanguard transcoder end-to-end over a single
// host bring-up (one teardown keeps the harness's shard-0 WAL-recycle race off
// the hot path). The subtests prove, in order:
//   - the SubmitInvocation google.api.http annotation serves REST: a JSON POST to
//     /v1/invocations (await-by-default) runs the real Submit→Await path;
//   - metaLiftHandler threads Reflw-Meta-* headers into ctx.Metadata();
//   - the Cedar authz interceptor fires on transcoded REST: an anonymous POST to
//     the operator-only PurgeInvocation route maps to 401 + WWW-Authenticate.
func TestTranscoder_REST(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("echo:"), in...), nil
	}); err != nil {
		t.Fatalf("Register echo: %v", err)
	}
	if err := reg.RegisterService("Echo", "metaecho", func(c handler.Context, _ []byte) ([]byte, error) {
		return []byte(c.Metadata()["foo"]), nil
	}); err != nil {
		t.Fatalf("Register metaecho: %v", err)
	}
	_, rt, _ := bringUpHostWithIngress(t, reg)
	addr := rt.Addr()

	t.Run("SubmitInvocation await", func(t *testing.T) {
		reqBody, err := protojson.Marshal(&ingressv1.SubmitInvocationRequest{
			Service: "Echo", Handler: "echo", Input: []byte("hello"),
		})
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		resp := postJSON(t, addr, "/v1/invocations", reqBody, nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%q)", resp.StatusCode, string(raw))
		}
		var out ingressv1.SubmitInvocationResponse
		if err := protojson.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal response %q: %v", string(raw), err)
		}
		if !out.GetCompleted() {
			t.Fatalf("completed = false, want true (%q)", string(raw))
		}
		if got := string(out.GetOutput()); got != "echo:hello" {
			t.Fatalf("output = %q, want echo:hello", got)
		}
		if out.GetInvocationId() == "" {
			t.Fatalf("missing invocation_id in %q", string(raw))
		}
	})

	t.Run("Reflw-Meta header lift", func(t *testing.T) {
		reqBody, err := protojson.Marshal(&ingressv1.SubmitInvocationRequest{Service: "Echo", Handler: "metaecho"})
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		resp := postJSON(t, addr, "/v1/invocations", reqBody, map[string]string{"Reflw-Meta-Foo": "bar"})
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (%q)", resp.StatusCode, string(raw))
		}
		var out ingressv1.SubmitInvocationResponse
		if err := protojson.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal response %q: %v", string(raw), err)
		}
		if got := string(out.GetOutput()); got != "bar" {
			t.Fatalf("ctx.Metadata()[foo] echoed = %q, want bar (%q)", got, string(raw))
		}
	})

	t.Run("anonymous denied on operator-only route", func(t *testing.T) {
		id := ingress.FormatInvocationID(makeID(1, make([]byte, 16)))
		resp := postJSON(t, addr, "/v1/invocations/"+id+"/purge", []byte("{}"), nil)
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401 (%q)", resp.StatusCode, string(raw))
		}
		if got := resp.Header.Get("WWW-Authenticate"); got != "Bearer" {
			t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
		}
	})
}
