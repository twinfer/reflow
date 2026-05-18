package handler_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/tls/certprovider"
	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/engine/handlerclient"
	"github.com/twinfer/reflow/internal/engine/handlerclient/connectclient"
	"github.com/twinfer/reflow/pkg/handler"
	"github.com/twinfer/reflow/pkg/reflow/creds"
	discoveryv1 "github.com/twinfer/reflow/proto/discoveryv1"
	protocolv1 "github.com/twinfer/reflow/proto/protocolv1"
)

// TestServer_RoundTrip drives a registered handler end-to-end via
// pkg/handler.NewServer + internal/engine/handlerclient/connectclient.
// The engine's wire_session is bypassed — this test asserts the
// handler-side half on its own: input arrives, handler runs, output
// flows back over Connect bidi streaming (h2c).
func TestServer_RoundTrip(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("h2-echo:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := connectclient.New("test-dep", url, true, nil)
	if err != nil {
		t.Fatalf("connectclient.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	output := runOneSession(t, client, handlerclient.Route{Service: "Echo", Handler: "echo"}, []byte("hi"))
	if got, want := string(output), "h2-echo:hi"; got != want {
		t.Errorf("output = %q; want %q", got, want)
	}
}

// TestServer_FailureRoundTrip verifies a handler returning *Failure
// surfaces as OutputCommandMessage.failure on the wire.
func TestServer_FailureRoundTrip(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "boom", func(_ handler.Context, _ []byte) ([]byte, error) {
		return nil, handler.NewFailure(42, "boom")
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}

	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := connectclient.New("test-dep", url, true, nil)
	if err != nil {
		t.Fatalf("connectclient.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	failure := runOneSessionExpectFailure(t, client,
		handlerclient.Route{Service: "Echo", Handler: "boom"}, []byte("input"))
	if failure.GetCode() != 42 || failure.GetMessage() != "boom" {
		t.Errorf("failure = (code=%d, msg=%q); want (42, %q)",
			failure.GetCode(), failure.GetMessage(), "boom")
	}
}

// TestServer_Discover asserts /discover returns the registered
// handlers grouped by (service, kind).
func TestServer_Discover(t *testing.T) {
	reg := handler.NewRegistry()
	for _, sh := range [][3]string{
		{"Echo", "echo", "service"},
		{"Cart", "checkout", "object"},
		{"Cart", "addItem", "object"},
		{"Wf", "run", "workflow"},
	} {
		var err error
		switch sh[2] {
		case "service":
			err = reg.RegisterService(sh[0], sh[1], stubHandler)
		case "object":
			err = reg.RegisterObject(sh[0], sh[1], stubHandler)
		case "workflow":
			err = reg.RegisterWorkflow(sh[0], sh[1], stubHandler)
		}
		if err != nil {
			t.Fatalf("register %s/%s: %v", sh[0], sh[1], err)
		}
	}

	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	// Discovery via the engine's admin path is exercised in
	// internal/engine integration tests — here we want a direct check
	// against /discover that doesn't drag in the full cluster bootstrap.
	resp := discoverHTTP2(t, "http://"+ln.Addr().String())
	if resp.GetProtocolVersion() != "v1" {
		t.Errorf("protocol_version = %q; want %q", resp.GetProtocolVersion(), "v1")
	}
	if got, want := len(resp.GetHandlers()), 3; got != want {
		t.Fatalf("len(handlers) = %d; want %d (Cart object handlers should fold into one group)", got, want)
	}
	// Assert Cart appears once with both handlers.
	var cart *discoveryv1.DiscoveredHandler
	for _, h := range resp.GetHandlers() {
		if h.GetService() == "Cart" {
			cart = h
			break
		}
	}
	if cart == nil {
		t.Fatal("Cart service not in discovery response")
	}
	if cart.GetKind() != protocolv1.Kind_KIND_OBJECT {
		t.Errorf("Cart.kind = %v; want KIND_OBJECT", cart.GetKind())
	}
	if got, want := len(cart.GetHandlerNames()), 2; got != want {
		t.Errorf("Cart handlers = %v; want 2 entries", cart.GetHandlerNames())
	}
}

// runOneSession opens a stream against client, sends StartMessage +
// InputCommandMessage matching the engine's wire-session protocol, and
// reads frames until EndMessage. Returns the OutputCommandMessage.value
// payload; fails the test on any unexpected condition.
func runOneSession(t *testing.T, client handlerclient.Client, route handlerclient.Route, input []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Invoke(ctx, route)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	defer func() { _ = stream.CloseSend() }()

	if err := sendStart(stream, route, input); err != nil {
		t.Fatalf("send start: %v", err)
	}

	codec := handlerclient.DefaultCodec()
	var got *protocolv1.OutputCommandMessage
	for {
		f, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if err := handlerclient.ValidatePayload(f); err != nil {
			t.Fatalf("frame validate: %v", err)
		}
		typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch typeCode {
		case handlerclient.TypeCmdOutput:
			var out protocolv1.OutputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &out); err != nil {
				t.Fatalf("decode OutputCommandMessage: %v", err)
			}
			got = &out
		case handlerclient.TypeEnd:
			if got == nil {
				t.Fatal("EndMessage before OutputCommandMessage")
			}
			val, ok := got.GetResult().(*protocolv1.OutputCommandMessage_Value)
			if !ok {
				t.Fatalf("output result = %T; want Value", got.GetResult())
			}
			return val.Value.GetContent()
		case handlerclient.TypeError:
			var em protocolv1.ErrorMessage
			_ = codec.Unmarshal(f.GetPayload(), &em)
			t.Fatalf("ErrorMessage from handler: code=%d msg=%q", em.GetCode(), em.GetMessage())
		default:
			t.Fatalf("unexpected frame type 0x%04x", typeCode)
		}
	}
}

// runOneSessionExpectFailure mirrors runOneSession but expects the
// terminal OutputCommandMessage.result to be a Failure.
func runOneSessionExpectFailure(t *testing.T, client handlerclient.Client, route handlerclient.Route, input []byte) *protocolv1.Failure {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.Invoke(ctx, route)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	defer func() { _ = stream.CloseSend() }()

	if err := sendStart(stream, route, input); err != nil {
		t.Fatalf("send start: %v", err)
	}

	codec := handlerclient.DefaultCodec()
	var got *protocolv1.OutputCommandMessage
	for {
		f, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		typeCode, _, _ := handlerclient.UnpackHeader(f.GetHeader())
		switch typeCode {
		case handlerclient.TypeCmdOutput:
			var out protocolv1.OutputCommandMessage
			if err := codec.Unmarshal(f.GetPayload(), &out); err != nil {
				t.Fatalf("decode OutputCommandMessage: %v", err)
			}
			got = &out
		case handlerclient.TypeEnd:
			if got == nil {
				t.Fatal("EndMessage before OutputCommandMessage")
			}
			fail, ok := got.GetResult().(*protocolv1.OutputCommandMessage_Failure)
			if !ok {
				t.Fatalf("output result = %T; want Failure", got.GetResult())
			}
			return fail.Failure
		default:
			t.Fatalf("unexpected frame type 0x%04x", typeCode)
		}
	}
}

// sendStart pushes the Start + Input frame pair the engine normally
// sends. The reflow-internal wire_session is bypassed here, so the
// helper has to do the encoding itself. known_entries=1 because the
// only replay frame is the InputCommandMessage at slot 0.
func sendStart(stream handlerclient.Stream, route handlerclient.Route, input []byte) error {
	codec := handlerclient.DefaultCodec()
	start := &protocolv1.StartMessage{
		Id:           []byte("test-uuid-16bytes"[:16]),
		DebugId:      "t/" + route.Service + "/" + route.Handler,
		ServiceName:  route.Service,
		HandlerName:  route.Handler,
		Kind:         protocolv1.Kind_KIND_SERVICE,
		KnownEntries: 1,
	}
	startBytes, err := codec.Marshal(start)
	if err != nil {
		return err
	}
	if err := stream.Send(handlerclient.FrameFor(handlerclient.TypeStart, startBytes)); err != nil {
		return err
	}
	in := &protocolv1.InputCommandMessage{Value: &protocolv1.Value{Content: input}}
	inputBytes, err := codec.Marshal(in)
	if err != nil {
		return err
	}
	return stream.Send(handlerclient.FrameFor(handlerclient.TypeCmdInput, inputBytes))
}

// discoverHTTP2 issues GET /discover over h2c and decodes the response.
// The engine's admin path uses the same shape (see
// internal/engine/admin/server.go.discoverHTTP); here we hit the
// endpoint directly so the test stays self-contained.
func discoverHTTP2(t *testing.T, baseURL string) *discoveryv1.DiscoveryResponse {
	t.Helper()
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	hc := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	defer tr.CloseIdleConnections()

	target := strings.TrimRight(baseURL, "/") + "/discover"
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("build /discover request: %v", err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("GET /discover: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		t.Fatalf("/discover returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /discover body: %v", err)
	}
	var dr discoveryv1.DiscoveryResponse
	if err := proto.Unmarshal(body, &dr); err != nil {
		t.Fatalf("unmarshal DiscoveryResponse: %v", err)
	}
	return &dr
}

// stubHandler is a no-op handler used to populate the registry for the
// discovery test.
var stubHandler handler.Handler = func(_ handler.Context, _ []byte) ([]byte, error) { return nil, nil }

// TestServer_RoundTrip_WithAuth wires a real signer + verifier
// built from the same CA and asserts an /invoke round-trip succeeds
// when the engine-side dispatch signs the request. Mirrors
// TestServer_RoundTrip but with auth enabled on both ends.
func TestServer_RoundTrip_WithAuth(t *testing.T) {
	caPEM, signer, spiffe := buildCAAndSigner(t, "/node/1")

	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("auth-echo:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	srv, err := handler.NewServer(handler.Config{
		Registry:      reg,
		RootCAs:       caPEM,
		AllowedSPIFFE: []string{spiffe},
		TrustDomain:   "reflow.local",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := connectclient.New("dep-auth", url, true, signer)
	if err != nil {
		t.Fatalf("connectclient.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	output := runOneSession(t, client,
		handlerclient.Route{Service: "Echo", Handler: "echo"}, []byte("hi"))
	if got, want := string(output), "auth-echo:hi"; got != want {
		t.Errorf("output = %q; want %q", got, want)
	}
}

// TestServer_AuthRejectsForeignCA: a client whose leaf is signed
// by a CA the server doesn't trust gets rejected at the HTTP layer.
// We bypass connectclient (which would surface the 401 as a transport
// error inside Receive) and hit the Connect InvokeStream URL directly
// — withAuth runs before Connect's stream handler, so an empty POST
// is enough to exercise the 401 path.
func TestServer_AuthRejectsForeignCA(t *testing.T) {
	caPEM, _, spiffe := buildCAAndSigner(t, "/node/1")
	// Foreign signer: rooted at a different CA — verifier won't accept.
	_, foreignSigner, _ := buildCAAndSigner(t, "/node/1")

	reg := handler.NewRegistry()
	_ = reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) { return in, nil })
	srv, err := handler.NewServer(handler.Config{
		Registry:      reg,
		RootCAs:       caPEM,
		AllowedSPIFFE: []string{spiffe},
		TrustDomain:   "reflow.local",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	tok, err := foreignSigner.Sign("dep-test")
	if err != nil {
		t.Fatalf("foreign Sign: %v", err)
	}
	tr := &http.Transport{Protocols: new(http.Protocols)}
	tr.Protocols.SetUnencryptedHTTP2(true)
	tr.Protocols.SetHTTP1(false)
	hc := &http.Client{Transport: tr, Timeout: 3 * time.Second}
	defer tr.CloseIdleConnections()

	req, _ := http.NewRequest(http.MethodPost,
		"http://"+ln.Addr().String()+"/reflow.handler.v1.HandlerService/InvokeStream",
		strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Error("WWW-Authenticate header missing on 401")
	}
}

// buildCAAndSigner builds a self-signed CA + a leaf signed by it,
// wraps the leaf in a *creds.Signer, and returns the CA PEM bundle.
// The leaf's SPIFFE URI is "spiffe://reflow.local"+spiffePath.
func buildCAAndSigner(t *testing.T, spiffePath string) (caPEM []byte, signer *creds.Signer, spiffe string) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CA cert: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	uri, _ := url.Parse("spiffe://reflow.local" + spiffePath)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-leaf"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		URIs:         []*url.URL{uri},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	leaf, _ := x509.ParseCertificate(leafDER)
	cert := tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey, Leaf: leaf}
	signer = creds.NewSigner(&e2eFakeProvider{cert: cert}, "reflow.local")
	return caPEM, signer, "spiffe://reflow.local" + spiffePath
}

type e2eFakeProvider struct{ cert tls.Certificate }

func (p *e2eFakeProvider) KeyMaterial(_ context.Context) (*certprovider.KeyMaterial, error) {
	return &certprovider.KeyMaterial{Certs: []tls.Certificate{p.cert}}, nil
}
func (p *e2eFakeProvider) Close() {}

// TestServer_NoBodyLeak runs many sequential sessions through one
// connectclient.Client and asserts the transport doesn't accumulate
// idle connections after the engine's typical "defer CloseSend"
// teardown. Regression for the bug where Recv terminating on EOF never
// closed the underlying HTTP/2 stream slot before GC.
func TestServer_NoBodyLeak(t *testing.T) {
	reg := handler.NewRegistry()
	if err := reg.RegisterService("Echo", "echo", func(_ handler.Context, in []byte) ([]byte, error) {
		return append([]byte("h2-echo:"), in...), nil
	}); err != nil {
		t.Fatalf("RegisterService: %v", err)
	}
	srv, err := handler.NewServer(handler.Config{Registry: reg})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	url := "http://" + ln.Addr().String()
	client, err := connectclient.New("test-dep", url, true, nil)
	if err != nil {
		t.Fatalf("connectclient.New: %v", err)
	}
	defer func() { _ = client.Close() }()

	// 32 sequential sessions: if Recv/CloseSend doesn't close resp.Body,
	// stdlib accumulates HTTP/2 stream tracking state. We can't directly
	// observe the leak without internal hooks, but a session count well
	// above the per-conn concurrency cap exercises the recycle path; a
	// real leak would manifest as stalls or stream-limit errors.
	for i := range 32 {
		output := runOneSession(t, client,
			handlerclient.Route{Service: "Echo", Handler: "echo"}, []byte("ping"))
		if got, want := string(output), "h2-echo:ping"; got != want {
			t.Fatalf("session %d output = %q; want %q", i, got, want)
		}
	}
}
