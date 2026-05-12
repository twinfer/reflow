package sdkstream_test

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/ingress"
	"github.com/twinfer/reflow/internal/sdkstream"
	"github.com/twinfer/reflow/pkg/sdk"
	sdkv1 "github.com/twinfer/reflow/proto/sdkv1"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// TestSessionServiceStub_ReturnsUnimplemented verifies that the gRPC
// SessionService is registered on the ingress server and returns
// Unimplemented when an SDK dials Invoke. Locks in the wire contract;
// real routing is wired in a later phase.
func TestSessionServiceStub_ReturnsUnimplemented(t *testing.T) {
	dir := t.TempDir()
	h, err := engine.NewHost(engine.HostConfig{
		NodeID:         1,
		RaftAddr:       freeAddr(t),
		DataDir:        filepath.Join(dir, "node1"),
		RTTMillisecond: 50,
		Handlers:       sdk.NewRegistry(),
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	if _, err := h.StartPartition(1); err != nil {
		t.Fatalf("StartPartition: %v", err)
	}
	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer awaitCancel()
	if err := h.AwaitLeader(awaitCtx, 1); err != nil {
		t.Fatalf("AwaitLeader: %v", err)
	}

	rt, err := ingress.Start(context.Background(), h, ingress.Config{
		GRPCAddr: "127.0.0.1:0",
		ExtraGRPC: func(s *grpc.Server) {
			sdkstream.Register(s, h, nil)
		},
	})
	if err != nil {
		t.Fatalf("ingress.Start: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })

	conn, err := grpc.NewClient(rt.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := sdkv1.NewSessionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Invoke(ctx)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	// The server should close the stream with Unimplemented before or
	// shortly after the first Recv. Sending a frame first to exercise
	// the bidi-stream wire on the client side; the server still rejects.
	_ = stream.Send(&sdkv1.SDKMessage{})
	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Fatalf("expected Unimplemented, got nil from Recv")
	}
	if recvErr == io.EOF {
		t.Fatalf("got EOF; want Unimplemented")
	}
	st, ok := status.FromError(recvErr)
	if !ok {
		t.Fatalf("Recv err is not a gRPC status: %v", recvErr)
	}
	if st.Code() != codes.Unimplemented {
		t.Errorf("status = %s; want Unimplemented", st.Code())
	}
}
