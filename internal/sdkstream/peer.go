package sdkstream

import (
	"google.golang.org/grpc/peer"

	sdkv1 "github.com/twinfer/reflow/proto/sdkv1"
)

// peerFromCtx pulls the client addr off a server stream's context. Used
// for log lines; failures fall through to "unknown" upstream.
func peerFromCtx(stream sdkv1.SessionService_InvokeServer) (string, bool) {
	p, ok := peer.FromContext(stream.Context())
	if !ok || p == nil || p.Addr == nil {
		return "", false
	}
	return p.Addr.String(), true
}
