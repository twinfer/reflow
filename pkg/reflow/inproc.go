package reflow

import (
	"context"

	"github.com/twinfer/reflw/internal/engine/handlerclient"
	"github.com/twinfer/reflw/pkg/handler"
	"github.com/twinfer/reflw/pkg/handler/wire"
	handlerv1 "github.com/twinfer/reflw/proto/handlerv1"
)

// inprocDialer returns a handlerclient.Dialer that hands back an in-process
// Client bound to reg. It is registered on the engine's handlerclient
// Registry under the "inproc" scheme via HostConfig.InProcDialer, letting
// the engine drive an embedded handler with no HTTP. The bridge lives here
// in pkg/reflow because internal/engine cannot import pkg/handler.
func inprocDialer(reg *handler.Registry, codec wire.Codec) handlerclient.Dialer {
	return func(_, _ string) (handlerclient.Client, error) {
		return &inprocClient{reg: reg, codec: codec}, nil
	}
}

// inprocClient implements handlerclient.Client by running the handler
// session directly in-process (handler.InvokeInProc) — no Connect, no HTTP,
// no serialization beyond the inner codec.
type inprocClient struct {
	reg   *handler.Registry
	codec wire.Codec
}

func (c *inprocClient) Invoke(ctx context.Context, _ wire.Route, req *handlerv1.InvokeRequest) (*handlerv1.InvokeResponse, error) {
	return handler.InvokeInProc(ctx, c.reg, c.codec, req), nil
}

func (c *inprocClient) Close() error { return nil }
