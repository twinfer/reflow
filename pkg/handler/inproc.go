package handler

import (
	"context"

	"github.com/twinfer/reflw/pkg/handler/wire"
	discoveryv1 "github.com/twinfer/reflw/proto/discoveryv1"
	handlerv1 "github.com/twinfer/reflw/proto/handlerv1"
)

// InvokeInProc runs one invocation session entirely in-process: req carries
// the StartMessage + replay frames, and the returned response carries the
// handler's emitted command frames + the terminal Output/Suspension/Error
// frame. It is the in-process transport entry — the same core (runInvoke)
// the Connect unary handler uses, but with no HTTP and no serialization
// beyond the inner codec. The engine's in-process handlerclient bridge calls
// this directly. A session error surfaces in-band as a terminal frame in the
// response, never as a Go error.
func InvokeInProc(ctx context.Context, reg *Registry, codec wire.Codec, req *handlerv1.InvokeRequest) *handlerv1.InvokeResponse {
	if codec == nil {
		codec = wire.DefaultCodec()
	}
	return &handlerv1.InvokeResponse{Frames: runInvoke(ctx, reg, codec, req.GetFrames())}
}

// LocalDiscovery synthesizes the DiscoveryResponse for reg without a network
// round-trip — the in-process equivalent of the DiscoveryService.Discover
// RPC. Used to register an in-process deployment, whose inproc:// URL the
// engine cannot reach over HTTP to run the usual discovery probe.
func LocalDiscovery(reg *Registry) *discoveryv1.DiscoveryResponse {
	return buildDiscoveryResponse(reg)
}
