package iflowengine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/twinfer/reflwos/capability"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// The capability-bridge is one deployed reflow handler that services every
// TaskInvoke the adapter emits by running the named iflow capability. These
// constants are the InvocationTarget the adapter addresses; the handler body and
// its registration land with step (c). The adapter depends only on the target
// address and the input schema, so it can emit TaskInvoke before the handler
// exists (a service-task model then simply fails cleanly at dispatch).
const (
	// BridgeService is the reflow service name the bridge handler is deployed under.
	BridgeService = "iflow.capability"
	// BridgeHandler is the handler name within BridgeService.
	BridgeHandler = "execute"
)

// BridgeInput is the payload the adapter marshals into TaskInvoke.Input and the
// bridge handler reads from ctx.Input(). It carries the iflow capability ref plus
// everything capability.Handler.Execute needs. Marshaled deterministically
// (struct field order + sorted map keys) so the enclosing ProcessAdvanced is
// byte-stable under replay. No tenant: reflow is single-tenant (iflow's tenancy
// is a dboshost-driver concern), so the capability.Request Principal is zero.
type BridgeInput struct {
	Ref  string         `json:"ref"`
	Vars map[string]any `json:"vars,omitempty"`
	Ext  []byte         `json:"ext,omitempty"`
}

// bridgeTarget is the InvocationTarget every RunServiceTask routes to.
func bridgeTarget() *enginev1.InvocationTarget {
	return &enginev1.InvocationTarget{ServiceName: BridgeService, HandlerName: BridgeHandler}
}

func encodeBridgeInput(ref string, vars map[string]any, ext []byte) ([]byte, error) {
	return json.Marshal(BridgeInput{Ref: ref, Vars: vars, Ext: ext})
}

// bridgeFailureCode is the Failure.Code the bridge stamps on a terminal
// capability fault. reflow's terminal-failure code channel is a uint32 and the
// engine never interprets it; the BPMN *string* error code instead rides the
// failure message as a bridgeFault envelope (see encodeBridgeFault), which
// eventForBPMN re-splits into bpmn.TaskFailed.ErrorCode so a coded business
// fault can reach the matching error boundary rather than only the catch-all.
const bridgeFailureCode uint32 = 1

// bridgeFault carries a coded BPMN business fault across reflow's terminal-
// failure channel. capability error codes are strings, but the channel is a
// uint32 code + a free-text message, so the string code rides the message: the
// bridge marshals (code, message) here when the capability returned a BPMN error
// code, reflow ferries the bytes verbatim (InvocationCompleted.failure_message →
// ProcessTaskCompleted.failure_message), and eventForBPMN re-splits them into
// bpmn.TaskFailed.{ErrorCode,Cause}. The envelope is iflow-private — internal/
// engine never decodes it — which is why the string code lives here and not on
// the core proto, whose failure_code is a uint32 mirror of handler.Failure.Code.
type bridgeFault struct {
	Code    string `json:"code"`
	Message string `json:"msg"`
}

// encodeBridgeFault renders a coded capability fault as a terminal-failure
// message. Called only when code != ""; a non-coded fault keeps its bare cause.
func encodeBridgeFault(code, msg string) string {
	b, err := json.Marshal(bridgeFault{Code: code, Message: msg})
	if err != nil { // unreachable for two strings; degrade to the bare cause
		return msg
	}
	return string(b)
}

// decodeBridgeFault splits a terminal-failure message produced by
// encodeBridgeFault back into (code, cause). Any message that is not a coded
// envelope — every non-iflow failure, every iflow fault without a BPMN error
// code, and a reflow-synthesized failure (step-budget, cancel) — returns code ""
// (the catch-all boundary) and the message unchanged.
func decodeBridgeFault(s string) (code, cause string) {
	var bf bridgeFault
	if err := json.Unmarshal([]byte(s), &bf); err == nil && bf.Code != "" {
		return bf.Code, bf.Message
	}
	return "", s
}

// encodeProcessFailure renders a child ProcessFailed terminal as a
// ProcessTerminal.FailureMessage. An escalation cause ("escalation:CODE",
// emitted by an uncaught escalation throw/end event in the child) is promoted
// into a bridgeFault envelope whose code is the full "escalation:CODE" — so the
// calling process's eventForBPMN recovers it as TaskFailed.ErrorCode and
// advanceTaskFailed's CutPrefix still matches — letting a CallActivity
// escalation boundary catch a child escalation. Any other cause stays a bare,
// human-readable message → parent ErrorCode "" (catch-all), as before.
func encodeProcessFailure(nodeID, cause string) string {
	msg := fmt.Sprintf("process failed at %q: %s", nodeID, cause)
	if strings.HasPrefix(cause, escalationPrefix) {
		return encodeBridgeFault(cause, msg)
	}
	return msg
}

// RegisterBridge installs the capability-bridge handler into reg, closing over
// capReg. Call at boot, before the InProcDialer is assembled, so the deployment
// seeding advertises it. One bridge serves both BPMN and CMMN — capability.Request
// is engine-neutral.
func RegisterBridge(reg *handler.Registry, capReg *capability.Registry) error {
	return reg.RegisterService(BridgeService, BridgeHandler, bridgeHandler(capReg))
}

// bridgeHandler runs the iflow capability named by the BridgeInput and returns
// its outputs as JSON. The effectful call is wrapped in ctx.Run so it is
// journaled exactly-once: a replay returns the recorded result without
// re-invoking the capability.
func bridgeHandler(capReg *capability.Registry) handler.Handler {
	return func(ctx handler.Context, input []byte) ([]byte, error) {
		var bi BridgeInput
		if err := json.Unmarshal(input, &bi); err != nil {
			return nil, handler.NewFailure(bridgeFailureCode, fmt.Sprintf("iflow bridge: decode input: %v", err))
		}
		return ctx.Run("capability:"+capability.Namespace(bi.Ref), func(_ *handler.RunContext) ([]byte, error) {
			return runCapability(ctx.Context(), capReg, bi)
		})
	}
}

// runCapability resolves and executes the capability, mapping its outcome onto
// the (bytes, error) contract ctx.Run expects: a coded (business) fault and an
// unresolved ref become a terminal *handler.Failure so the engine's
// error-boundary logic runs and reflow does not retry; a bare error stays
// transient so reflow retries the side effect under the Run's backoff policy.
func runCapability(ctx context.Context, capReg *capability.Registry, bi BridgeInput) ([]byte, error) {
	h, ok := capReg.Resolve(bi.Ref)
	if !ok {
		return nil, handler.NewFailure(bridgeFailureCode, fmt.Sprintf("iflow bridge: unresolved capability %q", bi.Ref))
	}
	out, err := h.Execute(ctx, capability.Request{
		Ref:           bi.Ref,
		Vars:          bi.Vars,
		ExtensionsXML: bi.Ext,
	})
	if err != nil {
		if code := capability.ErrorCodeOf(err); code != "" {
			return nil, handler.NewFailure(bridgeFailureCode, encodeBridgeFault(code, err.Error()))
		}
		return nil, err
	}
	return json.Marshal(out)
}
