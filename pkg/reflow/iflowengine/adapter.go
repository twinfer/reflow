package iflowengine

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twinfer/reflow/internal/engine/invoker"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// Adapter implements invoker.ProcessEngine by driving iflow's pure BPMN/CMMN
// reducers and translating their emitted Commands into a reflow ProcessAdvanced.
// It is stateless and pure: one Advance decodes one event, runs exactly one
// engine turn, and returns the instructions reflow's partition will actuate.
// All model access goes through an injected, cache-backed ModelResolver, so the
// turn does no I/O and is byte-for-byte reproducible under replay.
type Adapter struct {
	models ModelResolver
}

// New builds an Adapter over the given model resolver.
func New(models ModelResolver) *Adapter {
	return &Adapter{models: models}
}

var _ invoker.ProcessEngine = (*Adapter)(nil)

// retentionResolver is the optional capability a ModelResolver implements to
// declare a per-model history-retention window (the Camunda historyTimeToLive
// analog). Kept off the ModelResolver interface so a resolver that doesn't
// parse a window (e.g. a custom test resolver) keeps the immediate-delete
// default rather than failing to compile.
type retentionResolver interface {
	RetentionMs(ref *enginev1.ModelRef) uint64
}

// retentionMs resolves the model's history window in ms, or 0 (immediate
// delete) when the resolver doesn't implement the optional capability.
func (a *Adapter) retentionMs(ref *enginev1.ModelRef) uint64 {
	if rr, ok := a.models.(retentionResolver); ok {
		return rr.RetentionMs(ref)
	}
	return 0
}

// Advance runs one iflow engine turn for the instance described by in.Record,
// driven by in.Entry. See the package and Adapter docs for the contract. A
// returned error is converted by reflow's processSession into a failed
// ProcessTerminal, so model/translation failures fail just this instance.
func (a *Adapter) Advance(_ context.Context, in invoker.ProcessAdvanceInput) (*enginev1.ProcessAdvanced, error) {
	switch k := in.Record.GetKind(); k {
	case enginev1.ProcessKind_PROCESS_KIND_BPMN:
		return a.advanceBPMN(in)
	case enginev1.ProcessKind_PROCESS_KIND_CMMN:
		return a.advanceCMMN(in)
	default:
		return nil, fmt.Errorf("iflowengine: unknown process kind %v", k)
	}
}

// tenantOf renders reflow's numeric per-partition tenant id as the string
// identity.Principal.TenantID the iflow capability layer expects. The mapping is
// a convention for the first cut; a deployment that needs the original tenant
// string would resolve it out of band.
func tenantOf(pk uint64) string {
	return strconv.FormatUint(uint64(keys.TenantFromPartitionKey(pk)), 10)
}
