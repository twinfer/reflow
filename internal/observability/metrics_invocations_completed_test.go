package observability

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestInvocationsCompleted_CounterShape exercises the outcome counter
// against a private registry. Confirms the four documented outcomes
// (success, failure, cancelled, step_budget_exhausted) all increment
// independently and serialise with the expected label set.
func TestInvocationsCompleted_CounterShape(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.InvocationsCompleted.WithLabelValues("Orders", "success").Inc()
	m.InvocationsCompleted.WithLabelValues("Orders", "success").Inc()
	m.InvocationsCompleted.WithLabelValues("Orders", "failure").Inc()
	m.InvocationsCompleted.WithLabelValues("Orders", "cancelled").Inc()
	m.InvocationsCompleted.WithLabelValues("Workers", "step_budget_exhausted").Inc()

	const expected = `
# HELP reflow_invocations_completed_total Invocations that reached the Completed status, classified by outcome (success, failure, cancelled, step_budget_exhausted).
# TYPE reflow_invocations_completed_total counter
reflow_invocations_completed_total{outcome="cancelled",service="Orders"} 1
reflow_invocations_completed_total{outcome="failure",service="Orders"} 1
reflow_invocations_completed_total{outcome="step_budget_exhausted",service="Workers"} 1
reflow_invocations_completed_total{outcome="success",service="Orders"} 2
`
	if err := testutil.GatherAndCompare(reg,
		strings.NewReader(expected),
		"reflow_invocations_completed_total"); err != nil {
		t.Fatal(err)
	}
}
