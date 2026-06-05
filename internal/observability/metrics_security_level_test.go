package observability

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestListenerSecurityLevel_GaugeShape exercises the new gauge end-to-end
// against a private registry so we do not collide with the package-wide
// default registerer when this runs alongside other tests.
func TestListenerSecurityLevel_GaugeShape(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.ListenerSecurityLevel.WithLabelValues("delivery", "tls").Set(2)
	m.ListenerSecurityLevel.WithLabelValues("admin", "tls").Set(2)
	m.ListenerSecurityLevel.WithLabelValues("ingress", "insecure").Set(0)

	const expected = `
# HELP reflw_listener_security_level Transport security level per gRPC listener. 0=NoSecurity, 1=IntegrityOnly, 2=PrivacyAndIntegrity.
# TYPE reflw_listener_security_level gauge
reflw_listener_security_level{driver="insecure",listener="ingress"} 0
reflw_listener_security_level{driver="tls",listener="admin"} 2
reflw_listener_security_level{driver="tls",listener="delivery"} 2
`
	if err := testutil.GatherAndCompare(reg,
		strings.NewReader(expected),
		"reflw_listener_security_level"); err != nil {
		t.Fatal(err)
	}
}
