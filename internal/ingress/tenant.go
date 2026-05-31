package ingress

import (
	"context"
	"fmt"

	connect "connectrpc.com/connect"

	"github.com/twinfer/reflow/internal/auth"
	"github.com/twinfer/reflow/internal/storage/keys"
)

// principalTenant returns the tenant band of the request's authenticated
// principal — the single place a tenant value enters the system. The band is
// folded into partition_key by routing.PartitionKey and recovered downstream
// from the id, so no other ingress RPC needs to thread it explicitly.
//
// Anonymous and non-tenant principals (operator/node/user) map to band 0, the
// default/untenanted tenant. A verified tenant/<n> principal supplies band n.
// An n outside [0, MaxTenantBand) is a misissued leaf — rejected rather than
// silently masked into another tenant's band (BandLP would alias it).
func principalTenant(ctx context.Context) (uint32, error) {
	p, ok := auth.PrincipalFromContext(ctx)
	if !ok {
		return 0, nil
	}
	t := auth.TenantIDFromPrincipal(p)
	if t >= keys.MaxTenantBand {
		return 0, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("tenant id %d out of range [0,%d)", t, keys.MaxTenantBand))
	}
	return t, nil
}
