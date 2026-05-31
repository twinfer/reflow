package ingress

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// FormatInvocationID encodes an InvocationId as a stable, URL-safe string.
// Format: "inv_<tenant:8hex>_<partition_key:16hex>_<uuid:32hex>". 61 ASCII
// bytes total. Stable across phases; Restate uses Crockford-base32, which we
// may swap to later. The hex form is unambiguous and trivially reversible.
func FormatInvocationID(id *enginev1.InvocationId) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("inv_%08x_%016x_%s", id.GetTenantId(), id.GetPartitionKey(), hex.EncodeToString(id.GetUuid()))
}

// ParseInvocationID is the inverse of FormatInvocationID.
func ParseInvocationID(s string) (*enginev1.InvocationId, error) {
	if !strings.HasPrefix(s, "inv_") {
		return nil, errors.New("invocation id: missing inv_ prefix")
	}
	rest := strings.TrimPrefix(s, "inv_")
	parts := strings.SplitN(rest, "_", 3)
	if len(parts) != 3 {
		return nil, errors.New("invocation id: malformed (want inv_<tenant>_<pk>_<uuid>)")
	}
	if len(parts[0]) != 8 {
		return nil, fmt.Errorf("invocation id: tenant must be 8 hex chars (got %d)", len(parts[0]))
	}
	if len(parts[1]) != 16 {
		return nil, fmt.Errorf("invocation id: partition_key must be 16 hex chars (got %d)", len(parts[1]))
	}
	if len(parts[2]) != 32 {
		return nil, fmt.Errorf("invocation id: uuid must be 32 hex chars (got %d)", len(parts[2]))
	}
	var tenant uint32
	if _, err := fmt.Sscanf(parts[0], "%08x", &tenant); err != nil {
		return nil, fmt.Errorf("invocation id: parse tenant: %w", err)
	}
	var pk uint64
	if _, err := fmt.Sscanf(parts[1], "%016x", &pk); err != nil {
		return nil, fmt.Errorf("invocation id: parse partition_key: %w", err)
	}
	uuid, err := hex.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("invocation id: parse uuid: %w", err)
	}
	return &enginev1.InvocationId{TenantId: tenant, PartitionKey: pk, Uuid: uuid}, nil
}
