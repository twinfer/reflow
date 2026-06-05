package loadgen

import (
	"crypto/rand"
	"fmt"

	"github.com/twinfer/reflw/internal/engine/routing"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// mintInvocationID stamps a fresh UUIDv4 with the partition_key derived
// from (service, object_key). Matches the format the ingress server
// produces so in-process and subprocess invocations are
// indistinguishable downstream.
func mintInvocationID(target *enginev1.InvocationTarget) *enginev1.InvocationId {
	uuid := make([]byte, 16)
	_, _ = rand.Read(uuid)
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return &enginev1.InvocationId{
		PartitionKey: routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()),
		Uuid:         uuid,
	}
}

// formatInvocationKey is a stable, opaque-looking string suitable for
// use as a per-invocation producerID. Not parsed by anyone — just needs
// to be unique per invocation so the FSM dedup keys never collide.
func formatInvocationKey(id *enginev1.InvocationId) string {
	return fmt.Sprintf("%d:%x", id.GetPartitionKey(), id.GetUuid())
}
