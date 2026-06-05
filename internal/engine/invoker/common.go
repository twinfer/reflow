package invoker

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"time"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// Proposer is the subset of engine.RaftProposer the invoker uses to
// commit journal appends. Carved out so unit tests can substitute a fake
// without dragging dragonboat into the package.
type Proposer interface {
	ProposeSelf(ctx context.Context, cmd *enginev1.Command) error
}

// proposeTimeout bounds a single ProposeSelf call. Mirrors the timer
// service's value (timer_service.go:288). Independent of any session
// context so a stuck Raft doesn't hang the handler indefinitely.
const proposeTimeout = 5 * time.Second

// DefaultEagerStateMaxBytes is the default cap on the total byte size of
// the eager-state snapshot delivered with StartMessage. Operators tune
// this via Config.Handlers.EagerStateMaxBytes (zero on Invoker.Config
// means "use this default"). Larger object states fall back to lazy
// fetch.
const DefaultEagerStateMaxBytes uint32 = 64 * 1024

// errStatePreloadOverflow is the sentinel returned from ScanObject's
// callback to short-circuit a too-large scan.
var errStatePreloadOverflow = errors.New("state preload overflow")

// preloadEagerState reads every state row scoped to (service, object_key)
// into an in-memory map. Returns:
//
//   - (nil, false)   — unkeyed service (no per-object state).
//   - (nil, false)   — scan failure mid-stream; logged.
//   - (cache, false) — full snapshot fit under maxBytes.
//   - (cache, true)  — overflow: cache holds the keys that fit before the
//     limit tripped, in scan order. Wire callers set
//     StartMessage.PartialState=true so the SDK lazy-fetches keys that
//     fall outside the partial snapshot.
//
// Pre-lazy-fetch, the partial cache was discarded on overflow because
// the SDK had no way to retrieve the missing keys. Now that the SDK can
// emit GetLazyStateCommandMessage for any cache miss, returning what fit
// keeps reads cheap for the keys that did make it into the snapshot.
func preloadEagerState(
	stateTable tables.StateTable,
	target *enginev1.InvocationTarget,
	id *enginev1.InvocationId,
	maxBytes uint32,
	log *slog.Logger,
) (cache map[string][]byte, overflowed bool) {
	if target.GetObjectKey() == "" {
		return nil, false
	}
	if maxBytes == 0 {
		maxBytes = DefaultEagerStateMaxBytes
	}
	cache = make(map[string][]byte)
	total := uint32(0)
	// LP for state rows is recomputed from the routing tuple so it matches the
	// apply path's writes (partition.go entityPK) and stays robust to synthetic
	// test ids whose pk doesn't follow the mint invariant id.pk ==
	// PartitionKey(svc, objKey).
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	err := stateTable.ScanObject(lp, target, func(key string, value []byte) error {
		total += uint32(len(key) + len(value))
		if total > maxBytes {
			overflowed = true
			return errStatePreloadOverflow
		}
		cache[key] = append([]byte(nil), value...)
		return nil
	})
	if overflowed {
		log.Info("invoker: state preload overflow; partial snapshot retained, lazy fetch covers the rest",
			"id", invocationIDString(id),
			"limit_bytes", maxBytes,
			"preloaded_keys", len(cache),
		)
		return cache, true
	}
	if err != nil {
		log.Warn("invoker: state preload scan failed",
			"id", invocationIDString(id), "err", err)
		return nil, false
	}
	return cache, false
}

// sessionKey builds a stable string key from id's raw 24-byte
// representation (8-byte partition_key BE || 16-byte uuid). Used as the
// map key in Invoker.sessions; cheaper than reflect.DeepEqual and stable
// across replays.
func sessionKey(id *enginev1.InvocationId) string {
	if id == nil {
		return ""
	}
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[:8], id.GetPartitionKey())
	copy(buf[8:24], id.GetUuid())
	return string(buf[:])
}

// procSessionKey builds a stable map key for a process instance from its
// banded partition_key + (service, instance_key). Used in Invoker.procSessions.
func procSessionKey(pk uint64, service, instanceKey string) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], pk)
	return string(b[:]) + service + "\x00" + instanceKey
}

// invocationIDString renders id as "<partition_key_hex>:<uuid_hex>" for
// log lines. Lazy-allocated — only called inside log-statements.
func invocationIDString(id *enginev1.InvocationId) string {
	if id == nil {
		return "<nil>"
	}
	var pk [8]byte
	binary.BigEndian.PutUint64(pk[:], id.GetPartitionKey())
	return hex(pk[:]) + ":" + hex(id.GetUuid())
}

func hex(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[i*2] = digits[x>>4]
		out[i*2+1] = digits[x&0x0f]
	}
	return string(out)
}
