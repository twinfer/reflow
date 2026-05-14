// Package keys defines the Pebble key codec for a single reflow partition's
// state store. Because each partition has its own Pebble DB, keys do NOT carry
// a partition_id prefix — isolation is at the DB level.
//
// Namespaces (top-level prefixes):
//
//	meta                                         -> PartitionMeta singleton
//	format                                       -> uint32 BE storage_format_version
//	inv/<24-byte inv_id>                         -> InvocationStatus
//	journal/<24-byte inv_id>/<4-byte BE u32 idx> -> JournalEntry
//	timer/<8-byte BE fire_at_ms>/<24-byte id>    -> uint32 sleep_index
//	timer_idx/<24-byte id>/<8-byte BE fire_at_ms> -> "" (secondary index)
//	state/<service>/<obj_key>/<state_key>        -> bytes (Phase 2 lazy state)
//	outbox/<8-byte BE seq>                       -> OutboxEnvelope (Phase 2)
//	awakeable/<26-byte id>                       -> AwakeableEntry (Phase 2)
//	keylease/<service>/<obj_key>                 -> KeyLeaseStatus (Phase 3)
//	idempotency/<32-byte sha256>                 -> InvocationId (Phase 3)
//	dedup/self/<8-byte BE leader_epoch>/<8-byte BE seq>          -> DedupEntry
//	dedup/arbitrary/<producer_id>/<8-byte BE seq>                -> DedupEntry
//
// All multi-byte integers in keys are big-endian so lexicographic byte order
// equals numeric order. Invocation IDs are encoded as a fixed 24-byte raw
// form: 8-byte BE partition_key followed by 16-byte uuid. The fixed length is
// what makes namespace boundaries unambiguous (no prefix can be longer than
// the namespace + 24 bytes).
package keys

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

const (
	invocationIDLen = 24 // 8-byte partition_key + 16-byte uuid

	// awakeableIDLen is the fixed wire length of an awakeable identifier:
	// 4-byte "awk_" prefix + 22-char base64url-encoded 16-byte body. The
	// body's first 8 bytes are the owning invocation's partition_key
	// (big-endian); the remaining 8 are random. Anchoring the length lets
	// prefix scans on awakeable/ stay unambiguous and lets ingress derive
	// the owner's shard from the id alone — no fan-out lookup needed.
	awakeableIDLen   = 26
	awakeableBodyLen = 16

	metaPrefix      = "meta"
	formatPrefix    = "format"
	invPrefix       = "inv/"
	journalPrefix   = "journal/"
	timerPrefix     = "timer/"
	timerIdxPrefix  = "timer_idx/"
	statePrefix     = "state/"
	outboxPrefix    = "outbox/"
	awakeablePrefix = "awakeable/"
	keyLeasePrefix  = "keylease/"
	idempPrefix     = "idempotency/"
	dedupSelfPrefix = "dedup/self/"
	dedupArbPrefix  = "dedup/arbitrary/"
)

// ErrInvalidInvocationID is returned when an InvocationId has a uuid field of
// the wrong length.
var ErrInvalidInvocationID = errors.New("invocation id must have 16-byte uuid")

// EncodeInvocationID returns the canonical 24-byte raw form of an InvocationId.
func EncodeInvocationID(id *enginev1.InvocationId) ([]byte, error) {
	if len(id.GetUuid()) != 16 {
		return nil, ErrInvalidInvocationID
	}
	out := make([]byte, invocationIDLen)
	binary.BigEndian.PutUint64(out[:8], id.GetPartitionKey())
	copy(out[8:], id.GetUuid())
	return out, nil
}

// DecodeInvocationID is the inverse of EncodeInvocationID.
func DecodeInvocationID(buf []byte) (*enginev1.InvocationId, error) {
	if len(buf) != invocationIDLen {
		return nil, fmt.Errorf("invocation id raw length = %d; want %d", len(buf), invocationIDLen)
	}
	return &enginev1.InvocationId{
		PartitionKey: binary.BigEndian.Uint64(buf[:8]),
		Uuid:         append([]byte(nil), buf[8:]...),
	}, nil
}

// MetaKey returns the singleton key for the partition's PartitionMeta record.
func MetaKey() []byte { return []byte(metaPrefix) }

// FormatVersionKey returns the singleton key for the per-DB storage format
// version. Value is a 4-byte big-endian uint32. Lives in every reflow pebble
// DB (metadata shard + per-partition shards) so the local boot path can refuse
// to open a DB written by an incompatible binary.
func FormatVersionKey() []byte { return []byte(formatPrefix) }

// InvocationKey returns inv/<24-byte id>.
func InvocationKey(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(invPrefix)+invocationIDLen)
	out = append(out, invPrefix...)
	return append(out, raw...), nil
}

// JournalPrefix returns journal/<24-byte id>/.
//
// Use with PrefixUpperBound to scan every entry for an invocation.
func JournalPrefix(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(journalPrefix)+invocationIDLen+1)
	out = append(out, journalPrefix...)
	out = append(out, raw...)
	return append(out, '/'), nil
}

// JournalKey returns journal/<24-byte id>/<4-byte BE index>.
func JournalKey(id *enginev1.InvocationId, index uint32) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(journalPrefix)+invocationIDLen+1+4)
	out = append(out, journalPrefix...)
	out = append(out, raw...)
	out = append(out, '/')
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], index)
	return append(out, idxBuf[:]...), nil
}

// TimerPrefix returns the timer/ namespace prefix.
func TimerPrefix() []byte { return []byte(timerPrefix) }

// TimerKey returns timer/<8-byte BE fire_at>/<24-byte id>. Sorted by fire
// time then invocation id, which is what the timer service scans.
func TimerKey(fireAtMs uint64, id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(timerPrefix)+8+invocationIDLen)
	out = append(out, timerPrefix...)
	var fireBuf [8]byte
	binary.BigEndian.PutUint64(fireBuf[:], fireAtMs)
	out = append(out, fireBuf[:]...)
	return append(out, raw...), nil
}

// DecodeTimerKey extracts (fireAtMs, invocation_id) from a timer key.
func DecodeTimerKey(key []byte) (uint64, *enginev1.InvocationId, error) {
	want := len(timerPrefix) + 8 + invocationIDLen
	if len(key) != want {
		return 0, nil, fmt.Errorf("timer key length = %d; want %d", len(key), want)
	}
	p := len(timerPrefix)
	fireAt := binary.BigEndian.Uint64(key[p : p+8])
	id, err := DecodeInvocationID(key[p+8:])
	return fireAt, id, err
}

// TimerIdxKey returns timer_idx/<24-byte id>/<8-byte BE fire_at>. The
// secondary index lets onPurge find every pending timer for an
// invocation with a single bounded range scan instead of walking the
// whole timer/ namespace.
func TimerIdxKey(id *enginev1.InvocationId, fireAtMs uint64) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(timerIdxPrefix)+invocationIDLen+8)
	out = append(out, timerIdxPrefix...)
	out = append(out, raw...)
	var fireBuf [8]byte
	binary.BigEndian.PutUint64(fireBuf[:], fireAtMs)
	return append(out, fireBuf[:]...), nil
}

// TimerIdxPrefix returns the timer_idx/ namespace prefix, suitable for a
// range scan over every secondary-index row in the partition.
func TimerIdxPrefix() []byte { return []byte(timerIdxPrefix) }

// TimerIdxPrefixForID returns timer_idx/<24-byte id>/, suitable for a
// range scan over every secondary-index row for one invocation.
func TimerIdxPrefixForID(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(timerIdxPrefix)+invocationIDLen)
	out = append(out, timerIdxPrefix...)
	return append(out, raw...), nil
}

// DecodeTimerIdxKey extracts (invocation_id, fireAtMs) from a secondary-
// index key.
func DecodeTimerIdxKey(key []byte) (*enginev1.InvocationId, uint64, error) {
	want := len(timerIdxPrefix) + invocationIDLen + 8
	if len(key) != want {
		return nil, 0, fmt.Errorf("timer_idx key length = %d; want %d", len(key), want)
	}
	p := len(timerIdxPrefix)
	id, err := DecodeInvocationID(key[p : p+invocationIDLen])
	if err != nil {
		return nil, 0, err
	}
	fireAt := binary.BigEndian.Uint64(key[p+invocationIDLen:])
	return id, fireAt, nil
}

// DedupSelfKey returns dedup/self/<8-byte BE leader_epoch>/<8-byte BE seq>.
// Exact-match presence semantics: each (epoch, seq) pair gets its own key so
// dedup tolerates concurrent goroutines allocating seq atomically and
// submitting to raft out-of-order. A high-water-mark scheme would
// false-positive in that case: replica A applies seq=N+1 first, records the
// high-water as N+1, then applies seq=N — IsDuplicate sees N+1 >= N and
// skips the fresh propose. Distinct replicas diverge depending on whether
// seq=N and seq=N+1 land in the same Update batch (where the in-batch dedup
// record is invisible to the within-batch IsDuplicate read).
func DedupSelfKey(leaderEpoch, seq uint64) []byte {
	out := make([]byte, 0, len(dedupSelfPrefix)+16)
	out = append(out, dedupSelfPrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], leaderEpoch)
	out = append(out, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], seq)
	return append(out, buf[:]...)
}

// DedupArbitraryKey returns dedup/arbitrary/<producer_id>/<8-byte BE seq>.
// Exact-match keying for the same reason as DedupSelfKey — concurrent
// producers (e.g. loadgen goroutines) allocate seq from a shared atomic
// counter and submit out-of-order to dragonboat.
func DedupArbitraryKey(producerID string, seq uint64) []byte {
	out := make([]byte, 0, len(dedupArbPrefix)+len(producerID)+1+8)
	out = append(out, dedupArbPrefix...)
	out = append(out, producerID...)
	out = append(out, '/')
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	return append(out, buf[:]...)
}

// StatePrefix returns the state/ namespace prefix. Exported so other
// packages can avoid colliding with it.
func StatePrefix() []byte { return []byte(statePrefix) }

// StateKey returns state/<service>/<obj_key>/<state_key>. For unkeyed
// services pass objectKey="". Callers must ensure none of the three
// components contain '/', otherwise the namespace boundary is ambiguous —
// the API surface in pkg/sdk rejects invalid keys before they reach here.
func StateKey(service, objectKey, stateKey string) []byte {
	out := make([]byte, 0, len(statePrefix)+len(service)+1+len(objectKey)+1+len(stateKey))
	out = append(out, statePrefix...)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, objectKey...)
	out = append(out, '/')
	return append(out, stateKey...)
}

// StatePrefixForObject returns state/<service>/<obj_key>/, suitable as the
// LowerBound for a per-object scan paired with PrefixUpperBound for the
// matching UpperBound.
func StatePrefixForObject(service, objectKey string) []byte {
	out := make([]byte, 0, len(statePrefix)+len(service)+1+len(objectKey)+1)
	out = append(out, statePrefix...)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, objectKey...)
	return append(out, '/')
}

// OutboxPrefix returns the outbox/ namespace prefix.
func OutboxPrefix() []byte { return []byte(outboxPrefix) }

// OutboxKey returns outbox/<8-byte BE seq>. Big-endian guarantees
// lexicographic byte order matches numeric order, so a forward scan from
// OutboxPrefix yields pending envelopes in FIFO insertion order.
func OutboxKey(seq uint64) []byte {
	out := make([]byte, 0, len(outboxPrefix)+8)
	out = append(out, outboxPrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	return append(out, buf[:]...)
}

// DecodeOutboxKey returns the sequence number from an outbox key.
func DecodeOutboxKey(key []byte) (uint64, error) {
	want := len(outboxPrefix) + 8
	if len(key) != want {
		return 0, fmt.Errorf("outbox key length = %d; want %d", len(key), want)
	}
	return binary.BigEndian.Uint64(key[len(outboxPrefix):]), nil
}

// AwakeablePrefix returns the awakeable/ namespace prefix.
func AwakeablePrefix() []byte { return []byte(awakeablePrefix) }

// KeyLeaseKey returns keylease/<service>/<obj_key>. For unkeyed targets
// callers must skip this namespace entirely; the VO gate is only consulted
// for keyed invocations.
func KeyLeaseKey(service, objectKey string) []byte {
	out := make([]byte, 0, len(keyLeasePrefix)+len(service)+1+len(objectKey))
	out = append(out, keyLeasePrefix...)
	out = append(out, service...)
	out = append(out, '/')
	return append(out, objectKey...)
}

// IdempotencyKey returns idempotency/<sha256(tuple)>. The tuple is the
// caller-supplied (service, handler, object_key, idempotency_key) hashed
// with length-prefixed components so adjacent fields never alias. Used by
// the Phase 3 onInvoke dedup path: a hit means an invocation with the same
// tuple was already accepted; the stored value is the prior InvocationId.
func IdempotencyKey(service, handler, objectKey, idempotencyKey string) []byte {
	h := sha256.New()
	writeLP := func(s string) {
		var lp [4]byte
		binary.BigEndian.PutUint32(lp[:], uint32(len(s)))
		h.Write(lp[:])
		h.Write([]byte(s))
	}
	writeLP(service)
	writeLP(handler)
	writeLP(objectKey)
	writeLP(idempotencyKey)
	sum := h.Sum(nil)
	out := make([]byte, 0, len(idempPrefix)+len(sum))
	out = append(out, idempPrefix...)
	return append(out, sum...)
}

// AwakeableKey returns awakeable/<26-byte id>. The caller is responsible
// for validating the id via ValidateAwakeableID before constructing the
// key; passing a malformed id here produces a syntactically valid key but
// risks collision with future namespace extensions.
func AwakeableKey(id string) []byte {
	out := make([]byte, 0, len(awakeablePrefix)+len(id))
	out = append(out, awakeablePrefix...)
	return append(out, id...)
}

// AwakeableOwnerPartitionKey returns the owner invocation's partition_key
// embedded in the awakeable id. Returns an error if the id is malformed or
// the body isn't base64url-decodable to the expected 16 bytes — callers
// should treat that as InvalidArgument.
func AwakeableOwnerPartitionKey(id string) (uint64, error) {
	if err := ValidateAwakeableID(id); err != nil {
		return 0, err
	}
	body, err := base64.RawURLEncoding.DecodeString(id[4:])
	if err != nil {
		return 0, fmt.Errorf("awakeable id body decode: %w", err)
	}
	if len(body) != awakeableBodyLen {
		return 0, fmt.Errorf("awakeable id body length = %d; want %d", len(body), awakeableBodyLen)
	}
	return binary.BigEndian.Uint64(body[:8]), nil
}

// ValidateAwakeableID enforces the awk_<22-char base64url> shape. Used at
// mint time (SDK) and resolution time (ingress) so prefix scans on the
// awakeable/ namespace stay unambiguous and no awakeable ID can shadow a
// nearby key.
func ValidateAwakeableID(id string) error {
	if len(id) != awakeableIDLen {
		return fmt.Errorf("awakeable id length = %d; want %d", len(id), awakeableIDLen)
	}
	if id[:4] != "awk_" {
		return fmt.Errorf("awakeable id must start with %q, got %q", "awk_", id[:4])
	}
	for i := 4; i < awakeableIDLen; i++ {
		c := id[i]
		ok := (c >= 'A' && c <= 'Z') ||
			(c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') ||
			c == '_' || c == '-'
		if !ok {
			return fmt.Errorf("awakeable id char at %d not [A-Za-z0-9_-]: %q", i, c)
		}
	}
	return nil
}

// PrefixUpperBound returns the smallest key strictly greater than every key
// that begins with the given prefix. Returns nil if the prefix is empty or
// consists entirely of 0xFF bytes — Pebble treats a nil UpperBound as "no
// upper bound".
//
// This fixes the aliasing + overflow bug in the original reflow code which
// did append(prefix[:n-1], prefix[n-1]+1).
func PrefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for len(out) > 0 && out[len(out)-1] == 0xFF {
		out = out[:len(out)-1]
	}
	if len(out) == 0 {
		return nil
	}
	out[len(out)-1]++
	return out
}
