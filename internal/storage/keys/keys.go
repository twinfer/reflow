// Package keys defines the Pebble key codec for a single reflow partition's
// state store. Because each partition has its own Pebble DB, keys do NOT carry
// a partition_id prefix — isolation is at the DB level.
//
// Most namespaces carry a 4-byte big-endian Logical Partition (LP) id
// immediately after the namespace string. LP is `partition_key % LPCount`;
// it lets a per-LP range scan resolve to a bounded iterator
// `[ns/<lp>, ns/<lp+1>)` without filter overhead. This is prerequisite shape
// for the cross-shard LP transfer protocol.
//
// LP-prefixed namespaces (LP source in parentheses):
//
//	inv/<lp:4><24-byte inv_id>                                       -> InvocationStatus    (id.PartitionKey)
//	journal/<lp:4><24-byte inv_id>/<4-byte BE u32 idx>               -> JournalEntry        (id.PartitionKey)
//	timer_idx/<lp:4><24-byte id>/<8-byte BE fire_at_ms>              -> "" (secondary)     (id.PartitionKey)
//	state/<lp:4><service>/<obj_key>/<state_key>                      -> bytes              (svc, obj)
//	awakeable/<lp:4><26-byte id>                                     -> AwakeableEntry     (AwakeableOwnerPartitionKey)
//	keylease/<lp:4><service>/<obj_key>                               -> KeyLeaseStatus     (svc, obj)
//	idempotency/<lp:4><32-byte sha256>                               -> InvocationId       (svc, obj_key)
//	signal_inbox/<lp:4><24-byte inv_id>/<name>                       -> SignalInboxEntry   (id.PartitionKey)
//	signal_awaiter/<lp:4><24-byte inv_id>/<name>                     -> SignalAwaiter      (id.PartitionKey)
//	workflow_run/<lp:4><service>/<workflow_key>                      -> InvocationId       (svc, wf_key)
//	promise/<lp:4><service>/<workflow_key>/<name>                    -> PromiseValue       (svc, wf_key)
//	promise_awaiter/<lp:4><service>/<workflow_key>/<name>/<idx:4>    -> PromiseAwaiter     (svc, wf_key)
//	timer_lp/<lp:4><8-byte BE fire_at_ms>/<24-byte id>               -> uint32 sleep_index (id.PartitionKey)
//	dedup/arbitrary/<lp:4><producer_id>/<8-byte BE seq>              -> DedupEntry         (command kind)
//
// LP-agnostic namespaces (singletons, ordering-sensitive, or shard-scoped):
//
//	meta                                                          -> PartitionMeta singleton
//	format                                                        -> uint32 BE storage_format_version
//	timer/<8-byte BE fire_at_ms>/<24-byte id>                     -> uint32 sleep_index (primary; fire_at order)
//	outbox/<8-byte BE seq>                                        -> OutboxEnvelope (FIFO)
//	dedup/self/<8-byte BE leader_epoch>/<8-byte BE seq>           -> DedupEntry (shard-scoped to leader epoch)
//	workflow_reap/<8-byte BE fire_at_ms>/<service>/<workflow_key> -> empty (fire_at order)
//	lp_freeze/<lp:4>                                              -> LPFreezeRow (PR 3 freeze gate)
//	lp_staging/<transfer_id>                                      -> LPStagingRow (PR 3 dest staging)
//
// dedup/arbitrary is LP-prefixed so the row rides the LP-transfer scan and
// follows the LP across shard moves; LP-agnostic arbitrary dedups (today only
// the OutboxAck command) key under the LPNoLP sentinel = 0xFFFF_FFFF, which
// is never a real LP (LPCount=4096) and therefore is never range-deleted by
// FinishLPTransfer.
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

	// LPCount is the forever-fixed number of Logical Partitions. Routing
	// reduces `partition_key % LPCount` to an LP id; the LP is embedded as a
	// 4-byte big-endian uint32 immediately after the namespace prefix of
	// every LP-prefixed key. 4096 is a power of two so the reduction is a
	// single bitwise AND (`pk & 0xFFF`).
	LPCount uint32 = 4096
	// LPLen is the byte width of the encoded LP field.
	LPLen = 4
	// LPNoLP is the sentinel LP for arbitrary dedup rows that don't belong
	// to any real LP — today only the OutboxAck command, which is shard-
	// internal and LP-agnostic. The sentinel is chosen so it can never
	// collide with a real LP (real LPs are < LPCount = 4096) and so a
	// per-LP range scan / range-delete never touches it.
	LPNoLP uint32 = 0xFFFF_FFFF

	metaPrefix           = "meta"
	formatPrefix         = "format"
	invPrefix            = "inv/"
	journalPrefix        = "journal/"
	timerPrefix          = "timer/"
	timerIdxPrefix       = "timer_idx/"
	timerLPPrefix        = "timer_lp/"
	statePrefix          = "state/"
	outboxPrefix         = "outbox/"
	awakeablePrefix      = "awakeable/"
	keyLeasePrefix       = "keylease/"
	idempPrefix          = "idempotency/"
	dedupSelfPrefix      = "dedup/self/"
	dedupArbPrefix       = "dedup/arbitrary/"
	signalInboxPrefix    = "signal_inbox/"
	signalAwaiterPrefix  = "signal_awaiter/"
	workflowRunPrefix    = "workflow_run/"
	promisePrefix        = "promise/"
	promiseAwaiterPrefix = "promise_awaiter/"
	workflowReapPrefix   = "workflow_reap/"
	lpFreezePrefix       = "lp_freeze/"
	lpStagingPrefix      = "lp_staging/"
)

// ErrInvalidInvocationID is returned when an InvocationId has a uuid field of
// the wrong length.
var ErrInvalidInvocationID = errors.New("invocation id must have 16-byte uuid")

// LPFromPartitionKey reduces a 64-bit partition_key to a Logical Partition id.
// LPCount is a power of two so the reduction is a bitwise mask.
func LPFromPartitionKey(pk uint64) uint32 {
	return uint32(pk & uint64(LPCount-1))
}

// appendLP appends the 4-byte big-endian encoding of lp to out.
func appendLP(out []byte, lp uint32) []byte {
	var b [LPLen]byte
	binary.BigEndian.PutUint32(b[:], lp)
	return append(out, b[:]...)
}

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

// DecodeInvocationID is the inverse of EncodeInvocationID. The input must be
// exactly the 24-byte body — callers reading from an LP-prefixed key slice the
// LP off first (e.g. `key[len("inv/")+LPLen:]`).
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

// InvocationKey returns inv/<lp:4><24-byte id>.
func InvocationKey(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(invPrefix)+LPLen+invocationIDLen)
	out = append(out, invPrefix...)
	out = appendLP(out, lp)
	return append(out, raw...), nil
}

// InvocationLPPrefix returns inv/<lp:4>, suitable as the LowerBound for a
// per-LP scan of every invocation in one logical partition. Pair with
// PrefixUpperBound for the UpperBound.
func InvocationLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(invPrefix)+LPLen)
	out = append(out, invPrefix...)
	return appendLP(out, lp)
}

// JournalPrefix returns journal/<lp:4><24-byte id>/.
//
// Use with PrefixUpperBound to scan every entry for an invocation.
func JournalPrefix(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(journalPrefix)+LPLen+invocationIDLen+1)
	out = append(out, journalPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	return append(out, '/'), nil
}

// JournalKey returns journal/<lp:4><24-byte id>/<4-byte BE index>.
func JournalKey(id *enginev1.InvocationId, index uint32) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(journalPrefix)+LPLen+invocationIDLen+1+4)
	out = append(out, journalPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	out = append(out, '/')
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], index)
	return append(out, idxBuf[:]...), nil
}

// TimerPrefix returns the timer/ namespace prefix. timer/ is LP-agnostic;
// the live timer service drains in fire_at order, so adding an LP discriminator
// would fragment the scan. The dual-written timer_lp/ secondary index carries
// the per-LP shape — see TimerLPKey.
func TimerPrefix() []byte { return []byte(timerPrefix) }

// TimerKey returns timer/<8-byte BE fire_at>/<24-byte id>. Sorted by fire
// time then invocation id, which is what the timer service scans. LP-agnostic.
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

// DecodeTimerKey extracts (fireAtMs, invocation_id) from a primary timer key.
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

// TimerLPKey returns timer_lp/<lp:4><8-byte BE fire_at>/<24-byte id>.
// Pair-written with TimerKey so the LP transfer protocol can extract
// every timer in an LP via a single bounded range scan. The value mirrors
// the primary row (4-byte BE sleep_index).
func TimerLPKey(lp uint32, fireAtMs uint64, id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(timerLPPrefix)+LPLen+8+invocationIDLen)
	out = append(out, timerLPPrefix...)
	out = appendLP(out, lp)
	var fireBuf [8]byte
	binary.BigEndian.PutUint64(fireBuf[:], fireAtMs)
	out = append(out, fireBuf[:]...)
	return append(out, raw...), nil
}

// TimerLPPrefix returns the timer_lp/ namespace prefix (whole-namespace scan).
func TimerLPPrefix() []byte { return []byte(timerLPPrefix) }

// TimerLPPrefixForLP returns timer_lp/<lp:4>, suitable as the LowerBound for
// a per-LP scan of every timer in one logical partition.
func TimerLPPrefixForLP(lp uint32) []byte {
	out := make([]byte, 0, len(timerLPPrefix)+LPLen)
	out = append(out, timerLPPrefix...)
	return appendLP(out, lp)
}

// DecodeTimerLPKey extracts (lp, fireAtMs, invocation_id) from a timer_lp key.
func DecodeTimerLPKey(key []byte) (uint32, uint64, *enginev1.InvocationId, error) {
	want := len(timerLPPrefix) + LPLen + 8 + invocationIDLen
	if len(key) != want {
		return 0, 0, nil, fmt.Errorf("timer_lp key length = %d; want %d", len(key), want)
	}
	p := len(timerLPPrefix)
	lp := binary.BigEndian.Uint32(key[p : p+LPLen])
	p += LPLen
	fireAt := binary.BigEndian.Uint64(key[p : p+8])
	id, err := DecodeInvocationID(key[p+8:])
	return lp, fireAt, id, err
}

// TimerIdxKey returns timer_idx/<lp:4><24-byte id>/<8-byte BE fire_at>. The
// secondary index lets onPurge find every pending timer for an invocation
// with a single bounded range scan instead of walking the whole timer/
// namespace.
func TimerIdxKey(id *enginev1.InvocationId, fireAtMs uint64) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(timerIdxPrefix)+LPLen+invocationIDLen+8)
	out = append(out, timerIdxPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	var fireBuf [8]byte
	binary.BigEndian.PutUint64(fireBuf[:], fireAtMs)
	return append(out, fireBuf[:]...), nil
}

// TimerIdxPrefix returns the timer_idx/ namespace prefix, suitable for a
// range scan over every secondary-index row in the partition.
func TimerIdxPrefix() []byte { return []byte(timerIdxPrefix) }

// TimerIdxPrefixForID returns timer_idx/<lp:4><24-byte id>/, suitable for
// a range scan over every secondary-index row for one invocation.
func TimerIdxPrefixForID(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(timerIdxPrefix)+LPLen+invocationIDLen)
	out = append(out, timerIdxPrefix...)
	out = appendLP(out, lp)
	return append(out, raw...), nil
}

// DecodeTimerIdxKey extracts (invocation_id, fireAtMs) from a secondary-
// index key.
func DecodeTimerIdxKey(key []byte) (*enginev1.InvocationId, uint64, error) {
	want := len(timerIdxPrefix) + LPLen + invocationIDLen + 8
	if len(key) != want {
		return nil, 0, fmt.Errorf("timer_idx key length = %d; want %d", len(key), want)
	}
	p := len(timerIdxPrefix) + LPLen
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
//
// Shard-scoped (no LP prefix): dedup state belongs to the shard's leader
// epoch and does not move with a logical partition.
func DedupSelfKey(leaderEpoch, seq uint64) []byte {
	out := make([]byte, 0, len(dedupSelfPrefix)+16)
	out = append(out, dedupSelfPrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], leaderEpoch)
	out = append(out, buf[:]...)
	binary.BigEndian.PutUint64(buf[:], seq)
	return append(out, buf[:]...)
}

// DedupArbitraryKey returns dedup/arbitrary/<lp:4><producer_id>/<8-byte BE seq>.
// Exact-match keying for the same reason as DedupSelfKey — concurrent
// producers (e.g. loadgen goroutines) allocate seq from a shared atomic
// counter and submit out-of-order to dragonboat.
//
// LP-prefixed: arbitrary dedup state belongs to a logical partition and rides
// the LP-transfer scan, so a producer retry that hits the LP's new owner
// after a transfer flip finds its dedup row already present. The caller
// derives lp from the command kind via lpFromCommand in partition.go; for
// the few LP-agnostic kinds (OutboxAck) pass LPNoLP.
func DedupArbitraryKey(lp uint32, producerID string, seq uint64) []byte {
	out := make([]byte, 0, len(dedupArbPrefix)+LPLen+len(producerID)+1+8)
	out = append(out, dedupArbPrefix...)
	out = appendLP(out, lp)
	out = append(out, producerID...)
	out = append(out, '/')
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], seq)
	return append(out, buf[:]...)
}

// DedupArbitraryLPPrefix returns dedup/arbitrary/<lp:4>, suitable as the
// LowerBound for a per-LP scan of every arbitrary dedup row in one logical
// partition. Pair with PrefixUpperBound for the UpperBound. Used by the LP
// transfer scanner to ship the LP's dedup state to the destination shard,
// and by the partition apply path's FinishLPTransfer to range-delete the
// rows on the source side.
func DedupArbitraryLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(dedupArbPrefix)+LPLen)
	out = append(out, dedupArbPrefix...)
	return appendLP(out, lp)
}

// StatePrefix returns the state/ namespace prefix. Exported so other
// packages can avoid colliding with it.
func StatePrefix() []byte { return []byte(statePrefix) }

// StateKey returns state/<lp:4><service>/<obj_key>/<state_key>. For unkeyed
// services pass objectKey="". Callers must ensure none of the three string
// components contain '/', otherwise the namespace boundary is ambiguous —
// the API surface in pkg/handler rejects invalid keys before they reach here.
func StateKey(lp uint32, service, objectKey, stateKey string) []byte {
	out := make([]byte, 0, len(statePrefix)+LPLen+len(service)+1+len(objectKey)+1+len(stateKey))
	out = append(out, statePrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, objectKey...)
	out = append(out, '/')
	return append(out, stateKey...)
}

// StatePrefixForObject returns state/<lp:4><service>/<obj_key>/, suitable as
// the LowerBound for a per-object scan paired with PrefixUpperBound for the
// matching UpperBound.
func StatePrefixForObject(lp uint32, service, objectKey string) []byte {
	out := make([]byte, 0, len(statePrefix)+LPLen+len(service)+1+len(objectKey)+1)
	out = append(out, statePrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, objectKey...)
	return append(out, '/')
}

// OutboxPrefix returns the outbox/ namespace prefix.
func OutboxPrefix() []byte { return []byte(outboxPrefix) }

// OutboxKey returns outbox/<8-byte BE seq>. Big-endian guarantees
// lexicographic byte order matches numeric order, so a forward scan from
// OutboxPrefix yields pending envelopes in FIFO insertion order. LP-agnostic:
// the leader's shuffler drains in seq order, and the future LP transfer
// extracts outbox rows via full-namespace scan (outbox is normally small).
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

// AwakeableKey returns awakeable/<lp:4><26-byte id>. The caller is responsible
// for validating the id via ValidateAwakeableID before constructing the key;
// passing a malformed id here produces a syntactically valid key but risks
// collision with future namespace extensions. The lp must be derived from
// the awakeable id's owner partition key — callers compose
// `LPFromPartitionKey(AwakeableOwnerPartitionKey(id))`.
func AwakeableKey(lp uint32, id string) []byte {
	out := make([]byte, 0, len(awakeablePrefix)+LPLen+len(id))
	out = append(out, awakeablePrefix...)
	out = appendLP(out, lp)
	return append(out, id...)
}

// KeyLeaseKey returns keylease/<lp:4><service>/<obj_key>. For unkeyed targets
// callers must skip this namespace entirely; the VO gate is only consulted
// for keyed invocations.
func KeyLeaseKey(lp uint32, service, objectKey string) []byte {
	out := make([]byte, 0, len(keyLeasePrefix)+LPLen+len(service)+1+len(objectKey))
	out = append(out, keyLeasePrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	return append(out, objectKey...)
}

// IdempotencyKey returns idempotency/<lp:4><sha256(tuple)>. The tuple is the
// caller-supplied (service, handler, object_key, idempotency_key) hashed
// with length-prefixed components so adjacent fields never alias. Used by
// the onInvoke dedup path: a hit means an invocation with the same tuple
// was already accepted; the stored value is the prior InvocationId.
//
// The lp is derived from (service, object_key) at the caller site — it
// must match the LP that future writers would compute for the same tuple,
// otherwise point-Get misses the dedup row.
func IdempotencyKey(lp uint32, service, handler, objectKey, idempotencyKey string) []byte {
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
	out := make([]byte, 0, len(idempPrefix)+LPLen+len(sum))
	out = append(out, idempPrefix...)
	out = appendLP(out, lp)
	return append(out, sum...)
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

// SignalInboxKey returns signal_inbox/<lp:4><inv_id>/<name>. The name is a
// user-supplied UTF-8 string; callers must reject names containing the
// "/" delimiter at the SDK boundary so prefix scans on
// signal_inbox/<lp:4><inv_id>/ stay unambiguous.
func SignalInboxKey(id *enginev1.InvocationId, name string) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(signalInboxPrefix)+LPLen+invocationIDLen+1+len(name))
	out = append(out, signalInboxPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	out = append(out, '/')
	return append(out, name...), nil
}

// SignalInboxPrefixForInvocation returns signal_inbox/<lp:4><inv_id>/, used
// with PrefixUpperBound for range-delete on Purge.
func SignalInboxPrefixForInvocation(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(signalInboxPrefix)+LPLen+invocationIDLen+1)
	out = append(out, signalInboxPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	return append(out, '/'), nil
}

// SignalAwaiterKey returns signal_awaiter/<lp:4><inv_id>/<name>. Same shape
// as SignalInboxKey.
func SignalAwaiterKey(id *enginev1.InvocationId, name string) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(signalAwaiterPrefix)+LPLen+invocationIDLen+1+len(name))
	out = append(out, signalAwaiterPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	out = append(out, '/')
	return append(out, name...), nil
}

// SignalAwaiterPrefixForInvocation returns signal_awaiter/<lp:4><inv_id>/.
func SignalAwaiterPrefixForInvocation(id *enginev1.InvocationId) ([]byte, error) {
	raw, err := EncodeInvocationID(id)
	if err != nil {
		return nil, err
	}
	lp := LPFromPartitionKey(id.GetPartitionKey())
	out := make([]byte, 0, len(signalAwaiterPrefix)+LPLen+invocationIDLen+1)
	out = append(out, signalAwaiterPrefix...)
	out = appendLP(out, lp)
	out = append(out, raw...)
	return append(out, '/'), nil
}

// WorkflowRunKey returns workflow_run/<lp:4><service>/<workflow_key>. Used by
// the apply path to dedup repeated SubmitInvocation requests for a
// KIND_WORKFLOW Run handler — the value is the InvocationId of the
// currently-active or most-recently-completed run for that (service, key).
func WorkflowRunKey(lp uint32, service, workflowKey string) []byte {
	out := make([]byte, 0, len(workflowRunPrefix)+LPLen+len(service)+1+len(workflowKey))
	out = append(out, workflowRunPrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	return append(out, workflowKey...)
}

// WorkflowRunPrefix returns the workflow_run/ namespace prefix. Used by
// the workflow retention reaper to scan completed runs.
func WorkflowRunPrefix() []byte { return []byte(workflowRunPrefix) }

// PromiseKey returns promise/<lp:4><service>/<workflow_key>/<name>. Named
// durable promises are scoped to a workflow run; the key encodes that
// scope directly so per-workflow range scans are bounded.
func PromiseKey(lp uint32, service, workflowKey, name string) []byte {
	out := make([]byte, 0, len(promisePrefix)+LPLen+len(service)+1+len(workflowKey)+1+len(name))
	out = append(out, promisePrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, workflowKey...)
	out = append(out, '/')
	return append(out, name...)
}

// PromisePrefixForWorkflow returns promise/<lp:4><service>/<workflow_key>/,
// suitable as the LowerBound for a per-workflow scan. Used by the
// retention reaper.
func PromisePrefixForWorkflow(lp uint32, service, workflowKey string) []byte {
	out := make([]byte, 0, len(promisePrefix)+LPLen+len(service)+1+len(workflowKey)+1)
	out = append(out, promisePrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, workflowKey...)
	return append(out, '/')
}

// PromiseAwaiterKey returns
// promise_awaiter/<lp:4><service>/<workflow_key>/<name>/<4-byte BE entry_index>.
// The entry_index suffix lets multiple co-pending Promise(name).Result()
// calls from distinct invocations each get their own row; resolution
// prefix-scans (service, key, name) and stitches every awaiter in the same
// batch.
func PromiseAwaiterKey(lp uint32, service, workflowKey, name string, entryIndex uint32) []byte {
	out := make([]byte, 0, len(promiseAwaiterPrefix)+LPLen+len(service)+1+len(workflowKey)+1+len(name)+1+4)
	out = append(out, promiseAwaiterPrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, workflowKey...)
	out = append(out, '/')
	out = append(out, name...)
	out = append(out, '/')
	var idxBuf [4]byte
	binary.BigEndian.PutUint32(idxBuf[:], entryIndex)
	return append(out, idxBuf[:]...)
}

// PromiseAwaiterPrefixForName returns
// promise_awaiter/<lp:4><service>/<workflow_key>/<name>/, suitable as the
// LowerBound for a per-name scan of every awaiter slot.
func PromiseAwaiterPrefixForName(lp uint32, service, workflowKey, name string) []byte {
	out := make([]byte, 0, len(promiseAwaiterPrefix)+LPLen+len(service)+1+len(workflowKey)+1+len(name)+1)
	out = append(out, promiseAwaiterPrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, workflowKey...)
	out = append(out, '/')
	out = append(out, name...)
	return append(out, '/')
}

// PromiseAwaiterPrefixForWorkflow returns
// promise_awaiter/<lp:4><service>/<workflow_key>/.
func PromiseAwaiterPrefixForWorkflow(lp uint32, service, workflowKey string) []byte {
	out := make([]byte, 0, len(promiseAwaiterPrefix)+LPLen+len(service)+1+len(workflowKey)+1)
	out = append(out, promiseAwaiterPrefix...)
	out = appendLP(out, lp)
	out = append(out, service...)
	out = append(out, '/')
	out = append(out, workflowKey...)
	return append(out, '/')
}

// WorkflowReapKey returns
// workflow_reap/<8-byte BE fire_at_ms>/<service>/<workflow_key>. The
// fire_at_ms prefix gives lexicographic ordering matching numeric order
// so the leader's reap service drains in due-order (same shape as
// timer/<fire_at_ms>/...). LP-agnostic (ordering-sensitive, see TimerKey).
func WorkflowReapKey(fireAtMs uint64, service, workflowKey string) []byte {
	out := make([]byte, 0, len(workflowReapPrefix)+8+len(service)+1+len(workflowKey))
	out = append(out, workflowReapPrefix...)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], fireAtMs)
	out = append(out, buf[:]...)
	out = append(out, service...)
	out = append(out, '/')
	return append(out, workflowKey...)
}

// WorkflowReapPrefix returns the workflow_reap/ namespace prefix.
func WorkflowReapPrefix() []byte { return []byte(workflowReapPrefix) }

// DecodeWorkflowReapKey extracts (fireAtMs, service, workflow_key) from
// a workflow_reap key. Returns an error if the key shape doesn't match.
func DecodeWorkflowReapKey(key []byte) (uint64, string, string, error) {
	minLen := len(workflowReapPrefix) + 8 + 1 // at minimum: prefix + fire + '/'
	if len(key) < minLen {
		return 0, "", "", fmt.Errorf("workflow_reap key too short: len=%d", len(key))
	}
	body := key[len(workflowReapPrefix):]
	fireAt := binary.BigEndian.Uint64(body[:8])
	rest := body[8:]
	sep := -1
	for i, c := range rest {
		if c == '/' {
			sep = i
			break
		}
	}
	if sep < 0 {
		return 0, "", "", fmt.Errorf("workflow_reap key missing service/key separator")
	}
	return fireAt, string(rest[:sep]), string(rest[sep+1:]), nil
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

// JournalLPPrefix returns journal/<lp:4>, suitable as the LowerBound for a
// per-LP scan of every journal entry in one logical partition. Pair with
// PrefixUpperBound for the UpperBound.
func JournalLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(journalPrefix)+LPLen)
	out = append(out, journalPrefix...)
	return appendLP(out, lp)
}

// StateLPPrefix returns state/<lp:4>, suitable as the LowerBound for a
// per-LP scan of every state entry in one logical partition.
func StateLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(statePrefix)+LPLen)
	out = append(out, statePrefix...)
	return appendLP(out, lp)
}

// AwakeableLPPrefix returns awakeable/<lp:4>.
func AwakeableLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(awakeablePrefix)+LPLen)
	out = append(out, awakeablePrefix...)
	return appendLP(out, lp)
}

// KeyLeaseLPPrefix returns keylease/<lp:4>.
func KeyLeaseLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(keyLeasePrefix)+LPLen)
	out = append(out, keyLeasePrefix...)
	return appendLP(out, lp)
}

// IdempotencyLPPrefix returns idempotency/<lp:4>.
func IdempotencyLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(idempPrefix)+LPLen)
	out = append(out, idempPrefix...)
	return appendLP(out, lp)
}

// SignalInboxLPPrefix returns signal_inbox/<lp:4>.
func SignalInboxLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(signalInboxPrefix)+LPLen)
	out = append(out, signalInboxPrefix...)
	return appendLP(out, lp)
}

// SignalAwaiterLPPrefix returns signal_awaiter/<lp:4>.
func SignalAwaiterLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(signalAwaiterPrefix)+LPLen)
	out = append(out, signalAwaiterPrefix...)
	return appendLP(out, lp)
}

// WorkflowRunLPPrefix returns workflow_run/<lp:4>.
func WorkflowRunLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(workflowRunPrefix)+LPLen)
	out = append(out, workflowRunPrefix...)
	return appendLP(out, lp)
}

// PromiseLPPrefix returns promise/<lp:4>.
func PromiseLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(promisePrefix)+LPLen)
	out = append(out, promisePrefix...)
	return appendLP(out, lp)
}

// PromiseAwaiterLPPrefix returns promise_awaiter/<lp:4>.
func PromiseAwaiterLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(promiseAwaiterPrefix)+LPLen)
	out = append(out, promiseAwaiterPrefix...)
	return appendLP(out, lp)
}

// TimerIdxLPPrefix returns timer_idx/<lp:4>.
func TimerIdxLPPrefix(lp uint32) []byte {
	out := make([]byte, 0, len(timerIdxPrefix)+LPLen)
	out = append(out, timerIdxPrefix...)
	return appendLP(out, lp)
}

// LPFreezeKey returns lp_freeze/<lp:4>. The LPFreezeTable is a
// per-partition control-plane namespace populated by BeginLPTransfer's
// apply arm; the partition apply path reads it on every LP-touching
// command via the freeze gate. NOT in the LP-prefixed set: the row is
// metadata ABOUT the transfer, not state belonging to the LP, and
// never migrates with the LP.
func LPFreezeKey(lp uint32) []byte {
	out := make([]byte, 0, len(lpFreezePrefix)+LPLen)
	out = append(out, lpFreezePrefix...)
	return appendLP(out, lp)
}

// LPFreezePrefix returns the lp_freeze/ namespace prefix, used by
// LPTransferSourceService.Rebuild to enumerate frozen LPs on leader
// gain.
func LPFreezePrefix() []byte { return []byte(lpFreezePrefix) }

// LPStagingKey returns lp_staging/<transfer_id>. The LPStagingTable is
// a per-destination-partition control-plane namespace used by
// ApplyLPTransferSST to enforce in-order SST delivery and absorb
// retries. Dropped by CommitLPTransfer / AbortLPTransfer.
func LPStagingKey(transferID string) []byte {
	out := make([]byte, 0, len(lpStagingPrefix)+len(transferID))
	out = append(out, lpStagingPrefix...)
	return append(out, transferID...)
}

// LPStagingPrefix returns the lp_staging/ namespace prefix, used by
// orphan-cleanup at partition open to enumerate live destination
// transfers.
func LPStagingPrefix() []byte { return []byte(lpStagingPrefix) }

func init() {
	if LPCount&(LPCount-1) != 0 {
		panic("keys: LPCount must be a power of two")
	}
}
