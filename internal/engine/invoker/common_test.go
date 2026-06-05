package invoker

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/internal/storage/tables"
	"github.com/twinfer/reflw/pkg/handler/wire"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
	protocolv1 "github.com/twinfer/reflw/proto/protocolv1"
)

// TestPreloadEagerState_OverflowKeepsPartialCache verifies that when
// the scan trips the configured cap mid-way, the returned cache holds
// the rows that fit before the overflow rather than being discarded.
// Pre-lazy-fetch the function returned (nil, true); the SDK had no
// recourse so a single huge row poisoned the whole snapshot. With lazy
// fetch wired, the partial cache lets cheap keys stay cheap.
func TestPreloadEagerState_OverflowKeepsPartialCache(t *testing.T) {
	store := storage.NewMemStore()
	st := tables.StateTable{S: store}
	target := &enginev1.InvocationTarget{ServiceName: "Svc", ObjectKey: "obj-1"}
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789ABCDEF")}

	// Write 4 rows × 25 KiB each. Total = 100 KiB, well past the 64 KiB
	// cap. Keys are alphabetically ordered so the scan visits them in
	// (k0, k1, k2, k3) order; the cap should trip on k2 or k3.
	big := strings.Repeat("x", 25*1024)
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	batch := store.NewBatch()
	for i := range 4 {
		key := fmt.Sprintf("k%d", i)
		if err := st.Set(batch, lp, target, key, []byte(big)); err != nil {
			t.Fatalf("StateTable.Set(%s): %v", key, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	cache, overflowed := preloadEagerState(st, target, id, DefaultEagerStateMaxBytes, discardLogger())
	if !overflowed {
		t.Fatalf("overflowed=false; want true (100 KiB across 4 rows > 64 KiB cap)")
	}
	if cache == nil {
		t.Fatal("cache=nil; want partial cache retained on overflow")
	}
	if len(cache) == 0 {
		t.Errorf("cache empty; want partial cache with keys that fit before overflow")
	}
	if len(cache) >= 4 {
		t.Errorf("cache has %d entries; want fewer than 4 (overflow should have stopped the scan)", len(cache))
	}
	// Every cached entry must round-trip its value verbatim — the fix
	// preserves what already fit, it doesn't truncate values.
	for k, v := range cache {
		if string(v) != big {
			t.Errorf("cache[%s] value len=%d; want %d (verbatim)", k, len(v), len(big))
		}
	}
}

// TestPreloadEagerState_NoOverflowFullSnapshot is the non-overflow
// baseline: small state fits comfortably; cache holds every row and
// overflowed is false.
func TestPreloadEagerState_NoOverflowFullSnapshot(t *testing.T) {
	store := storage.NewMemStore()
	st := tables.StateTable{S: store}
	target := &enginev1.InvocationTarget{ServiceName: "Svc", ObjectKey: "obj-2"}
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789ABCDEF")}

	want := map[string][]byte{
		"alpha": []byte("1"),
		"beta":  []byte("2"),
		"gamma": []byte("3"),
	}
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	batch := store.NewBatch()
	for k, v := range want {
		if err := st.Set(batch, lp, target, k, v); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	cache, overflowed := preloadEagerState(st, target, id, DefaultEagerStateMaxBytes, discardLogger())
	if overflowed {
		t.Errorf("overflowed=true; want false (3 tiny rows are well under the cap)")
	}
	if got, want := len(cache), len(want); got != want {
		t.Errorf("cache size = %d; want %d", got, want)
	}
	for k, v := range want {
		got, ok := cache[k]
		if !ok {
			t.Errorf("cache[%s] missing; want present", k)
			continue
		}
		if string(got) != string(v) {
			t.Errorf("cache[%s] = %q; want %q", k, got, v)
		}
	}
}

// TestPreloadEagerState_CustomCapHonored verifies the operator knob:
// passing a non-zero maxBytes overrides DefaultEagerStateMaxBytes. Two
// rows × 3 KiB total 6 KiB and fit comfortably under 64 KiB, but blow
// a 4 KiB cap; the first row fits, the second trips overflow.
func TestPreloadEagerState_CustomCapHonored(t *testing.T) {
	store := storage.NewMemStore()
	st := tables.StateTable{S: store}
	target := &enginev1.InvocationTarget{ServiceName: "Svc", ObjectKey: "obj-cap"}
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789ABCDEF")}

	row := strings.Repeat("y", 3*1024)
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	batch := store.NewBatch()
	for _, k := range []string{"a", "b"} {
		if err := st.Set(batch, lp, target, k, []byte(row)); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	cache, overflowed := preloadEagerState(st, target, id, 4*1024, discardLogger())
	if !overflowed {
		t.Fatalf("overflowed=false with 4 KiB cap on 6 KiB total; want true")
	}
	if len(cache) != 1 {
		t.Errorf("cache size = %d; want 1 (first row fits at 3 KiB; second trips the cap)", len(cache))
	}
}

// TestPreloadEagerState_ZeroCapUsesDefault verifies that maxBytes=0 falls
// back to DefaultEagerStateMaxBytes — the contract the HostConfig/Invoker
// plumbing relies on so operators don't have to repeat the default
// everywhere.
func TestPreloadEagerState_ZeroCapUsesDefault(t *testing.T) {
	store := storage.NewMemStore()
	st := tables.StateTable{S: store}
	target := &enginev1.InvocationTarget{ServiceName: "Svc", ObjectKey: "obj-default"}
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789ABCDEF")}

	// 4 × 25 KiB = 100 KiB. Default cap is 64 KiB → overflow.
	big := strings.Repeat("x", 25*1024)
	lp := keys.LPFromPartitionKey(routing.PartitionKey(target.GetServiceName(), target.GetObjectKey()))
	batch := store.NewBatch()
	for i := range 4 {
		if err := st.Set(batch, lp, target, fmt.Sprintf("k%d", i), []byte(big)); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if err := batch.Commit(false); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	cache, overflowed := preloadEagerState(st, target, id, 0, discardLogger())
	if !overflowed {
		t.Fatalf("overflowed=false with default cap; want true (100 KiB > 64 KiB default)")
	}
	if len(cache) >= 4 {
		t.Errorf("cache size = %d; want fewer than 4 (default cap should have tripped)", len(cache))
	}
}

// TestPreloadEagerState_UnkeyedReturnsNil covers the unkeyed-service
// fast path: no object key means no per-object state, so the function
// returns (nil, false) without scanning.
func TestPreloadEagerState_UnkeyedReturnsNil(t *testing.T) {
	store := storage.NewMemStore()
	st := tables.StateTable{S: store}
	target := &enginev1.InvocationTarget{ServiceName: "Svc"} // ObjectKey empty
	id := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789ABCDEF")}

	cache, overflowed := preloadEagerState(st, target, id, DefaultEagerStateMaxBytes, discardLogger())
	if cache != nil {
		t.Errorf("cache = %v; want nil for unkeyed service", cache)
	}
	if overflowed {
		t.Errorf("overflowed = true; want false for unkeyed service")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestTranslateEntry_GetEagerStateKeys verifies the JEGetEagerStateKeys
// replay path: the journal entry's keys list is rendered as a single
// GetEagerStateKeysCommandMessage frame at the entry's own slot. No
// completion notification — eager is single-slot.
func TestTranslateEntry_GetEagerStateKeys(t *testing.T) {
	codec := wire.DefaultCodec()
	invID := &enginev1.InvocationId{PartitionKey: 1, Uuid: []byte("0123456789ABCDEF")}
	entry := &enginev1.JournalEntry{
		Index: 1,
		Entry: &enginev1.JournalEntry_GetEagerStateKeys{
			GetEagerStateKeys: &enginev1.JEGetEagerStateKeys{
				Keys: []string{"alpha", "beta", "charlie"},
			},
		},
	}
	frames, err := translateEntry(invID, entry, codec, discardLogger())
	if err != nil {
		t.Fatalf("translateEntry: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames = %d; want 1 (eager is single-slot)", len(frames))
	}
	if frames[0].typeCode != wire.TypeCmdGetEagerStateKeys {
		t.Errorf("typeCode = 0x%04x; want TypeCmdGetEagerStateKeys", frames[0].typeCode)
	}
	if frames[0].slot != 1 {
		t.Errorf("slot = %d; want 1", frames[0].slot)
	}
	var cmd protocolv1.GetEagerStateKeysCommandMessage
	if err := codec.Unmarshal(frames[0].payload, &cmd); err != nil {
		t.Fatalf("decode GetEagerStateKeysCommandMessage: %v", err)
	}
	got := cmd.GetValue().GetKeys()
	if len(got) != 3 || string(got[0]) != "alpha" || string(got[1]) != "beta" || string(got[2]) != "charlie" {
		t.Errorf("keys = %v; want [alpha beta charlie]", got)
	}
}
