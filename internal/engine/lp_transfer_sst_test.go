package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	enginev1 "github.com/twinfer/reflow/proto/enginev1"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
)

// TestBuildLPSSTs_EmptyLP confirms that an LP with no rows produces
// zero SSTs (every namespace iter is empty, and timer_lp is empty so
// the primary SST is also skipped).
func TestBuildLPSSTs_EmptyLP(t *testing.T) {
	src := openTempPebble(t)
	outDir := filepath.Join(t.TempDir(), "lpstage_out", "txn-empty")

	refs, err := buildLPSSTs(context.Background(), src, 7, outDir)
	if err != nil {
		t.Fatalf("buildLPSSTs: %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("expected zero SSTs for empty LP, got %d", len(refs))
	}
	entries, _ := os.ReadDir(outDir)
	for _, e := range entries {
		t.Fatalf("expected no SST files but found %s", e.Name())
	}
}

// TestBuildLPSSTs_PopulatedLP seeds rows across three LP-prefixed
// namespaces + one timer pair, builds SSTs, and ingests them into a
// fresh store. Round-trip verifies the bytes survive Set → SST file →
// Ingest → Get.
func TestBuildLPSSTs_PopulatedLP(t *testing.T) {
	src := openTempPebble(t)
	dst := openTempPebble(t)

	const lp = 7

	// Seed three LP-prefixed namespaces.
	type seedKV struct{ k, v []byte }
	seeds := []seedKV{
		{appendBytes(keys.InvocationLPPrefix(lp), []byte("inv-a")), []byte("inv-value-a")},
		{appendBytes(keys.InvocationLPPrefix(lp), []byte("inv-b")), []byte("inv-value-b")},
		{appendBytes(keys.JournalLPPrefix(lp), []byte("jour-1")), []byte("journal-1")},
		{appendBytes(keys.StateLPPrefix(lp), []byte("state-x")), []byte("state-val-x")},
	}
	b := src.NewBatch()
	for _, kv := range seeds {
		if err := b.Set(kv.k, kv.v); err != nil {
			t.Fatal(err)
		}
	}

	// Seed a timer pair: one primary `timer/<fire>/<id>` and its matching
	// `timer_lp/<lp>/<fire>/<id>` secondary.
	id := &enginev1.InvocationId{PartitionKey: 42, Uuid: bytes.Repeat([]byte{0xAB}, 16)}
	pk, err := keys.TimerKey(1700000000123, id)
	if err != nil {
		t.Fatal(err)
	}
	lpk, err := keys.TimerLPKey(lp, 1700000000123, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Set(pk, []byte("timer-payload")); err != nil {
		t.Fatal(err)
	}
	if err := b.Set(lpk, []byte{1}); err != nil {
		t.Fatal(err)
	}

	// Decoy: a row for a different LP must not leak into our SSTs.
	decoyK := appendBytes(keys.InvocationLPPrefix(8), []byte("decoy"))
	if err := b.Set(decoyK, []byte("nope")); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	b.Close()

	outDir := filepath.Join(t.TempDir(), "lpstage_out", "txn-pop")
	refs, err := buildLPSSTs(context.Background(), src, lp, outDir)
	if err != nil {
		t.Fatalf("buildLPSSTs: %v", err)
	}

	// Expect SSTs for: inv, journal, state, timer_lp, timer_primary.
	// (timer_idx + the other 10 namespaces are empty for this LP.)
	wantNames := []string{"inv.sst", "journal.sst", "state.sst", "timer_lp.sst", "timer_primary.sst"}
	gotNames := make([]string, 0, len(refs))
	for _, r := range refs {
		gotNames = append(gotNames, r.GetRelativePath())
	}
	slices.Sort(gotNames)
	slices.Sort(wantNames)
	if !slices.Equal(gotNames, wantNames) {
		t.Fatalf("SST set mismatch:\n  got  %v\n  want %v", gotNames, wantNames)
	}

	// Verify each ref's metadata: file exists, size + sha256 match.
	for _, r := range refs {
		path := filepath.Join(outDir, r.GetRelativePath())
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", r.GetRelativePath(), err)
		}
		h := sha256.New()
		n, err := io.Copy(h, f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if uint64(n) != r.GetSizeBytes() {
			t.Errorf("%s size mismatch: file=%d ref=%d", r.GetRelativePath(), n, r.GetSizeBytes())
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != r.GetSha256Hex() {
			t.Errorf("%s sha256 mismatch:\n  got  %s\n  want %s", r.GetRelativePath(), got, r.GetSha256Hex())
		}
		if r.GetSmallestUserKey() == nil || r.GetLargestUserKey() == nil {
			t.Errorf("%s missing key bounds", r.GetRelativePath())
		}
	}

	// Ingest into a fresh store and verify every seeded LP=7 key
	// round-trips. The decoy key (LP=8) must not appear.
	paths := make([]string, 0, len(refs))
	for _, r := range refs {
		paths = append(paths, filepath.Join(outDir, r.GetRelativePath()))
	}
	if err := dst.IngestSSTs(context.Background(), paths); err != nil {
		t.Fatalf("IngestSSTs: %v", err)
	}

	wantKeys := append([]seedKV(nil), seeds...)
	wantKeys = append(wantKeys, seedKV{pk, []byte("timer-payload")}, seedKV{lpk, []byte{1}})
	for _, kv := range wantKeys {
		got, closer, gerr := dst.Get(kv.k)
		if gerr != nil {
			t.Errorf("Get %x: %v", kv.k, gerr)
			continue
		}
		if !bytes.Equal(got, kv.v) {
			t.Errorf("value mismatch for %x: got %q want %q", kv.k, got, kv.v)
		}
		closer.Close()
	}

	// Decoy key (LP=8) must NOT be on the dest store.
	if _, _, gerr := dst.Get(decoyK); gerr == nil {
		t.Fatal("decoy LP=8 row leaked into the dest store")
	}
}

// TestBuildLPSSTs_RoundTripEachRegisteredNamespace is the per-namespace
// round-trip companion to keys.TestAllLPNamespacesCoversEveryPrefixBuilder.
// For each entry in keys.AllLPNamespaces, seed exactly one row inside
// that namespace's LP range, build SSTs, and assert the namespace's
// `<name>.sst` appears in the result with non-zero size. Catches:
//
//   - A registry entry whose Prefix function points at the wrong
//     namespace (the seeded row falls outside the scan range and the
//     SST is missing or empty).
//   - A registry entry whose Name doesn't match what buildLPSSTs
//     stamps onto TransferSSTRef.relative_path (paths don't line up).
//   - buildLPSSTs silently skipping an entry (no iteration, no SST).
//
// timer_lp is special-cased because buildTimerPrimarySST decodes its
// keys to derive the LP-agnostic timer/<fire>/<id> primary row — an
// arbitrary byte suffix would fail DecodeTimerLPKey and abort
// buildLPSSTs before the per-namespace assertion runs.
func TestBuildLPSSTs_RoundTripEachRegisteredNamespace(t *testing.T) {
	for _, ns := range keys.AllLPNamespaces {
		t.Run(ns.Name, func(t *testing.T) {
			src := openTempPebble(t)
			const lp uint32 = 7
			seedNamespaceRow(t, src, ns, lp)

			outDir := filepath.Join(t.TempDir(), "out")
			refs, err := buildLPSSTs(context.Background(), src, lp, outDir)
			if err != nil {
				t.Fatalf("buildLPSSTs: %v", err)
			}

			wantName := ns.Name + ".sst"
			var found *enginev1.TransferSSTRef
			for _, r := range refs {
				if r.GetRelativePath() == wantName {
					found = r
					break
				}
			}
			if found == nil {
				t.Fatalf("namespace %q seeded a row but its SST is missing from buildLPSSTs result; got refs %v — check that the registry entry's Prefix returns the right namespace and that buildLPSSTs iterates keys.AllLPNamespaces", ns.Name, refNames(refs))
			}
			if found.GetSizeBytes() == 0 {
				t.Fatalf("namespace %q SST has zero size — scan picked up no rows even though we seeded one", ns.Name)
			}
		})
	}
}

func seedNamespaceRow(t *testing.T, store *storage.PebbleStore, ns keys.LPNamespace, lp uint32) {
	t.Helper()
	b := store.NewBatch()
	defer b.Close()
	if ns.Name == "timer_lp" {
		// buildTimerPrimarySST decodes timer_lp keys, so the seed must
		// be a valid (lp, fire, id) row plus a matching primary timer
		// row keyed by the same (fire, id).
		id := &enginev1.InvocationId{PartitionKey: 42, Uuid: bytes.Repeat([]byte{0xAB}, 16)}
		const fireAt uint64 = 1_700_000_000_000
		lpk, err := keys.TimerLPKey(lp, fireAt, id)
		if err != nil {
			t.Fatalf("TimerLPKey: %v", err)
		}
		pk, err := keys.TimerKey(fireAt, id)
		if err != nil {
			t.Fatalf("TimerKey: %v", err)
		}
		if err := b.Set(lpk, []byte{1}); err != nil {
			t.Fatalf("set timer_lp: %v", err)
		}
		if err := b.Set(pk, []byte("timer-payload")); err != nil {
			t.Fatalf("set timer primary: %v", err)
		}
	} else {
		k := appendBytes(ns.Prefix(lp), []byte("row-suffix"))
		if err := b.Set(k, []byte("row-value")); err != nil {
			t.Fatalf("set %s: %v", ns.Name, err)
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatalf("commit %s seed: %v", ns.Name, err)
	}
}

func refNames(refs []*enginev1.TransferSSTRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.GetRelativePath())
	}
	return out
}

func openTempPebble(t *testing.T) *storage.PebbleStore {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "pebble")
	s, err := storage.OpenPebble(dir, nil)
	if err != nil {
		t.Fatalf("OpenPebble: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func appendBytes(prefix, suffix []byte) []byte {
	out := make([]byte, 0, len(prefix)+len(suffix))
	out = append(out, prefix...)
	return append(out, suffix...)
}
