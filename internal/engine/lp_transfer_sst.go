package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/keys"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// sstNsTimerPrimary is the on-disk filename for the LP-agnostic
// timer/<fire>/<id> rows shipped via the timer_lp secondary scan.
// The LP-prefixed namespaces' filenames come from
// keys.AllLPNamespaces, the single source of truth for the LP-scope
// walker; timer_primary is the one exception because it's derived
// from timer_lp rather than directly LP-prefixed.
//
// Wire-visible: the destination resolves TransferSSTRef.relative_path
// against its staging dir, so this name MUST NOT change.
const sstNsTimerPrimary = "timer_primary"

// buildLPSSTs scans the LP's data on the source store and writes one
// SST per non-empty LP-prefixed namespace plus one SST for the LP's
// primary timer rows (extracted via the timer_lp walk). All SSTs are
// written to outDir using sstable.WriterOptions sourced from the
// store, so the destination — running the same reflow binary — can
// Ingest them without format conversion.
//
// Empty namespaces are skipped (no file written, no TransferSSTRef
// returned). The destination must tolerate any subset of the 15
// possible namespaces.
//
// Returned refs are in deterministic namespace order so the upload +
// propose path stamps the same byte sequence across retries (the
// proto's repeated field is order-preserving, and dedup is stable
// only if the input is stable).
func buildLPSSTs(
	ctx context.Context,
	pstore *storage.PebbleStore,
	lp uint32,
	outDir string,
) ([]*enginev1.TransferSSTRef, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("buildLPSSTs: mkdir %s: %w", outDir, err)
	}

	var out []*enginev1.TransferSSTRef
	for _, ns := range keys.AllLPNamespaces {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ref, err := buildNamespaceSST(pstore, ns.Name, ns.Prefix(lp), outDir)
		if err != nil {
			return nil, err
		}
		if ref != nil {
			out = append(out, ref)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timerRef, err := buildTimerPrimarySST(pstore, lp, outDir)
	if err != nil {
		return nil, err
	}
	if timerRef != nil {
		out = append(out, timerRef)
	}
	return out, nil
}

// buildNamespaceSST writes one SST for the LP-prefixed slice rooted
// at prefix. Returns (nil, nil) when the namespace is empty for this
// LP — no file is created and the destination's Ingest skips this
// shard entirely.
func buildNamespaceSST(
	pstore *storage.PebbleStore,
	name string,
	prefix []byte,
	outDir string,
) (*enginev1.TransferSSTRef, error) {
	upper := keys.PrefixUpperBound(prefix)
	it, err := pstore.NewIter(prefix, upper)
	if err != nil {
		return nil, fmt.Errorf("buildLPSSTs: open iter for %s: %w", name, err)
	}
	defer it.Close()
	if !it.First() {
		if err := it.Error(); err != nil {
			return nil, fmt.Errorf("buildLPSSTs: iter %s: %w", name, err)
		}
		return nil, nil
	}

	path := filepath.Join(outDir, name+".sst")
	w, err := pstore.OpenSSTFile(path)
	if err != nil {
		return nil, fmt.Errorf("buildLPSSTs: open sst %s: %w", name, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = w.Close()
			_ = os.Remove(path)
		}
	}()

	var smallest, largest []byte
	for ok := true; ok; ok = it.Next() {
		k := it.Key()
		v := it.Value()
		if err := w.Set(k, v); err != nil {
			return nil, fmt.Errorf("buildLPSSTs: write %s: %w", name, err)
		}
		if smallest == nil {
			smallest = append([]byte(nil), k...)
		}
		largest = append(largest[:0], k...)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("buildLPSSTs: iter %s: %w", name, err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("buildLPSSTs: close sst %s: %w", name, err)
	}
	closed = true

	size, sum, err := fileSizeAndSHA256(path)
	if err != nil {
		return nil, fmt.Errorf("buildLPSSTs: hash sst %s: %w", name, err)
	}
	return &enginev1.TransferSSTRef{
		RelativePath:    name + ".sst",
		SizeBytes:       size,
		Sha256Hex:       sum,
		SmallestUserKey: smallest,
		LargestUserKey:  largest,
	}, nil
}

// buildTimerPrimarySST walks the LP's timer_lp index and writes one
// SST for the corresponding LP-agnostic timer/<fire>/<id> primary
// rows. The walk order of timer_lp is sorted by Pebble's byte
// comparer, which yields fire-at-then-id order — which equals the
// sort order of the timer/<fire>/<id> primary keys, so we can stream
// directly into the SST without buffering.
//
// Returns (nil, nil) when the LP has no timer_lp rows.
func buildTimerPrimarySST(
	pstore *storage.PebbleStore,
	lp uint32,
	outDir string,
) (*enginev1.TransferSSTRef, error) {
	lower := keys.TimerLPPrefixForLP(lp)
	upper := keys.PrefixUpperBound(lower)
	it, err := pstore.NewIter(lower, upper)
	if err != nil {
		return nil, fmt.Errorf("buildLPSSTs: open timer_lp iter: %w", err)
	}
	defer it.Close()
	if !it.First() {
		if err := it.Error(); err != nil {
			return nil, fmt.Errorf("buildLPSSTs: timer_lp iter: %w", err)
		}
		return nil, nil
	}

	path := filepath.Join(outDir, sstNsTimerPrimary+".sst")
	w, err := pstore.OpenSSTFile(path)
	if err != nil {
		return nil, fmt.Errorf("buildLPSSTs: open timer-primary sst: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = w.Close()
			_ = os.Remove(path)
		}
	}()

	var smallest, largest []byte
	var wrote bool
	for ok := true; ok; ok = it.Next() {
		_, fireAt, id, derr := keys.DecodeTimerLPKey(it.Key())
		if derr != nil {
			return nil, fmt.Errorf("buildLPSSTs: decode timer_lp key: %w", derr)
		}
		primary, perr := keys.TimerKey(fireAt, id)
		if perr != nil {
			return nil, fmt.Errorf("buildLPSSTs: encode timer key: %w", perr)
		}
		val, closer, gerr := pstore.Get(primary)
		if gerr != nil {
			if errors.Is(gerr, storage.ErrNotFound) {
				// orphan timer_lp row — log via the caller's logger
				// (callers wrap this; here we just continue).
				continue
			}
			return nil, fmt.Errorf("buildLPSSTs: get timer primary: %w", gerr)
		}
		v := append([]byte(nil), val...)
		closer.Close()
		if err := w.Set(primary, v); err != nil {
			return nil, fmt.Errorf("buildLPSSTs: write timer primary: %w", err)
		}
		wrote = true
		if smallest == nil {
			smallest = append([]byte(nil), primary...)
		}
		largest = append(largest[:0], primary...)
	}
	if err := it.Error(); err != nil {
		return nil, fmt.Errorf("buildLPSSTs: timer_lp iter: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("buildLPSSTs: close timer-primary sst: %w", err)
	}
	closed = true

	if !wrote {
		// timer_lp had rows but every primary lookup missed — drop the
		// empty file rather than ship a zero-row SST that Pebble's
		// Ingest would reject.
		_ = os.Remove(path)
		return nil, nil
	}

	size, sum, err := fileSizeAndSHA256(path)
	if err != nil {
		return nil, fmt.Errorf("buildLPSSTs: hash timer-primary sst: %w", err)
	}
	return &enginev1.TransferSSTRef{
		RelativePath:    sstNsTimerPrimary + ".sst",
		SizeBytes:       size,
		Sha256Hex:       sum,
		SmallestUserKey: smallest,
		LargestUserKey:  largest,
	}, nil
}

func fileSizeAndSHA256(path string) (uint64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return uint64(n), hex.EncodeToString(h.Sum(nil)), nil
}
