package invoker

import (
	"github.com/twinfer/reflw/internal/storage/tables"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// JournalReader is a thin wrapper over tables.JournalTable.Scan that
// materialises entries in index order for replay. Mirrors restate's
// InvocationReader (service_protocol_runner_v4.rs:60-65).
//
// The reader is stateless aside from the underlying storage handle.
// Sessions hold their own per-invocation snapshot; concurrent Load calls
// for different invocations share the same reader safely.
type JournalReader struct {
	table tables.JournalTable
}

// NewJournalReader constructs a reader backed by the given table.
func NewJournalReader(t tables.JournalTable) *JournalReader {
	return &JournalReader{table: t}
}

// Rebind swaps the underlying storage handle. Used by the partition
// runner after snapshot recovery, where the Pebble DB is replaced
// in-place.
func (r *JournalReader) Rebind(t tables.JournalTable) {
	r.table = t
}

// Load returns every journal entry for id in ascending index order. An
// empty journal returns ([]*JournalEntry{}, nil) — distinct from an error,
// which signals storage corruption.
func (r *JournalReader) Load(id *enginev1.InvocationId) ([]*enginev1.JournalEntry, error) {
	var entries []*enginev1.JournalEntry
	if err := r.table.Scan(id, func(e *enginev1.JournalEntry) error {
		entries = append(entries, e)
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}
