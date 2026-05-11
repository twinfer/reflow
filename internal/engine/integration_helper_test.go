package engine_test

import (
	"github.com/twinfer/reflow/internal/storage"
	"github.com/twinfer/reflow/internal/storage/tables"
)

// journalTableFor returns a JournalTable bound to the given Store. Lives in
// a separate file so it can be shared across integration tests without
// pulling internal types into the public surface.
func journalTableFor(s storage.Store) tables.JournalTable {
	return tables.JournalTable{S: s}
}
