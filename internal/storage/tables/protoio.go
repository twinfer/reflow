package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
)

// isNotFound is the package-local alias for the storage absent sentinel —
// keeps the import noise in each table down to one symbol.
func isNotFound(err error) bool { return errors.Is(err, storage.ErrNotFound) }

// getProto reads key from r, unmarshals into msg, and returns the
// storage error verbatim (so callers can branch on
// storage.ErrNotFound to apply per-table "absent" conventions). On
// success msg is populated in place and err is nil. r is any
// storage.Reader — typically a Store (general reads) or a Batch
// (read-your-writes within partition.go's apply loop).
func getProto(r storage.Reader, key []byte, msg proto.Message) error {
	val, closer, err := r.Get(key)
	if err != nil {
		return err
	}
	defer closer.Close()
	return proto.Unmarshal(val, msg)
}

// putProto marshals msg under key into the batch.
func putProto(b storage.Batch, key []byte, msg proto.Message) error {
	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return b.Set(key, buf)
}
