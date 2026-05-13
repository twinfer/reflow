package tables

import (
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflow/internal/storage"
)

// isNotFound is the package-local alias for the storage absent sentinel —
// keeps the import noise in each table down to one symbol.
func isNotFound(err error) bool { return errors.Is(err, storage.ErrNotFound) }

// getProto reads key from s, unmarshals into msg, and returns the
// storage.Store error verbatim (so callers can branch on
// storage.ErrNotFound to apply per-table "absent" conventions). On
// success msg is populated in place and err is nil.
func getProto(s storage.Store, key []byte, msg proto.Message) error {
	val, closer, err := s.Get(key)
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
