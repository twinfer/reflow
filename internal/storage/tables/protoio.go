package tables

import (
	"bytes"
	"errors"

	"google.golang.org/protobuf/proto"

	"github.com/twinfer/reflw/internal/storage"
)

// scanStart positions iter for a forward scan, returning whether it landed on a
// valid key. With after == nil it starts at the first key; otherwise it resumes
// strictly past after (SeekGE then skip an exact match), so after is an
// exclusive cursor. Bounds clamp it: an after below the iterator's range scans
// from the start, one at/above the range yields nothing. Shared by the per-LP
// ScanLP cursors behind the ListInvocations / ListProcessInstances fan-out.
func scanStart(iter storage.Iter, after []byte) bool {
	if after == nil {
		return iter.First()
	}
	ok := iter.SeekGE(after)
	if ok && bytes.Equal(iter.Key(), after) {
		ok = iter.Next()
	}
	return ok
}

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
