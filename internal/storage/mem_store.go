package storage

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"sync"
)

// MemStore is an in-memory Store for unit tests. It implements every method of
// Store except Checkpoint, which always errors (memory cannot be checkpointed
// to disk — use PebbleStore with vfs.NewMem() for checkpoint tests).
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{data: make(map[string][]byte)}
}

// wipeCloser zeroes the slice it guards when Close is called. MemStore
// hands these out from Get so callers that use the returned bytes after
// Close (a use-after-free under Pebble) see zeroed data instead of the
// silent success a noop closer would have produced.
type wipeCloser struct{ buf []byte }

func (c *wipeCloser) Close() error {
	for i := range c.buf {
		c.buf[i] = 0
	}
	return nil
}

func (m *MemStore) Get(key []byte) ([]byte, io.Closer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[string(key)]
	if !ok {
		return nil, nil, ErrNotFound
	}
	out := append([]byte(nil), v...)
	return out, &wipeCloser{buf: out}, nil
}

func (m *MemStore) NewBatch() Batch {
	return &memBatch{store: m}
}

func (m *MemStore) NewIter(lower, upper []byte) (Iter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([][2][]byte, 0, len(m.data))
	for k, v := range m.data {
		kb := []byte(k)
		if lower != nil && bytes.Compare(kb, lower) < 0 {
			continue
		}
		if upper != nil && bytes.Compare(kb, upper) >= 0 {
			continue
		}
		entries = append(entries, [2][]byte{
			append([]byte(nil), kb...),
			append([]byte(nil), v...),
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i][0], entries[j][0]) < 0
	})
	return &memIter{entries: entries, idx: -1}, nil
}

func (m *MemStore) Checkpoint(string) error {
	return errors.New("mem store does not support checkpoint")
}

func (m *MemStore) Flush() error { return nil }

func (m *MemStore) Close() error { return nil }

type memOpType uint8

const (
	opSet memOpType = iota
	opDel
	opDelRange
)

type memBatchOp struct {
	typ   memOpType
	key   []byte
	value []byte
	end   []byte
}

var errBatchClosed = errors.New("storage: batch closed")

type memBatch struct {
	store  *MemStore
	ops    []memBatchOp
	closed bool
}

func (b *memBatch) Set(key, value []byte) error {
	if b.closed {
		return errBatchClosed
	}
	b.ops = append(b.ops, memBatchOp{
		typ:   opSet,
		key:   append([]byte(nil), key...),
		value: append([]byte(nil), value...),
	})
	return nil
}

func (b *memBatch) Delete(key []byte) error {
	if b.closed {
		return errBatchClosed
	}
	b.ops = append(b.ops, memBatchOp{
		typ: opDel,
		key: append([]byte(nil), key...),
	})
	return nil
}

func (b *memBatch) DeleteRange(start, end []byte) error {
	if b.closed {
		return errBatchClosed
	}
	b.ops = append(b.ops, memBatchOp{
		typ: opDelRange,
		key: append([]byte(nil), start...),
		end: append([]byte(nil), end...),
	})
	return nil
}

func (b *memBatch) Commit(_ bool) error {
	if b.closed {
		return errBatchClosed
	}
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	for _, op := range b.ops {
		switch op.typ {
		case opSet:
			b.store.data[string(op.key)] = op.value
		case opDel:
			delete(b.store.data, string(op.key))
		case opDelRange:
			for k := range b.store.data {
				kb := []byte(k)
				if bytes.Compare(kb, op.key) >= 0 && bytes.Compare(kb, op.end) < 0 {
					delete(b.store.data, k)
				}
			}
		}
	}
	b.ops = nil
	return nil
}

func (b *memBatch) Close() error {
	b.closed = true
	b.ops = nil
	return nil
}

type memIter struct {
	entries [][2][]byte
	idx     int
}

func (it *memIter) First() bool {
	it.idx = 0
	return it.idx < len(it.entries)
}

func (it *memIter) SeekGE(key []byte) bool {
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i][0], key) >= 0
	})
	return it.idx < len(it.entries)
}

func (it *memIter) Next() bool {
	if it.idx < len(it.entries) {
		it.idx++
	}
	return it.idx < len(it.entries)
}

func (it *memIter) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.entries)
}

func (it *memIter) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx][0]
}

func (it *memIter) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx][1]
}

func (*memIter) Error() error { return nil }
func (*memIter) Close() error { return nil }
