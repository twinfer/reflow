package storage

import (
	"runtime"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// DefaultPebbleCacheBytes is the node-global block-cache budget used when
// the operator doesn't set one. It is shared across every shard DB on the
// node (see NewSharedCaches). Pebble's own default is a small per-DB cache;
// on reflw's one-DB-per-shard topology that default would multiply by
// shard count and fragment the working set into N independent LRUs.
const DefaultPebbleCacheBytes int64 = 256 << 20 // 256 MiB

// DefaultMaxSyncDuration is the disk-stall threshold armed by default. A
// disk operation whose health-checked duration meets or exceeds it is
// treated as a stall. Mirrors cockroach storage.max_sync_duration
// (pkg/storage/fs/fs.go).
const DefaultMaxSyncDuration = 20 * time.Second

// PebbleTuning carries the node-global Pebble resources and scalar knobs
// shared across every shard DB on one engine.Host. Construct the caches
// once with NewSharedCaches, build a PebbleTuning, and pass Options as
// engine.HostConfig.PebbleOptions. The scalar values mirror cockroach's
// production defaults (pkg/storage/pebble.go initPebbleOptions); the
// rationale for each is inline in Options.
type PebbleTuning struct {
	// Cache is the node-global block cache, shared by every shard DB. The
	// caller owns one ref; see NewSharedCaches.
	Cache *pebble.Cache
	// FileCache is the node-global open-sstable (table reader) cache.
	FileCache *pebble.FileCache
	// EventListener, when non-nil, is attached to every shard DB so
	// compaction / flush / write-stall / disk-slow events reach the
	// metrics + disk-stall handler. nil leaves Pebble's no-op default.
	EventListener *pebble.EventListener
}

// Options returns the tuned per-shard *pebble.Options. shardID is unused
// today — every shard gets identical tuning — but kept in the signature
// to match engine.HostConfig.PebbleOptions and leave room for per-shard
// policy (e.g. a latency-tolerant metadata shard).
func (t PebbleTuning) Options(shardID uint64) *pebble.Options {
	opts := &pebble.Options{
		Cache:     t.Cache,
		FileCache: t.FileCache,
		// L0CompactionThreshold=2 triggers a compaction at one L0
		// sub-level — keep read-amp low. L0StopWritesThreshold is set
		// high so L0 read-amp (a soft signal the apply path tolerates),
		// not a hard write stop, is the back-pressure under burst.
		L0CompactionThreshold: 2,
		L0StopWritesThreshold: 1000,
		LBaseMaxBytes:         64 << 20, // 64 MiB
		// Allow a few queued memtables before stalling writes; queued
		// memtables borrow from the block cache, so this is bounded.
		MemTableStopWritesThreshold: 4,
		// Flush range tombstones / range keys ~10s after they land even
		// on an otherwise-idle shard, so space from LP-transfer range
		// deletes (FinishLPTransfer) and reap is reclaimed promptly
		// instead of waiting for the next write-driven flush.
		FlushDelayDeleteRange: 10 * time.Second,
		FlushDelayRangeKey:    10 * time.Second,
	}
	if t.EventListener != nil {
		opts.EventListener = t.EventListener
	}
	return opts
}

// NewSharedCaches builds the node-global block + file caches sized for a
// host that will open numShards DBs. The caller holds one ref on each;
// Unref both — Cache.Unref / FileCache.Unref — only after the engine, and
// therefore every shard DB that referenced them, has closed. cacheBytes
// <= 0 falls back to DefaultPebbleCacheBytes.
func NewSharedCaches(cacheBytes int64, numShards int) (*pebble.Cache, *pebble.FileCache) {
	if cacheBytes <= 0 {
		cacheBytes = DefaultPebbleCacheBytes
	}
	// File-cache slots cap the number of sstable readers held open across
	// all shards as one shared LRU. A shared pool of this size beats N
	// static per-DB pools: hot shards borrow slots cold shards don't use.
	fileCacheSlots := max(numShards*100, 1000)
	// First arg is the file cache's internal lock-striping shard count —
	// size it to parallelism (GOMAXPROCS), not the DB count.
	stripes := max(runtime.GOMAXPROCS(0), 1)
	return pebble.NewCache(cacheBytes), pebble.NewFileCache(stripes, fileCacheSlots)
}
