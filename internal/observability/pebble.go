package observability

import (
	"log/slog"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

// NewPebbleEventListener builds a *pebble.EventListener that feeds m's
// Pebble counters and routes disk-stall detection. One listener is shared
// by every shard DB on the node (attached via storage.PebbleTuning).
//
// maxSyncDuration > 0 arms disk-stall handling: a DiskSlow event whose
// Duration meets or exceeds it is treated as a stall — logged at Error
// and, if onStall != nil, onStall(info) is invoked. Production passes a
// process-fatal there so a wedged disk can't keep the node in quorum
// while it silently stops applying (cockroach does the same,
// pkg/storage/pebble.go:1462). A DiskSlow below the threshold is logged
// at Warn. maxSyncDuration <= 0 disables stall handling entirely — the
// right choice for tests and slow-disk fault injection, which must never
// crash the process.
//
// m may be nil (metrics disabled); the listener still logs and handles
// stalls. The returned listener has EnsureDefaults applied, so the
// unset event hooks are safe no-ops.
func (m *Metrics) NewPebbleEventListener(
	log *slog.Logger,
	maxSyncDuration time.Duration,
	onStall func(pebble.DiskSlowInfo),
) *pebble.EventListener {
	if log == nil {
		log = slog.Default()
	}
	el := pebble.EventListener{
		CompactionEnd: func(pebble.CompactionInfo) {
			if m != nil {
				m.PebbleCompactions.Inc()
			}
		},
		FlushEnd: func(pebble.FlushInfo) {
			if m != nil {
				m.PebbleFlushes.Inc()
			}
		},
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			if m != nil {
				m.PebbleWriteStalls.Inc()
			}
			log.Warn("pebble: write stall", "reason", info.Reason)
		},
		BackgroundError: func(err error) {
			log.Error("pebble: background error", "err", err)
		},
		DiskSlow: func(info pebble.DiskSlowInfo) {
			if m != nil {
				m.PebbleDiskSlow.Inc()
			}
			if maxSyncDuration > 0 && info.Duration >= maxSyncDuration {
				log.Error("pebble: disk stall detected",
					"path", info.Path,
					"duration", info.Duration,
					"threshold", maxSyncDuration)
				if onStall != nil {
					onStall(info)
				}
				return
			}
			log.Warn("pebble: slow disk op",
				"path", info.Path,
				"duration", info.Duration)
		},
	}
	el.EnsureDefaults(nil)
	return &el
}
