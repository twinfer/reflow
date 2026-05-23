package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// lpReplicaLookupTimeout bounds the SyncRead on shard 0 used to
// resolve dest-shard replicas. The caller's runScan context is
// leader-scoped (no deadline); dragonboat's SyncRead rejects a
// deadlineless context.
const lpReplicaLookupTimeout = 5 * time.Second

// LPSSTUploader fans an LP-transfer SST upload across every replica
// hosting the destination shard. Pebble Ingest is replica-local —
// every replica needs the file before its apply arm Ingests it.
//
// Construction is cheap; the resolver + client are shared with the
// host's Send path (one delivery.Client per node, one resolver). Wire
// once at startup, hand to engine via Host.SetLPSSTUploader.
type LPSSTUploader struct {
	resolver ReplicaResolver
	client   *Client
	log      *slog.Logger
}

// NewLPSSTUploader builds the fan-out wrapper. resolver is typically
// *engine.Host; client is the same delivery.Client wired as
// CrossShardSender on the host.
func NewLPSSTUploader(resolver ReplicaResolver, client *Client, log *slog.Logger) *LPSSTUploader {
	if log == nil {
		log = slog.Default()
	}
	return &LPSSTUploader{resolver: resolver, client: client, log: log}
}

// UploadSSTToReplicas uploads filePath to every replica of destShard.
// Returns the receiver-confirmed relative path (identical on every
// replica by construction: the server names the file
// `<namespace>.sst`). All replicas must succeed; a single failure
// aborts the fan-out and surfaces the first error.
func (u *LPSSTUploader) UploadSSTToReplicas(
	ctx context.Context,
	destShard uint64,
	transferID, namespace, filePath string,
) (string, error) {
	lookupCtx, lookupCancel := context.WithTimeout(ctx, lpReplicaLookupTimeout)
	replicas, err := u.resolver.PartitionReplicas(lookupCtx, destShard)
	lookupCancel()
	if err != nil {
		return "", fmt.Errorf("lp upload: list replicas of shard %d: %w", destShard, err)
	}
	if len(replicas) == 0 {
		return "", fmt.Errorf("lp upload: no replicas for shard %d", destShard)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		relPath  string
	)
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, nodeID := range replicas {
		wg.Add(1)
		go func(nid uint64) {
			defer wg.Done()
			rp, err := u.client.UploadLPTransferSST(uploadCtx, nid, destShard, transferID, namespace, filePath)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("lp upload to node %d: %w", nid, err)
					cancel()
				}
				return
			}
			if relPath == "" {
				relPath = rp
			} else if relPath != rp {
				if firstErr == nil {
					firstErr = fmt.Errorf("lp upload: replica %d reported %q; first replica reported %q", nid, rp, relPath)
				}
			}
		}(nodeID)
	}
	wg.Wait()
	if firstErr != nil {
		return "", firstErr
	}
	if relPath == "" {
		return "", errors.New("lp upload: no replica returned a relative_path")
	}
	return relPath, nil
}
