package loadgen

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/twinfer/reflow/internal/engine"
	"github.com/twinfer/reflow/internal/engine/routing"
	"github.com/twinfer/reflow/pkg/sdk"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
)

// WorkloadConfig configures the invoke-and-complete workload.
type WorkloadConfig struct {
	// Cluster is the live cluster the workload drives.
	Cluster *Cluster
	// Service / Handler are registered with sdk.Registry by the
	// caller before NewCluster; the workload will fan invocations
	// out to (Service, Handler) using random object keys so that
	// partition assignment spreads roughly evenly across shards.
	Service string
	Handler string
	// RatePerSec caps issued invocations per second across all
	// shards. Internally implemented with golang.org/x/time/rate.
	RatePerSec float64
	// Concurrency caps the number of in-flight invocations the
	// workload tracks at once; the issuer blocks once
	// in-flight == Concurrency until completions free a slot.
	Concurrency int
	// Duration bounds how long Run executes.
	Duration time.Duration
	// PollInterval is the cadence the workload uses to poll
	// LookupInvocationStatus for each in-flight invocation.
	PollInterval time.Duration
}

// WorkloadStats is the per-run summary returned by Run. Latency is
// captured by a Sampler the caller provides separately — this struct
// holds only counts and timing totals.
type WorkloadStats struct {
	Issued    uint64
	Completed uint64
	Failed    uint64
	// InFlightAtEnd is the count of issued-but-never-observed
	// invocations at the moment Run returned. Non-zero means the
	// workload's PollInterval / wind-down didn't catch up before
	// Duration expired; the post-run invariant check still has to
	// resolve them.
	InFlightAtEnd uint64
	Elapsed       time.Duration
	// FailedSamples captures up to 10 distinct ProposeIngress error
	// strings so the operator can see WHAT broke (not just the count).
	FailedSamples []string
}

// HelloHandler is a trivial sdk.Handler that returns its input
// unchanged. Use it as the registered handler for invoke-and-complete
// workloads.
func HelloHandler(_ sdk.Context, in []byte) ([]byte, error) { return in, nil }

// Run executes the workload until ctx is cancelled or Duration
// elapses, whichever comes first. Returns a WorkloadStats and the
// set of (shardID, invocationID) tuples that were issued — the
// post-run invariant checker uses the set to assert completion.
func (cfg WorkloadConfig) Run(ctx context.Context, sampler *Sampler) (WorkloadStats, []IssuedInvocation, error) {
	if cfg.Cluster == nil {
		return WorkloadStats{}, nil, fmt.Errorf("loadgen: WorkloadConfig.Cluster is nil")
	}
	if cfg.RatePerSec <= 0 {
		return WorkloadStats{}, nil, fmt.Errorf("loadgen: RatePerSec must be > 0")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 256
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.Service == "" || cfg.Handler == "" {
		return WorkloadStats{}, nil, fmt.Errorf("loadgen: Service and Handler are required")
	}

	limiter := rate.NewLimiter(rate.Limit(cfg.RatePerSec), int(cfg.RatePerSec)+1)
	runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	type inflightEntry struct {
		inv      IssuedInvocation
		issuedAt time.Time
	}
	var inflight sync.Map // string(invocation key) -> inflightEntry
	var inflightCount atomic.Int64
	slots := make(chan struct{}, cfg.Concurrency)
	for range cfg.Concurrency {
		slots <- struct{}{}
	}

	var (
		issued      atomic.Uint64
		completed   atomic.Uint64
		failed      atomic.Uint64
		issuedList  []IssuedInvocation
		issuedMu    sync.Mutex
		errSamples  []string
		errSampleMu sync.Mutex
	)

	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		t := time.NewTicker(cfg.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
			}
			inflight.Range(func(k, v any) bool {
				key := k.(string)
				entry := v.(inflightEntry)
				live := cfg.Cluster.AnyLiveNode()
				if live == nil {
					return false
				}
				lookupCtx, lc := context.WithTimeout(context.Background(), 500*time.Millisecond)
				st, err := live.Host.LookupInvocationStatus(lookupCtx, entry.inv.ShardID, entry.inv.ID)
				lc()
				if err != nil || st == nil {
					return true
				}
				switch s := st.GetStatus().(type) {
				case *enginev1.InvocationStatus_Completed:
					if sampler != nil {
						sampler.ObserveLatency(time.Since(entry.issuedAt))
					}
					if s.Completed.GetFailureMessage() != "" {
						failed.Add(1)
					} else {
						completed.Add(1)
					}
					inflight.Delete(key)
					inflightCount.Add(-1)
					select {
					case slots <- struct{}{}:
					default:
					}
				}
				return true
			})
		}
	}()

	partitioner := cfg.Cluster.Partitioner
	for {
		if err := limiter.Wait(runCtx); err != nil {
			break
		}
		select {
		case <-slots:
		case <-runCtx.Done():
		}
		if runCtx.Err() != nil {
			break
		}
		inv := newInvocation(cfg.Service, partitioner)
		// Pick the current leader for inv.ShardID; if none is known
		// (election in flight), fall back to any live node that
		// hosts the shard — dragonboat forwards the propose to
		// whoever leads. Killed nodes appear as nil in Cluster.Nodes
		// and must be skipped.
		leader := cfg.Cluster.FindPartitionLeader(inv.ShardID)
		var proposer *engine.PartitionRunner
		if leader != nil {
			proposer = leader.Host.Partition(inv.ShardID)
		}
		if proposer == nil {
			for _, n := range cfg.Cluster.Nodes {
				if n == nil {
					continue
				}
				if pr := n.Host.Partition(inv.ShardID); pr != nil {
					leader = n
					proposer = pr
					break
				}
			}
		}
		if proposer == nil {
			failed.Add(1)
			select {
			case slots <- struct{}{}:
			default:
			}
			continue
		}

		seq := issued.Add(1)
		// Decouple propose budget from runCtx so end-of-run shrinkage
		// doesn't shorten ctx below dragonboat's RTT-based minimum.
		// The runCtx still cancels in-flight proposes when fired.
		proposeCtx, pc := context.WithTimeout(context.Background(), 15*time.Second)
		go func(c context.Context, cancel context.CancelFunc) {
			select {
			case <-runCtx.Done():
				cancel()
			case <-c.Done():
			}
		}(proposeCtx, pc)
		err := proposer.Proposer().ProposeIngress(proposeCtx, "loadgen", seq, &enginev1.Command{
			Kind: &enginev1.Command_Invoke{Invoke: &enginev1.InvokeCommand{
				InvocationId: inv.ID,
				Target:       &enginev1.InvocationTarget{ServiceName: cfg.Service, HandlerName: cfg.Handler},
				Input:        []byte("x"),
			}},
		})
		pc()
		if err != nil {
			failed.Add(1)
			errSampleMu.Lock()
			if len(errSamples) < 10 {
				errSamples = append(errSamples, err.Error())
			}
			errSampleMu.Unlock()
			select {
			case slots <- struct{}{}:
			default:
			}
			continue
		}
		// Only successfully-proposed invocations count toward the
		// in-flight tracking and the post-run invariant set.
		issuedMu.Lock()
		issuedList = append(issuedList, inv)
		issuedMu.Unlock()
		inflight.Store(encodeKey(inv), inflightEntry{inv: inv, issuedAt: time.Now()})
		inflightCount.Add(1)
	}

	<-pollerDone

	errSampleMu.Lock()
	samplesCopy := append([]string(nil), errSamples...)
	errSampleMu.Unlock()
	return WorkloadStats{
		Issued:        issued.Load(),
		Completed:     completed.Load(),
		Failed:        failed.Load(),
		InFlightAtEnd: uint64(inflightCount.Load()),
		Elapsed:       cfg.Duration,
		FailedSamples: samplesCopy,
	}, issuedList, nil
}

// IssuedInvocation captures the minimum info the invariant checker
// needs to verify post-run state: which invocation id was issued, on
// which shard, and against which service.
type IssuedInvocation struct {
	ID      *enginev1.InvocationId
	ShardID uint64
	Service string
}

func newInvocation(service string, p routing.Partitioner) IssuedInvocation {
	var uuidBytes [16]byte
	_, _ = rand.Read(uuidBytes[:])
	// Spread keys across shards by varying the object key.
	objectKey := fmt.Sprintf("k%d", binary.BigEndian.Uint64(uuidBytes[:8])%1024)
	pk := routing.PartitionKey(service, objectKey)
	shard := p.ShardForKey(pk)
	return IssuedInvocation{
		ID:      &enginev1.InvocationId{PartitionKey: pk, Uuid: uuidBytes[:]},
		ShardID: shard,
		Service: service,
	}
}

func encodeKey(inv IssuedInvocation) string {
	return fmt.Sprintf("%d/%x", inv.ShardID, inv.ID.GetUuid())
}
