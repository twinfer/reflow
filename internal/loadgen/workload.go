package loadgen

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/twinfer/reflw/internal/engine/routing"
	"github.com/twinfer/reflw/internal/storage/keys"
	"github.com/twinfer/reflw/pkg/handler"
	enginev1 "github.com/twinfer/reflw/proto/enginev1"
)

// WorkloadCluster is the cluster surface the workload + invariant
// helpers consume. *Cluster (in-proc) and *internal/e2e.ContainerCluster
// both satisfy it. Kept minimal so a fresh implementation only needs
// a way to pick a live node to submit/poll against.
type WorkloadCluster interface {
	// AnyLiveNode returns any non-killed node, or nil when every node
	// has been torn down.
	AnyLiveNode() Node
}

// WorkloadConfig configures the invoke-and-complete workload.
type WorkloadConfig struct {
	// Cluster picks the live node each submit and poll routes through.
	Cluster WorkloadCluster
	// Partitioner is the modulus the workload uses to derive the
	// shard owning each invocation (only for IssuedInvocation.ShardID,
	// which feeds invariant diagnostics). For in-proc clusters this
	// is `(*loadgen.Cluster).Partitioner`; for the e2e tier it's a
	// fresh routing.NewPartitioner sized for the cluster.
	Partitioner routing.Partitioner
	// Service / Handler are registered with handler.Registry by the
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
	// DescribeInvocation for each in-flight invocation.
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
	// FailedSamples captures up to 10 distinct submit-error strings so
	// the operator can see WHAT broke (not just the count).
	FailedSamples []string
}

// HelloHandler is a trivial handler.Handler that returns its input
// unchanged. Use it as the registered handler for invoke-and-complete
// workloads.
func HelloHandler(_ handler.Context, in []byte) ([]byte, error) { return in, nil }

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
				st, err := live.DescribeInvocation(lookupCtx, entry.inv.ID)
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

	partitioner := cfg.Partitioner
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

		// Submit through any live node. The node's SubmitInvocation
		// routes server-side via the host's Partitioner (in-process)
		// or via ingressv1.SubmitInvocation (subprocess) — the workload
		// no longer needs to pre-pick the leader.
		node := cfg.Cluster.AnyLiveNode()
		if node == nil {
			failed.Add(1)
			select {
			case slots <- struct{}{}:
			default:
			}
			continue
		}

		objectKey := randomObjectKey(cfg.Service)
		// Decouple submit budget from runCtx so end-of-run shrinkage
		// doesn't shorten ctx below dragonboat's RTT-based minimum.
		// The runCtx still cancels in-flight submits when fired.
		submitCtx, sc := context.WithTimeout(context.Background(), 15*time.Second)
		go func(c context.Context, cancel context.CancelFunc) {
			select {
			case <-runCtx.Done():
				cancel()
			case <-c.Done():
			}
		}(submitCtx, sc)
		id, err := node.SubmitInvocation(submitCtx, cfg.Service, cfg.Handler, objectKey, []byte("x"))
		sc()
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

		issued.Add(1)
		inv := IssuedInvocation{
			ID:      id,
			ShardID: partitioner.ShardForKey(id.GetPartitionKey()),
			Service: cfg.Service,
		}
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

// randomObjectKey spreads invocations across partitions while keeping them in
// LPs below FirstTenantedLP — the region the chain-transfer driver's high-LP
// targets stay clear of, so live traffic never routes to an LP mid-transfer (the
// in-process loadgen host routes statically; see transfer.go). Rejection-samples
// free-form keys until the (service, key) hash lands in the low region, so the
// spread across those LPs stays uniform.
func randomObjectKey(service string) string {
	var b [8]byte
	for {
		_, _ = rand.Read(b[:])
		key := fmt.Sprintf("k%x", b[:])
		if keys.LPFromPartitionKey(routing.PartitionKey(service, key)) < FirstTenantedLP {
			return key
		}
	}
}

func encodeKey(inv IssuedInvocation) string {
	return fmt.Sprintf("%d/%x", inv.ShardID, inv.ID.GetUuid())
}
