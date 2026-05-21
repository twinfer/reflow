// Package webhook mounts config-driven inbound vendor webhook
// endpoints on the existing ingress listener. Cluster-managed in v2:
// each WebhookSourceRecord lives on shard 0; per-node Manager
// instances reconcile against a TableNotifier wake (5s ticker
// backstop), resolve secrets via SecretRef on each pass, and
// atomically swap a fresh path→source snapshot. Inbound requests hit
// a single stable subtree route at /webhooks/; the handler looks up
// r.URL.Path in the live snapshot. The atomic-snapshot pattern means
// in-flight requests keep dispatching against the secret + verifier
// they were dispatched with — no per-request locking on the hot path.
package webhook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	connect "connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/twinfer/reflow/internal/connectserver"
	"github.com/twinfer/reflow/pkg/webhook"
	enginev1 "github.com/twinfer/reflow/proto/enginev1"
	ingressv1 "github.com/twinfer/reflow/proto/ingressv1"
)

// reconcileInterval is the backstop tick. Even if the TableNotifier
// wake is lost the loop re-reads shard 0 every interval; the SyncRead
// traffic is trivial.
const reconcileInterval = 5 * time.Second

// catchAllPath is the subtree pattern the stdlib mux uses to route
// every request under /webhooks/ to the Manager's single handler.
// Trailing slash matters — http.ServeMux's subtree semantics depend
// on it. See internal/connectserver/server.go's mux.Handle.
const catchAllPath = "/webhooks/"

// Submitter is the in-process surface the Manager calls. Satisfied by
// *internal/ingress.Server. Mirrors eventsource.Submitter.
type Submitter interface {
	SubmitInvocation(ctx context.Context, req *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error)
}

// SourceConfig is the resolved in-memory shape one Manager snapshot
// holds per webhook. Production builds it inside RunReconciler from a
// WebhookSourceRecord + a SecretRef resolution; tests build it
// directly.
//
// Name is the operator-facing key (mirrors metadata.name in the
// kubectl-style YAML). Path is the absolute URL the listener serves;
// uniqueness across the snapshot is enforced at Reconcile time
// (sorted by Name, first wins on a collision).
type SourceConfig struct {
	Name      string
	Path      string
	Verifier  string
	Secret    []byte
	Service   string
	Handler   string
	ObjectKey string
	Metadata  map[string]string
}

// Reader is the seam RunReconciler uses to fetch desired state.
// Production wiring is a thin adapter over engine.Host.WebhookSources;
// tests can hand in a fake.
type Reader interface {
	ListWebhookSources(ctx context.Context) ([]*enginev1.WebhookSourceRecord, uint64 /*tableRev*/, error)
}

// Manager owns the live path→resolvedSource snapshot. The snapshot is
// only mutated by Reconcile, which atomically swaps a fresh map; in-
// flight requests see whichever map was live when their Load() ran.
// There is no per-request lock and no goroutine drain on Reconcile.
type Manager struct {
	submitter Submitter
	metrics   *Metrics
	log       *slog.Logger
	errWriter *connect.ErrorWriter
	live      atomic.Pointer[snapshot]
	closed    atomic.Bool

	// reconcileMu serializes concurrent Reconcile() calls so the
	// "previous snapshot" lookup for secret carry-over stays
	// consistent across overlapping callers (which only happen in
	// tests; RunReconciler is single-flighted).
	reconcileMu sync.Mutex
	// cancel is the RunReconciler loop's cancel; Close trips it.
	cancel context.CancelFunc
}

// resolvedSource is one row in the live snapshot: SourceConfig plus
// the verifier resolved from the registry.
type resolvedSource struct {
	cfg      SourceConfig
	verifier webhook.Verifier
}

// snapshot is what live points to: byPath for the hot lookup +
// byName for prev-secret carry-over at next reconcile.
type snapshot struct {
	byPath map[string]*resolvedSource
	byName map[string]*resolvedSource
}

// New constructs an empty Manager. Use Reconcile to populate the
// snapshot; use RunReconciler for the production wake-on-notifier
// loop, or call Reconcile directly from tests.
func New(submitter Submitter, reg prometheus.Registerer, log *slog.Logger) (*Manager, error) {
	if submitter == nil {
		return nil, errors.New("webhook: submitter is required")
	}
	if log == nil {
		log = slog.Default()
	}
	m := &Manager{
		submitter: submitter,
		metrics:   NewMetrics(reg),
		log:       log,
		errWriter: connect.NewErrorWriter(),
	}
	empty := &snapshot{
		byPath: map[string]*resolvedSource{},
		byName: map[string]*resolvedSource{},
	}
	m.live.Store(empty)
	return m, nil
}

// NewManager is the legacy static-config convenience used by the
// existing unit-test smoke. ValidateSources runs upfront so a bad
// static config aborts startup with a clean error; production
// (reconcile-driven) reconciles log+drop bad rows instead so a single
// malformed row can't take the whole snapshot offline.
//
// For an empty slice returns a Manager whose live snapshot is empty;
// Routes() still yields the stable /webhooks/ catch-all which 404s
// every request (operators can wire unconditionally).
func NewManager(sources []SourceConfig, submitter Submitter, log *slog.Logger) (*Manager, error) {
	if len(sources) > 0 {
		if submitter == nil {
			return nil, errors.New("webhook: NewManager requires a non-nil Submitter when sources are configured")
		}
		if err := ValidateSources(sources); err != nil {
			return nil, err
		}
	}
	if submitter == nil {
		// Empty-sources Manager still needs a stub-safe Submitter for
		// the Routes() catch-all (which 404s without dispatching);
		// production callers always pass a real Submitter.
		submitter = noopSubmitter{}
	}
	// Use a fresh registry so legacy tests don't fight the default
	// global; production callers use New + a real registerer.
	reg := prometheus.NewRegistry()
	m, err := New(submitter, reg, log)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		return m, nil
	}
	if err := m.Reconcile(context.Background(), sources); err != nil {
		return nil, err
	}
	return m, nil
}

// noopSubmitter is a placeholder for empty-sources Managers built via
// NewManager(nil, nil, nil). The catch-all route returns 404 for
// every request when the snapshot is empty, so the submitter is
// never invoked.
type noopSubmitter struct{}

func (noopSubmitter) SubmitInvocation(_ context.Context, _ *connect.Request[ingressv1.SubmitInvocationRequest]) (*connect.Response[ingressv1.SubmitInvocationResponse], error) {
	return nil, errors.New("webhook: noop submitter")
}

// Routes returns one stable subtree route at /webhooks/. The route is
// mounted unconditionally — an empty snapshot just produces 404s.
// connectserver/server.go uses stdlib http.NewServeMux whose subtree
// pattern ("/foo/") matches every request whose path starts with
// "/foo/", which is exactly the catch-all we want.
func (m *Manager) Routes() []connectserver.Route {
	return []connectserver.Route{{
		Path:    catchAllPath,
		Handler: http.HandlerFunc(m.serve),
	}}
}

// Close trips the closed flag and cancels any active RunReconciler.
// The Manager holds no goroutines or long-lived resources beyond the
// reconciler loop, so this returns immediately.
func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	if m.cancel != nil {
		m.cancel()
	}
	return nil
}

// RunReconciler is the production-mode reconcile loop. Wakes on the
// notifier (FSM post-commit Bump) or a 5s ticker, SyncRead's the
// desired state, resolves secrets, and atomically swaps the snapshot.
// Errors are logged + counted; the loop keeps running until ctx is
// cancelled.
//
// Goroutine affinity: own dedicated goroutine. Never runs on the FSM
// apply path. The notifier wake just signals; secret reads + Reconcile
// happen off-loop, in line with internal/engine/CLAUDE.md.
func (m *Manager) RunReconciler(ctx context.Context, sub <-chan struct{}, reader Reader) error {
	if m == nil {
		return nil
	}
	if reader == nil {
		return errors.New("webhook: reader is required for reconcile loop")
	}
	ctx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	m.reconcileFromReader(ctx, reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sub:
			m.reconcileFromReader(ctx, reader)
		case <-ticker.C:
			m.reconcileFromReader(ctx, reader)
		}
	}
}

// reconcileFromReader does one ListWebhookSources + secret resolution
// + Reconcile pass. Errors are logged + counted, never propagated.
func (m *Manager) reconcileFromReader(ctx context.Context, reader Reader) {
	records, rev, err := reader.ListWebhookSources(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			m.log.Warn("webhook: read desired state", "err", err)
			m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, "*").Inc()
		}
		return
	}
	prev := m.live.Load()
	desired := make([]SourceConfig, 0, len(records))
	for _, rec := range records {
		var prevSecret []byte
		if prev != nil {
			if prevR, ok := prev.byName[rec.GetName()]; ok {
				prevSecret = prevR.cfg.Secret
			}
		}
		secret, source, serr := resolveSecret(ctx, rec.GetSecretRef(), rec.GetName(), m.metrics)
		if serr != nil {
			m.log.Warn("webhook: resolve secret",
				"name", rec.GetName(), "source", source, "err", serr)
			m.metrics.SecretResolveErrors.WithLabelValues(rec.GetName(), source).Inc()
			if prevSecret == nil {
				// No prior bytes to carry — skip this source. The
				// next reconcile will retry; meanwhile the previous
				// snapshot's other entries keep serving.
				continue
			}
			secret = prevSecret
		}
		desired = append(desired, SourceConfig{
			Name:      rec.GetName(),
			Path:      rec.GetPath(),
			Verifier:  rec.GetVerifier(),
			Secret:    secret,
			Service:   rec.GetService(),
			Handler:   rec.GetHandler(),
			ObjectKey: rec.GetObjectKey(),
			Metadata:  copyMetadata(rec.GetMetadata()),
		})
	}
	if err := m.Reconcile(ctx, desired); err != nil {
		m.log.Warn("webhook: reconcile failed", "err", err)
	}
	m.metrics.TableRevision.WithLabelValues(reconcileLabelTable).Set(float64(rev))
}

// Reconcile rebuilds the live snapshot from `desired`. The build pass
// sorts by Name and drops duplicates on Path (first wins) so every
// node lands on the same byPath map even if the table somehow
// committed a path collision.
func (m *Manager) Reconcile(ctx context.Context, desired []SourceConfig) error {
	if m == nil {
		return nil
	}
	if m.closed.Load() {
		return errors.New("webhook: manager is closed")
	}
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()
	prev := m.live.Load()
	// Stable sort by Name so the dedup winner is identical across nodes.
	sorted := append([]SourceConfig(nil), desired...)
	sort.SliceStable(sorted, func(i, j int) bool {
		ni := sorted[i].Name
		nj := sorted[j].Name
		if ni == "" {
			ni = sorted[i].Path
		}
		if nj == "" {
			nj = sorted[j].Path
		}
		return ni < nj
	})
	next := &snapshot{
		byPath: make(map[string]*resolvedSource, len(sorted)),
		byName: make(map[string]*resolvedSource, len(sorted)),
	}
	for _, sc := range sorted {
		if err := validateSourceConfig(sc); err != nil {
			m.log.Warn("webhook: skipping invalid source",
				"name", sc.Name, "path", sc.Path, "err", err)
			m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, sc.Name).Inc()
			continue
		}
		// Verifier registry lookup is stateless; instances are safe
		// for concurrent use across snapshots.
		var verifier webhook.Verifier
		if prev != nil {
			if prevR, ok := prev.byName[sc.Name]; ok && prevR.cfg.Verifier == sc.Verifier {
				verifier = prevR.verifier
			}
		}
		if verifier == nil {
			v, err := webhook.LookupVerifier(sc.Verifier)
			if err != nil {
				m.log.Warn("webhook: verifier lookup failed",
					"name", sc.Name, "verifier", sc.Verifier, "err", err)
				m.metrics.ReconcileErrors.WithLabelValues(reconcileLabelTable, sc.Name).Inc()
				continue
			}
			verifier = v
		}
		// Dedup on path. Sorted-by-name so the winner is deterministic
		// across every node even if the FSM somehow committed both
		// rows (the admin RPC's path-uniqueness check is racy).
		if _, taken := next.byPath[sc.Path]; taken {
			m.log.Warn("webhook: duplicate path; dropping loser",
				"name", sc.Name, "path", sc.Path)
			m.metrics.DuplicatePath.WithLabelValues(sc.Path).Inc()
			continue
		}
		entry := &resolvedSource{cfg: sc, verifier: verifier}
		next.byPath[sc.Path] = entry
		next.byName[sc.Name] = entry
	}
	m.live.Store(next)
	return nil
}

// validateSourceConfig is the per-record sanity check Reconcile runs
// before adding to the next snapshot. Same rule set as the admin
// validator; duplicated locally so test callers (NewManager) get the
// same protection without dragging proto into their test setup.
func validateSourceConfig(sc SourceConfig) error {
	if sc.Path == "" || !strings.HasPrefix(sc.Path, "/") {
		return fmt.Errorf("path %q must be absolute", sc.Path)
	}
	if sc.Verifier == "" {
		return errors.New("verifier is required")
	}
	if sc.Service == "" {
		return errors.New("service is required")
	}
	if sc.Handler == "" {
		return errors.New("handler is required")
	}
	if len(sc.Secret) == 0 {
		return errors.New("secret is required")
	}
	return nil
}

// ValidateSources runs validateSourceConfig + verifier-registry
// existence + path-uniqueness across the slice. Retained as a public
// helper so callers (cmd/reflowd seeding, tests) can pre-flight
// configs without constructing a Manager. Returns nil for an empty
// slice.
func ValidateSources(sources []SourceConfig) error {
	if len(sources) == 0 {
		return nil
	}
	seen := make(map[string]string, len(sources))
	for i, s := range sources {
		if err := validateSourceConfig(s); err != nil {
			return fmt.Errorf("webhook: sources[%d] (path=%q): %w", i, s.Path, err)
		}
		if prior, dup := seen[s.Path]; dup {
			return fmt.Errorf("webhook: sources[%d]: duplicate path %q (previously bound to verifier %q)", i, s.Path, prior)
		}
		seen[s.Path] = s.Verifier
		if _, err := webhook.LookupVerifier(s.Verifier); err != nil {
			return fmt.Errorf("webhook: sources[%d] (path=%q): %w (registered: %s)", i, s.Path, err, registeredList())
		}
	}
	return nil
}

// serve is the single handler bound to /webhooks/. Looks up the
// request path in the live snapshot; on miss returns 404 +
// reflow_webhook_unknown_path_total; on hit runs the per-source
// verify + dispatch pipeline.
func (m *Manager) serve(w http.ResponseWriter, r *http.Request) {
	snap := m.live.Load()
	var src *resolvedSource
	if snap != nil {
		src = snap.byPath[r.URL.Path]
	}
	if src == nil {
		m.metrics.UnknownPath.Inc()
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ev, err := src.verifier.Verify(r.Context(), r, src.cfg.Secret)
	if err != nil {
		m.log.Warn("webhook: signature rejected",
			"path", src.cfg.Path, "verifier", src.cfg.Verifier, "err", err)
		m.metrics.VerifyFailed.WithLabelValues(src.cfg.Verifier).Inc()
		writeWebhookError(w, r, m.errWriter, err)
		return
	}
	meta := mergeMetadata(src.cfg.Metadata, ev.Metadata)
	req := connect.NewRequest(&ingressv1.SubmitInvocationRequest{
		Service:   src.cfg.Service,
		Handler:   src.cfg.Handler,
		ObjectKey: src.cfg.ObjectKey,
		Input:     ev.Body,
		Metadata:  meta,
	})
	resp, err := m.submitter.SubmitInvocation(r.Context(), req)
	if err != nil {
		m.log.Warn("webhook: submit failed",
			"path", src.cfg.Path, "verifier", src.cfg.Verifier, "err", err)
		writeWebhookError(w, r, m.errWriter, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(resp.Msg.GetInvocationIdStr()))
}

// mergeMetadata combines the static SourceConfig.Metadata with the
// verifier-stamped facts. Verifier values win on collision.
func mergeMetadata(static, stamped map[string]string) map[string]string {
	if len(static) == 0 && len(stamped) == 0 {
		return nil
	}
	out := make(map[string]string, len(static)+len(stamped))
	maps.Copy(out, static)
	maps.Copy(out, stamped)
	return out
}

func copyMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}

// writeWebhookError emits Connect-coded errors when the request shape
// is Connect; otherwise falls back to plain HTTP status + body
// (the conventional vendor-client expectation).
func writeWebhookError(w http.ResponseWriter, r *http.Request, ew *connect.ErrorWriter, err error) {
	if cerr, ok := err.(*connect.Error); ok {
		if ew != nil && ew.IsSupported(r) {
			_ = ew.Write(w, r, cerr)
			return
		}
		status := httpStatusFromConnectCode(cerr.Code())
		http.Error(w, "webhook: "+cerr.Message(), status)
		return
	}
	http.Error(w, "webhook: "+err.Error(), http.StatusInternalServerError)
}

// httpStatusFromConnectCode maps a Connect code to the HTTP status
// the manager surfaces on the plain-HTTP fallback path.
func httpStatusFromConnectCode(c connect.Code) int {
	switch c {
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeInvalidArgument, connect.CodeFailedPrecondition, connect.CodeOutOfRange:
		return http.StatusBadRequest
	case connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	case connect.CodeResourceExhausted:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func registeredList() string {
	names := webhook.RegisteredNames()
	sort.Strings(names)
	return strings.Join(names, ",")
}
