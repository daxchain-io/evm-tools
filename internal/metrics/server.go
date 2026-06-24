package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Health holds the readiness signals consulted by /readyz, kept as atomics so
// the poll loop can update them without locking the HTTP handler. /healthz
// reflects only process liveness; /readyz additionally requires RPC
// reachability, an unblocked record writer (within threshold), and lag within
// bound — so a producer wedged on a stalled sink or far behind head reads as
// not-ready (see docs/design.md, "RPC Health Checks").
type Health struct {
	// live is set false only on an unrecoverable state; the process is live by
	// default once the server starts.
	live atomic.Bool
	// rpcReachable reflects the most recent RPC interaction outcome.
	rpcReachable atomic.Bool
	// emitBlockedMillis is the current/last record-write blocked duration.
	emitBlockedMillis atomic.Int64
	// lagBlocks is head-minus-processed.
	lagBlocks atomic.Uint64
	// headBlockUnixMillis is the timestamp of the latest observed chain head block;
	// headBlockKnown gates the staleness check until the first head block is seen.
	headBlockUnixMillis atomic.Int64
	headBlockKnown      atomic.Bool

	// emitBlockedThreshold is the blocked duration beyond which /readyz flips to
	// not-ready. lagThreshold is the lag bound. Both are set at construction.
	emitBlockedThreshold time.Duration
	lagThreshold         uint64
	// headStalenessThreshold is the max age of the latest chain head block before
	// /readyz flips not-ready; 0 disables the check. Set once at startup (before the
	// server serves) via SetHeadStalenessThreshold, so it needs no synchronization.
	headStalenessThreshold time.Duration

	// nowFn supplies the wall clock for the staleness age computation; it is
	// time.Now in production and overridable in tests. Set once at construction.
	nowFn func() time.Time
}

// NewHealth builds a Health with the given readiness thresholds. A zero
// threshold disables that check (always-ready for that dimension).
func NewHealth(emitBlockedThreshold time.Duration, lagThreshold uint64) *Health {
	h := &Health{
		emitBlockedThreshold: emitBlockedThreshold,
		lagThreshold:         lagThreshold,
		nowFn:                time.Now,
	}
	h.live.Store(true)
	return h
}

// SetHeadStalenessThreshold sets the maximum age of the latest chain head block
// before /readyz reports not-ready; 0 (the default) disables the check. It is
// configured once at startup before the server begins serving.
func (h *Health) SetHeadStalenessThreshold(d time.Duration) { h.headStalenessThreshold = d }

// SetHeadBlockTime records the timestamp of the latest observed chain head block.
// The head-staleness readiness check compares this against the wall clock at probe
// time, so /readyz flips not-ready once the chain head stops advancing — even if
// the poll loop itself wedges, since the age grows by wall clock rather than by
// poll updates.
func (h *Health) SetHeadBlockTime(t time.Time) {
	h.headBlockUnixMillis.Store(t.UnixMilli())
	h.headBlockKnown.Store(true)
}

// SetLive marks the process live or unrecoverably dead.
func (h *Health) SetLive(v bool) { h.live.Store(v) }

// SetRPCReachable records the latest RPC reachability.
func (h *Health) SetRPCReachable(v bool) { h.rpcReachable.Store(v) }

// SetEmitBlocked records the current/last record-write blocked duration.
func (h *Health) SetEmitBlocked(d time.Duration) { h.emitBlockedMillis.Store(d.Milliseconds()) }

// SetLag records head-minus-processed lag.
func (h *Health) SetLag(n uint64) { h.lagBlocks.Store(n) }

// Live reports process liveness.
func (h *Health) Live() bool { return h.live.Load() }

// readyReason returns ("", true) when ready, or a short, secret-free reason and
// false when not. The reason text uses only coarse, non-sensitive values.
func (h *Health) readyReason() (string, bool) {
	if !h.live.Load() {
		return "process not live", false
	}
	if !h.rpcReachable.Load() {
		return "rpc unreachable", false
	}
	if h.emitBlockedThreshold > 0 {
		if time.Duration(h.emitBlockedMillis.Load())*time.Millisecond >= h.emitBlockedThreshold {
			return "record write blocked beyond threshold", false
		}
	}
	if h.lagThreshold > 0 && h.lagBlocks.Load() > h.lagThreshold {
		return "lag beyond threshold", false
	}
	if h.headStalenessThreshold > 0 && h.headBlockKnown.Load() {
		headTime := time.UnixMilli(h.headBlockUnixMillis.Load())
		if h.nowFn().Sub(headTime) >= h.headStalenessThreshold {
			return "chain head stale (no new block within threshold)", false
		}
	}
	return "", true
}

// ServerOptions configures the metrics/health HTTP server.
type ServerOptions struct {
	// Addr is the bind address, e.g. ":9000".
	Addr string
	// MetricsEnabled controls whether the metrics route is served. Health
	// endpoints are always served.
	MetricsEnabled bool
	// MetricsPath is the metrics route (e.g. "/metrics"); defaults to /metrics.
	MetricsPath string
	// Registry is the gatherer scraped at MetricsPath. Required when
	// MetricsEnabled is true.
	Registry *prometheus.Registry
	// Health backs /healthz and /readyz. Required.
	Health *Health
}

// Server is the HTTP server exposing /metrics (optional), /healthz, and /readyz.
type Server struct {
	httpSrv *http.Server
	ln      net.Listener
	opts    ServerOptions
}

// NewServer builds the server and binds its listener so the actual address is
// known immediately (useful when Addr is ":0" in tests). Call Serve to begin
// accepting, then Shutdown for a clean stop.
func NewServer(opts ServerOptions) (*Server, error) {
	if opts.Health == nil {
		return nil, errors.New("metrics: server requires a Health")
	}
	if opts.MetricsPath == "" {
		opts.MetricsPath = "/metrics"
	}
	if opts.MetricsEnabled && opts.Registry == nil {
		return nil, errors.New("metrics: server requires a Registry when metrics are enabled")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if opts.Health.Live() {
			writeStatus(w, http.StatusOK, "ok")
			return
		}
		writeStatus(w, http.StatusServiceUnavailable, "not live")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if reason, ok := opts.Health.readyReason(); ok {
			writeStatus(w, http.StatusOK, "ready")
		} else {
			writeStatus(w, http.StatusServiceUnavailable, reason)
		}
	})
	if opts.MetricsEnabled {
		mux.Handle(opts.MetricsPath, promhttp.HandlerFor(opts.Registry, promhttp.HandlerOpts{}))
	}

	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("metrics: listen on %q: %w", opts.Addr, err)
	}

	return &Server{
		httpSrv: &http.Server{
			Handler: mux,
			// Conservative timeouts on every phase: scrape/health responses are
			// tiny, so tight bounds are safe and they close the slowloris-style
			// resource-exhaustion surface on an endpoint that binds all interfaces
			// by default. ReadHeaderTimeout also guards the header phase.
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		ln:   ln,
		opts: opts,
	}, nil
}

// Addr returns the bound address (resolves ":0" to a concrete port).
func (s *Server) Addr() string { return s.ln.Addr().String() }

// Serve blocks serving HTTP until the listener is closed (via Shutdown). A
// clean shutdown returns nil.
func (s *Server) Serve() error {
	err := s.httpSrv.Serve(s.ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the server within ctx's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

func writeStatus(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// Static, secret-free body.
	_, _ = fmt.Fprintf(w, "{%q:%q}\n", "status", msg)
}
