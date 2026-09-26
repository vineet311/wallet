package platform

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Ops is a small HTTP server on its own port (OPS_PORT, default 9090).
// It serves only:
//
//	/healthz  liveness: the process is running
//	/readyz   readiness: every dependency check passes and we are not shutting down
//	/metrics  Prometheus metrics
//
// It is separate from the main port so it is never exposed to the internet.
type Ops struct {
	mu       sync.Mutex
	checks   map[string]func(context.Context) error
	draining atomic.Bool
	server   *http.Server
}

func NewOps() *Ops {
	return &Ops{checks: map[string]func(context.Context) error{}}
}

// AddCheck registers a dependency check that /readyz runs.
func (o *Ops) AddCheck(name string, check func(context.Context) error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.checks[name] = check
}

// SetDraining makes /readyz return 503, so Kubernetes stops sending traffic.
func (o *Ops) SetDraining() {
	o.draining.Store(true)
}

func (o *Ops) Start(log *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", o.readyz)
	mux.Handle("GET /metrics", promhttp.Handler())

	addr := ":" + EnvOr("OPS_PORT", "9090")
	o.server = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := o.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server failed", "err", err)
		}
	}()
	log.Info("ops server listening", "addr", addr)
}

func (o *Ops) readyz(w http.ResponseWriter, r *http.Request) {
	if o.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "draining"})
		return
	}
	o.mu.Lock()
	checks := make(map[string]func(context.Context) error, len(o.checks))
	for name, check := range o.checks {
		checks[name] = check
	}
	o.mu.Unlock()

	results := map[string]string{}
	ok := true
	for name, check := range checks {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := check(ctx)
		cancel()
		if err != nil {
			ok = false
			results[name] = "fail: " + err.Error()
		} else {
			results[name] = "ok"
		}
	}
	code, status := http.StatusOK, "ok"
	if !ok {
		code, status = http.StatusServiceUnavailable, "not_ready"
	}
	writeJSON(w, code, map[string]any{"status": status, "checks": results})
}

func (o *Ops) Shutdown(ctx context.Context) error {
	if o.server == nil {
		return nil
	}
	return o.server.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}