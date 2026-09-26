package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests by method, route and status.",
	}, []string{"method", "route", "status"})

	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})
)

// statusRecorder remembers the status code a handler wrote, for logs and metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// observe wraps the whole router: request id, access log, and metrics for every request.
// The route label is the pattern ("GET /transfers/{id}"), never the raw URL,
// so metrics don't get one series per transfer id.
func observe(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" {
			reqID = newRequestID()
		}
		w.Header().Set("X-Request-Id", reqID)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		route := r.Pattern // set by ServeMux once it matched a route
		if route == "" || route == "/" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
		httpDuration.WithLabelValues(r.Method, route).Observe(elapsed.Seconds())
		log.Info("request", "request_id", reqID, "method", r.Method, "route", route,
			"status", rec.status, "ms", elapsed.Milliseconds())
	})
}

func newRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// clientIP is the address used for the per-IP rate limit on login and register.
// Behind a load balancer every request comes from the balancer's IP, so when
// TRUST_PROXY=true we take the first address in X-Forwarded-For instead.
// Only turn that on behind a proxy you control: clients can fake the header.
func clientIP(trustProxy bool) func(*http.Request) string {
	return func(r *http.Request) string {
		if trustProxy {
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				return strings.TrimSpace(strings.Split(xff, ",")[0])
			}
		}
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return r.RemoteAddr
		}
		return host
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// readJSON decodes a small JSON body. Bodies over 10KB are rejected.
func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 10<<10)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}