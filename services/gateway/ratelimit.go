package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

var rateLimited = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "gateway_rate_limited_total",
	Help: "Requests rejected by the rate limiter.",
}, []string{"bucket"})

// rateLimiter is a fixed-window limit in Redis: one counter per key per minute.
// Redis holds the counters, so the limit is shared across all gateway pods.
//
// If Redis is down we FAIL OPEN: the request goes through and we log it.
// Choice: a Redis outage should not take the whole API down with it.
type rateLimiter struct {
	rdb *redis.Client
	log *slog.Logger
}

func (rl rateLimiter) limit(bucket string, perMinute int, keyFn func(*http.Request) string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		window := now.Unix() / 60
		key := fmt.Sprintf("rl:%s:%s:%d", bucket, keyFn(r), window)

		ctx, cancel := context.WithTimeout(r.Context(), 200*time.Millisecond)
		defer cancel()
		pipe := rl.rdb.TxPipeline()
		incr := pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, 70*time.Second)
		if _, err := pipe.Exec(ctx); err != nil {
			rl.log.Warn("rate limiter unavailable, allowing request", "err", err)
			next(w, r)
			return
		}

		count := int(incr.Val())
		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(perMinute))
		w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(max(0, perMinute-count)))
		if count > perMinute {
			rateLimited.WithLabelValues(bucket).Inc()
			w.Header().Set("Retry-After", strconv.Itoa(60-int(now.Unix()%60)))
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r)
	}
}