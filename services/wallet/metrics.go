package main

import (
	"context"
	"errors"
	"path"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	grpcRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_grpc_requests_total",
		Help: "gRPC calls by method and status code.",
	}, []string{"method", "code"})

	grpcDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wallet_grpc_duration_seconds",
		Help:    "gRPC call duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	transactionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_transactions_total",
		Help: "Deposits and transfers by result: ok, replayed, rejected, error.",
	}, []string{"kind", "result"})

	lockRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_optimistic_lock_retries_total",
		Help: "Times a balance update lost the optimistic-lock race and retried.",
	}, []string{"kind"})

	outboxPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wallet_outbox_published_total",
		Help: "Outbox events published to Kafka.",
	}, []string{"topic"})

	outboxFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wallet_outbox_publish_failures_total",
		Help: "Outbox batches that failed to publish and will be retried.",
	})

	outboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_outbox_pending",
		Help: "Outbox events not yet published.",
	})

	outboxOldest = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "wallet_outbox_oldest_pending_seconds",
		Help: "Age of the oldest unpublished event. If this keeps growing, Kafka is unreachable.",
	})
)

// metricsInterceptor runs around every gRPC call.
// It times the call and counts it by method and result code.
func metricsInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	method := path.Base(info.FullMethod) // "/wallet.v1.WalletService/Transfer" -> "Transfer"
	grpcRequests.WithLabelValues(method, status.Code(err).String()).Inc()
	grpcDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
	return resp, err
}

func recordResult(kind string, res Result, err error) {
	result := "ok"
	switch {
	case err == nil && res.Replayed:
		result = "replayed"
	case err != nil && isBusinessError(err):
		result = "rejected"
	case err != nil:
		result = "error"
	}
	transactionsTotal.WithLabelValues(kind, result).Inc()
}

func isBusinessError(err error) bool {
	for _, e := range []error{ErrNotFound, ErrInsufficientFunds, ErrSameWallet,
		ErrCurrencyMismatch, ErrIdempotencyConflict, ErrTooMuchContention} {
		if errors.Is(err, e) {
			return true
		}
	}
	return false
}