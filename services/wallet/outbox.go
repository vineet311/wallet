package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// Relay is the outbox relay.
// Every poll it takes a batch of unsent outbox rows, publishes them to Kafka,
// and marks them sent, all inside one database transaction.
//
// FOR UPDATE SKIP LOCKED lets several wallet pods run the relay at once:
// each pod takes rows the others are not holding.
//
// Delivery is "at least once". If the pod dies after Kafka accepted a batch but
// before the COMMIT, those rows are sent again later. That is why every consumer
// (ledger, fraud, notification) must skip events it has already handled.
type Relay struct {
	pool      *pgxpool.Pool
	writer    *kafka.Writer
	log       *slog.Logger
	poll      time.Duration
	batchSize int
	retention time.Duration

	cancel context.CancelFunc
	done   chan struct{}
}

type outboxRow struct {
	id      int64
	topic   string
	key     string
	payload []byte
}

func (r *Relay) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	go r.loop(ctx)
	go r.housekeeping(ctx)
	r.log.Info("outbox relay started", "poll", r.poll.String(), "batch", r.batchSize)
}

// Stop finishes the batch in progress, then returns.
func (r *Relay) Stop() {
	r.cancel()
	<-r.done
}

func (r *Relay) loop(ctx context.Context) {
	defer close(r.done)
	backoff := r.poll
	// A batch that has started runs to the end even if Stop is called,
	// so shutdown does not cut a batch off between the Kafka write and the COMMIT.
	batchCtx := context.WithoutCancel(ctx)
	for ctx.Err() == nil {
		n, err := r.publishBatch(batchCtx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			outboxFailures.Inc()
			r.log.Error("outbox publish failed, will retry", "err", err, "retry_in", backoff.String())
			sleep(ctx, backoff)
			backoff = min(backoff*2, 10*time.Second)
			continue
		}
		backoff = r.poll
		if n < r.batchSize {
			sleep(ctx, r.poll) // caught up; wait before polling again
		}
	}
}

func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background()) // no-op after a successful Commit

	rows, err := tx.Query(ctx,
		`SELECT id, topic, message_key, payload
		   FROM wallet.outbox
		  WHERE published_at IS NULL
		  ORDER BY id
		  LIMIT $1
		  FOR UPDATE SKIP LOCKED`, r.batchSize)
	if err != nil {
		return 0, err
	}
	var batch []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.id, &row.topic, &row.key, &row.payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, tx.Commit(ctx)
	}

	msgs := make([]kafka.Message, len(batch))
	ids := make([]int64, len(batch))
	for i, row := range batch {
		msgs[i] = kafka.Message{Topic: row.topic, Key: []byte(row.key), Value: row.payload}
		ids[i] = row.id
	}

	// If Kafka is down this fails after a timeout, the transaction rolls back,
	// and the rows stay unsent until the next try.
	writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := r.writer.WriteMessages(writeCtx, msgs...); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE wallet.outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	for _, row := range batch {
		outboxPublished.WithLabelValues(row.topic).Inc()
	}
	return len(batch), nil
}

// housekeeping refreshes the backlog gauges every 10s and deletes old sent rows every hour.
func (r *Relay) housekeeping(ctx context.Context) {
	gauges := time.NewTicker(10 * time.Second)
	cleanup := time.NewTicker(time.Hour)
	defer gauges.Stop()
	defer cleanup.Stop()
	r.refreshGauges(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-gauges.C:
			r.refreshGauges(ctx)
		case <-cleanup.C:
			tag, err := r.pool.Exec(ctx,
				`DELETE FROM wallet.outbox
				  WHERE published_at IS NOT NULL AND published_at < now() - make_interval(hours => $1)`,
				int(r.retention.Hours()))
			if err != nil {
				r.log.Warn("outbox cleanup failed", "err", err)
			} else if tag.RowsAffected() > 0 {
				r.log.Info("old outbox rows removed", "deleted", tag.RowsAffected())
			}
		}
	}
}

func (r *Relay) refreshGauges(ctx context.Context) {
	var pending int64
	var oldest float64
	err := r.pool.QueryRow(ctx,
		`SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8
		   FROM wallet.outbox WHERE published_at IS NULL`).Scan(&pending, &oldest)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("could not refresh outbox gauges", "err", err)
		}
		return
	}
	outboxPending.Set(float64(pending))
	outboxOldest.Set(oldest)
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}