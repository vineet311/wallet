package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store holds all the SQL the wallet service runs.
type Store struct {
	pool       *pgxpool.Pool
	maxRetries int
}

// Business errors. server.go turns each one into a gRPC status code.
var (
	ErrNotFound            = errors.New("not found")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrSameWallet          = errors.New("cannot transfer to your own wallet")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrIdempotencyConflict = errors.New("idempotency_key was already used for a different request")
	ErrTooMuchContention   = errors.New("too much contention on this wallet, retry the request")
)

// errVersionConflict means another request changed the wallet between our read and our update.
// It never leaves this file: withRetries catches it and tries again.
var errVersionConflict = errors.New("version conflict")

// Kafka topics this service writes to (through the outbox).
const (
	topicTransfers = "transfers.completed"
	topicDeposits  = "wallet.deposited"
)

type Wallet struct {
	ID           string
	UserID       string
	BalanceCents int64
	Currency     string
	Version      int32
	CreatedAt    time.Time
}

type Transaction struct {
	ID             string
	Kind           string
	FromWalletID   string // empty for deposits
	ToWalletID     string
	AmountCents    int64
	Status         string
	IdempotencyKey string
	CreatedAt      time.Time
}

// The ::text casts turn UUIDs into plain strings as Postgres returns them.
const walletColumns = `id::text, user_id::text, balance_cents, currency, version, created_at`
const txColumns = `id::text, kind, COALESCE(from_wallet_id::text, ''), to_wallet_id::text,
	amount_cents, status, idempotency_key, created_at`

func scanWallet(row pgx.Row) (Wallet, error) {
	var w Wallet
	err := row.Scan(&w.ID, &w.UserID, &w.BalanceCents, &w.Currency, &w.Version, &w.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Wallet{}, ErrNotFound
	}
	return w, err
}

func scanTx(row pgx.Row) (Transaction, error) {
	var t Transaction
	err := row.Scan(&t.ID, &t.Kind, &t.FromWalletID, &t.ToWalletID,
		&t.AmountCents, &t.Status, &t.IdempotencyKey, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transaction{}, ErrNotFound
	}
	return t, err
}

// ---------- wallets ----------

// CreateWallet creates the user's wallet, or returns the existing one.
// Calling it twice for the same user is safe.
func (s *Store) CreateWallet(ctx context.Context, userID string) (Wallet, error) {
	// ON CONFLICT DO NOTHING would return no row when the wallet already exists.
	// A do-nothing UPDATE makes RETURNING hand back the existing row instead.
	return scanWallet(s.pool.QueryRow(ctx,
		`INSERT INTO wallet.wallets (user_id) VALUES ($1)
		 ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id
		 RETURNING `+walletColumns, userID))
}

func (s *Store) GetWalletByUser(ctx context.Context, userID string) (Wallet, error) {
	return scanWallet(s.pool.QueryRow(ctx,
		`SELECT `+walletColumns+` FROM wallet.wallets WHERE user_id = $1`, userID))
}

// ---------- money movement ----------

// Result of a deposit or transfer. Replayed is true when the idempotency key
// was seen before and we returned the original transaction without moving money.
type Result struct {
	Tx       Transaction
	Replayed bool
}

// Deposit adds money to the user's own wallet.
func (s *Store) Deposit(ctx context.Context, userID string, amount int64, key string) (Result, error) {
	return s.withRetries(ctx, "DEPOSIT", func(tx pgx.Tx) (Result, error) {
		existing, err := findByKey(ctx, tx, userID, key)
		if err == nil {
			if existing.Kind != "DEPOSIT" || existing.AmountCents != amount {
				return Result{}, ErrIdempotencyConflict
			}
			return Result{Tx: existing, Replayed: true}, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return Result{}, err
		}

		w, err := scanWallet(tx.QueryRow(ctx,
			`SELECT `+walletColumns+` FROM wallet.wallets WHERE user_id = $1`, userID))
		if err != nil {
			return Result{}, fmt.Errorf("wallet: %w", err)
		}
		if err := applyDelta(ctx, tx, w, amount); err != nil {
			return Result{}, err
		}

		t, err := scanTx(tx.QueryRow(ctx,
			`INSERT INTO wallet.transactions (user_id, kind, to_wallet_id, amount_cents, idempotency_key)
			 VALUES ($1, 'DEPOSIT', $2, $3, $4) RETURNING `+txColumns,
			userID, w.ID, amount, key))
		if err != nil {
			return Result{}, err
		}

		err = insertOutbox(ctx, tx, topicDeposits, w.ID, depositEvent{
			EventID:       t.ID,
			Type:          "DepositCompleted",
			TransactionID: t.ID,
			WalletID:      w.ID,
			UserID:        userID,
			AmountCents:   amount,
			Currency:      w.Currency,
			OccurredAt:    t.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
		return Result{Tx: t}, err
	})
}

// Transfer moves money from the user's wallet to another wallet.
func (s *Store) Transfer(ctx context.Context, userID, toWalletID string, amount int64, key string) (Result, error) {
	return s.withRetries(ctx, "TRANSFER", func(tx pgx.Tx) (Result, error) {
		existing, err := findByKey(ctx, tx, userID, key)
		if err == nil {
			if existing.Kind != "TRANSFER" || existing.AmountCents != amount || existing.ToWalletID != toWalletID {
				return Result{}, ErrIdempotencyConflict
			}
			return Result{Tx: existing, Replayed: true}, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return Result{}, err
		}

		from, err := scanWallet(tx.QueryRow(ctx,
			`SELECT `+walletColumns+` FROM wallet.wallets WHERE user_id = $1`, userID))
		if err != nil {
			return Result{}, fmt.Errorf("sender wallet: %w", err)
		}
		to, err := scanWallet(tx.QueryRow(ctx,
			`SELECT `+walletColumns+` FROM wallet.wallets WHERE id = $1`, toWalletID))
		if err != nil {
			return Result{}, fmt.Errorf("recipient wallet: %w", err)
		}
		if from.ID == to.ID {
			return Result{}, ErrSameWallet
		}
		if from.Currency != to.Currency {
			return Result{}, ErrCurrencyMismatch
		}
		if from.BalanceCents < amount {
			return Result{}, ErrInsufficientFunds
		}

		// Always update the two wallets in the same order (by id).
		// If A->B and B->A run at the same moment and each locks its sender first,
		// they wait on each other forever (a deadlock). A fixed order means one simply waits.
		first, second := from, to
		firstDelta, secondDelta := -amount, amount
		if to.ID < from.ID {
			first, second = to, from
			firstDelta, secondDelta = amount, -amount
		}
		if err := applyDelta(ctx, tx, first, firstDelta); err != nil {
			return Result{}, err
		}
		if err := applyDelta(ctx, tx, second, secondDelta); err != nil {
			return Result{}, err
		}

		t, err := scanTx(tx.QueryRow(ctx,
			`INSERT INTO wallet.transactions (user_id, kind, from_wallet_id, to_wallet_id, amount_cents, idempotency_key)
			 VALUES ($1, 'TRANSFER', $2, $3, $4, $5) RETURNING `+txColumns,
			userID, from.ID, to.ID, amount, key))
		if err != nil {
			return Result{}, err
		}

		err = insertOutbox(ctx, tx, topicTransfers, from.ID, transferEvent{
			EventID:       t.ID,
			Type:          "TransferCompleted",
			TransactionID: t.ID,
			FromWalletID:  from.ID,
			ToWalletID:    to.ID,
			FromUserID:    from.UserID,
			ToUserID:      to.UserID,
			AmountCents:   amount,
			Currency:      from.Currency,
			OccurredAt:    t.CreatedAt.UTC().Format(time.RFC3339Nano),
		})
		return Result{Tx: t}, err
	})
}

// GetTransaction returns a transaction if one of the user's wallets is on either side of it.
func (s *Store) GetTransaction(ctx context.Context, userID, txID string) (Transaction, error) {
	return scanTx(s.pool.QueryRow(ctx,
		`SELECT t.id::text, t.kind, COALESCE(t.from_wallet_id::text, ''), t.to_wallet_id::text,
		        t.amount_cents, t.status, t.idempotency_key, t.created_at
		   FROM wallet.transactions t
		   JOIN wallet.wallets w ON w.user_id = $1
		  WHERE t.id = $2 AND (t.from_wallet_id = w.id OR t.to_wallet_id = w.id)`,
		userID, txID))
}

// ---------- helpers ----------

func findByKey(ctx context.Context, tx pgx.Tx, userID, key string) (Transaction, error) {
	return scanTx(tx.QueryRow(ctx,
		`SELECT `+txColumns+` FROM wallet.transactions WHERE user_id = $1 AND idempotency_key = $2`,
		userID, key))
}

// applyDelta changes a balance using optimistic locking: the UPDATE only matches
// if the version is still the one we read. Zero rows means someone else got there first.
func applyDelta(ctx context.Context, tx pgx.Tx, w Wallet, delta int64) error {
	tag, err := tx.Exec(ctx,
		`UPDATE wallet.wallets
		    SET balance_cents = balance_cents + $2, version = version + 1, updated_at = now()
		  WHERE id = $1 AND version = $3`,
		w.ID, delta, w.Version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errVersionConflict
	}
	return nil
}

// insertOutbox records an event in the same transaction as the balance change.
// pgx turns the event struct into JSON for the jsonb column.
func insertOutbox(ctx context.Context, tx pgx.Tx, topic, key string, event any) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO wallet.outbox (topic, message_key, payload) VALUES ($1, $2, $3)`,
		topic, key, event)
	return err
}

// withRetries runs fn in a database transaction. If we lose an optimistic-lock race,
// or two requests with the same idempotency key collide (unique violation 23505),
// it rolls back and tries again with exponential backoff and jitter.
func (s *Store) withRetries(ctx context.Context, kind string, fn func(pgx.Tx) (Result, error)) (Result, error) {
	for attempt := 1; attempt <= s.maxRetries; attempt++ {
		var res Result
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			var err error
			res, err = fn(tx)
			return err
		})
		if err == nil {
			return res, nil
		}
		var pgErr *pgconn.PgError
		retryable := errors.Is(err, errVersionConflict) ||
			(errors.As(err, &pgErr) && pgErr.Code == "23505")
		if !retryable {
			return Result{}, err
		}
		lockRetries.WithLabelValues(kind).Inc()

		// Wait a random time up to 10ms, 20ms, 40ms ... capped at 500ms.
		// The randomness stops all the losers from retrying at the same instant.
		limit := min(500*time.Millisecond, 10*time.Millisecond<<attempt)
		select {
		case <-time.After(rand.N(limit)):
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	return Result{}, ErrTooMuchContention
}

// ---------- event payloads (the JSON the ledger, fraud and notification services read) ----------

type transferEvent struct {
	EventID       string `json:"eventId"`
	Type          string `json:"type"`
	TransactionID string `json:"transactionId"`
	FromWalletID  string `json:"fromWalletId"`
	ToWalletID    string `json:"toWalletId"`
	FromUserID    string `json:"fromUserId"`
	ToUserID      string `json:"toUserId"`
	AmountCents   int64  `json:"amountCents"`
	Currency      string `json:"currency"`
	OccurredAt    string `json:"occurredAt"`
}

type depositEvent struct {
	EventID       string `json:"eventId"`
	Type          string `json:"type"`
	TransactionID string `json:"transactionId"`
	WalletID      string `json:"walletId"`
	UserID        string `json:"userId"`
	AmountCents   int64  `json:"amountCents"`
	Currency      string `json:"currency"`
	OccurredAt    string `json:"occurredAt"`
}