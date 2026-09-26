package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/mail"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	walletv1 "github.com/Utkarsh-262003/wallet-ledger/gen/wallet/v1"
)

type api struct {
	db          *pgxpool.Pool
	wallet      walletv1.WalletServiceClient
	tokens      tokenIssuer
	log         *slog.Logger
	callTimeout time.Duration
}

type userIDKey struct{}

// requireAuth checks the "Authorization: Bearer <token>" header
// and puts the user id into the request context for the handler.
func (a *api) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || scheme != "Bearer" || token == "" {
			writeError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		userID, err := a.tokens.verify(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), userIDKey{}, userID)))
	}
}

func userID(r *http.Request) string {
	id, _ := r.Context().Value(userIDKey{}).(string)
	return id
}

// ---------- auth ----------

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (a *api) register(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !readJSON(w, r, &in) {
		return
	}
	email := strings.ToLower(strings.TrimSpace(in.Email))
	if _, err := mail.ParseAddress(email); err != nil || !strings.Contains(email, ".") {
		writeError(w, http.StatusBadRequest, "valid email required")
		return
	}
	if len(in.Password) < 8 || len(in.Password) > 72 {
		writeError(w, http.StatusBadRequest, "password must be 8 to 72 characters")
		return
	}
	hash, err := hashPassword(in.Password)
	if err != nil {
		a.internal(w, "hash password", err)
		return
	}

	var id string
	err = a.db.QueryRow(r.Context(),
		`INSERT INTO auth.users (email, password_hash) VALUES ($1, $2) RETURNING id::text`,
		email, hash).Scan(&id)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		writeError(w, http.StatusConflict, "email already registered")
		return
	}
	if err != nil {
		a.internal(w, "insert user", err)
		return
	}

	// Create the wallet now. If this fails, GET /wallets/me creates it later.
	walletID := ""
	ctx, cancel := callCtx(r.Context(), a.callTimeout)
	defer cancel()
	if res, err := a.wallet.CreateWallet(ctx, &walletv1.CreateWalletRequest{UserId: id}); err == nil {
		walletID = res.GetWallet().GetId()
	} else {
		a.log.Warn("wallet creation deferred", "user_id", id, "err", err)
	}
	writeJSON(w, http.StatusCreated, map[string]string{"userId": id, "walletId": walletID})
}

func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var in credentials
	if !readJSON(w, r, &in) {
		return
	}
	var id, hash string
	err := a.db.QueryRow(r.Context(),
		`SELECT id::text, password_hash FROM auth.users WHERE email = $1`,
		strings.ToLower(strings.TrimSpace(in.Email))).Scan(&id, &hash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		a.internal(w, "find user", err)
		return
	}
	// Same answer for "no such user" and "wrong password", so emails can't be probed.
	if err != nil || !checkPassword(hash, in.Password) {
		writeError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}
	token, err := a.tokens.issue(id)
	if err != nil {
		a.internal(w, "issue token", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accessToken": token,
		"tokenType":   "Bearer",
		"expiresIn":   int(a.tokens.ttl.Seconds()),
	})
}

// ---------- wallet ----------

func (a *api) getMyWallet(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := callCtx(r.Context(), a.callTimeout)
	defer cancel()
	res, err := a.wallet.GetWallet(ctx, &walletv1.GetWalletRequest{UserId: userID(r)})
	if status.Code(err) == codes.NotFound {
		// Self-healing: the wallet should exist since sign-up, but if that call failed, create it now.
		var created *walletv1.CreateWalletResponse
		created, err = a.wallet.CreateWallet(ctx, &walletv1.CreateWalletRequest{UserId: userID(r)})
		if err == nil {
			writeJSON(w, http.StatusOK, toWalletJSON(created.GetWallet()))
			return
		}
	}
	if err != nil {
		a.walletError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletJSON(res.GetWallet()))
}

func (a *api) deposit(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	var in struct {
		AmountCents int64 `json:"amountCents"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	ctx, cancel := callCtx(r.Context(), a.callTimeout)
	defer cancel()
	res, err := a.wallet.Deposit(ctx, &walletv1.DepositRequest{
		UserId: userID(r), AmountCents: in.AmountCents, IdempotencyKey: key,
	})
	if err != nil {
		a.walletError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toTransactionJSON(res.GetTransaction()))
}

func (a *api) transfer(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key header is required")
		return
	}
	var in struct {
		ToWalletID  string `json:"toWalletId"`
		AmountCents int64  `json:"amountCents"`
	}
	if !readJSON(w, r, &in) {
		return
	}
	ctx, cancel := callCtx(r.Context(), a.callTimeout)
	defer cancel()
	res, err := a.wallet.Transfer(ctx, &walletv1.TransferRequest{
		UserId: userID(r), ToWalletId: in.ToWalletID, AmountCents: in.AmountCents, IdempotencyKey: key,
	})
	if err != nil {
		a.walletError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toTransactionJSON(res.GetTransaction()))
}

func (a *api) getTransfer(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := callCtx(r.Context(), a.callTimeout)
	defer cancel()
	res, err := a.wallet.GetTransaction(ctx, &walletv1.GetTransactionRequest{
		UserId: userID(r), TransactionId: r.PathValue("id"),
	})
	if err != nil {
		a.walletError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionJSON(res.GetTransaction()))
}

// ---------- errors ----------

// walletError passes the wallet's business message through (it is written for users),
// but hides the details of 5xx errors.
func (a *api) walletError(w http.ResponseWriter, err error) {
	code := httpStatusFor(err)
	if code >= 500 {
		a.log.Error("wallet call failed", "grpc_code", status.Code(err).String(), "err", err)
		writeError(w, code, "wallet service unavailable")
		return
	}
	writeError(w, code, status.Convert(err).Message())
}

func (a *api) internal(w http.ResponseWriter, op string, err error) {
	a.log.Error("internal error", "op", op, "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}