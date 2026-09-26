package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	walletv1 "github.com/Utkarsh-262003/wallet-ledger/gen/wallet/v1"
)

// walletServer implements the gRPC service defined in proto/wallet/v1/wallet.proto.
type walletServer struct {
	walletv1.UnimplementedWalletServiceServer
	store *Store
	log   *slog.Logger
}

func (s *walletServer) CreateWallet(ctx context.Context, req *walletv1.CreateWalletRequest) (*walletv1.CreateWalletResponse, error) {
	userID, err := parseUUID(req.GetUserId(), "user_id")
	if err != nil {
		return nil, err
	}
	w, err := s.store.CreateWallet(ctx, userID)
	if err != nil {
		return nil, s.toStatus("create wallet", err)
	}
	return &walletv1.CreateWalletResponse{Wallet: w.toProto()}, nil
}

func (s *walletServer) GetWallet(ctx context.Context, req *walletv1.GetWalletRequest) (*walletv1.GetWalletResponse, error) {
	userID, err := parseUUID(req.GetUserId(), "user_id")
	if err != nil {
		return nil, err
	}
	w, err := s.store.GetWalletByUser(ctx, userID)
	if err != nil {
		return nil, s.toStatus("get wallet", err)
	}
	return &walletv1.GetWalletResponse{Wallet: w.toProto()}, nil
}

func (s *walletServer) Deposit(ctx context.Context, req *walletv1.DepositRequest) (*walletv1.DepositResponse, error) {
	userID, err := parseUUID(req.GetUserId(), "user_id")
	if err != nil {
		return nil, err
	}
	if err := validateMoney(req.GetAmountCents(), req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	res, err := s.store.Deposit(ctx, userID, req.GetAmountCents(), req.GetIdempotencyKey())
	recordResult("DEPOSIT", res, err)
	if err != nil {
		return nil, s.toStatus("deposit", err)
	}
	return &walletv1.DepositResponse{Transaction: res.Tx.toProto()}, nil
}

func (s *walletServer) Transfer(ctx context.Context, req *walletv1.TransferRequest) (*walletv1.TransferResponse, error) {
	userID, err := parseUUID(req.GetUserId(), "user_id")
	if err != nil {
		return nil, err
	}
	toWalletID, err := parseUUID(req.GetToWalletId(), "to_wallet_id")
	if err != nil {
		return nil, err
	}
	if err := validateMoney(req.GetAmountCents(), req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	res, err := s.store.Transfer(ctx, userID, toWalletID, req.GetAmountCents(), req.GetIdempotencyKey())
	recordResult("TRANSFER", res, err)
	if err != nil {
		return nil, s.toStatus("transfer", err)
	}
	return &walletv1.TransferResponse{Transaction: res.Tx.toProto()}, nil
}

func (s *walletServer) GetTransaction(ctx context.Context, req *walletv1.GetTransactionRequest) (*walletv1.GetTransactionResponse, error) {
	userID, err := parseUUID(req.GetUserId(), "user_id")
	if err != nil {
		return nil, err
	}
	txID, err := parseUUID(req.GetTransactionId(), "transaction_id")
	if err != nil {
		return nil, err
	}
	t, err := s.store.GetTransaction(ctx, userID, txID)
	if err != nil {
		return nil, s.toStatus("get transaction", err)
	}
	return &walletv1.GetTransactionResponse{Transaction: t.toProto()}, nil
}

// toStatus turns a store error into a gRPC status.
// Business errors get a clear code and message. Anything else is logged here
// and the caller only sees "internal error", so database details never leak.
func (s *walletServer) toStatus(op string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrSameWallet), errors.Is(err, ErrCurrencyMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrTooMuchContention):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request cancelled or timed out")
	}
	s.log.Error("internal error", "op", op, "err", err)
	return status.Error(codes.Internal, "internal error")
}

// parseUUID checks the format and returns the lowercase canonical form,
// so "ABC..." and "abc..." are treated as the same id.
func parseUUID(value, field string) (string, error) {
	u, err := uuid.Parse(value)
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "%s must be a UUID", field)
	}
	return u.String(), nil
}

func validateMoney(amount int64, key string) error {
	if amount <= 0 {
		return status.Error(codes.InvalidArgument, "amount_cents must be a positive integer")
	}
	if len(key) == 0 || len(key) > 200 {
		return status.Error(codes.InvalidArgument, "idempotency_key is required (1-200 characters)")
	}
	return nil
}

func (w Wallet) toProto() *walletv1.Wallet {
	return &walletv1.Wallet{
		Id:           w.ID,
		UserId:       w.UserID,
		BalanceCents: w.BalanceCents,
		Currency:     w.Currency,
		Version:      w.Version,
		CreatedAt:    w.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func (t Transaction) toProto() *walletv1.Transaction {
	return &walletv1.Transaction{
		Id:             t.ID,
		Kind:           t.Kind,
		FromWalletId:   t.FromWalletID,
		ToWalletId:     t.ToWalletID,
		AmountCents:    t.AmountCents,
		Status:         t.Status,
		IdempotencyKey: t.IdempotencyKey,
		CreatedAt:      t.CreatedAt.UTC().Format(time.RFC3339),
	}
}