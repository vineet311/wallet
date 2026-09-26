package main

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	walletv1 "github.com/Utkarsh-262003/wallet-ledger/gen/wallet/v1"
)

// newWalletClient connects to the wallet service over gRPC.
//
// Kubernetes gotcha: gRPC keeps ONE long-lived HTTP/2 connection open.
// A normal ClusterIP Service balances connections, not requests, so every call
// from this pod would land on the same wallet pod. Two fixes:
//  1. point WALLET_GRPC_ADDR at a headless Service, e.g. dns:///wallet-headless:50051,
//     and the round_robin policy below spreads calls across all wallet pods
//  2. let a service mesh (Istio, Linkerd) balance per request
//
// Plaintext on purpose: mTLS between pods is the mesh's job.
func newWalletClient(addr string) (walletv1.WalletServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin": {}}]}`),
	)
	if err != nil {
		return nil, nil, err
	}
	return walletv1.NewWalletServiceClient(conn), conn, nil
}

// callCtx gives every wallet call a deadline, so a stuck wallet can't hang the gateway.
func callCtx(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// httpStatusFor maps a gRPC status code from the wallet to an HTTP status code.
func httpStatusFor(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.FailedPrecondition:
		return http.StatusUnprocessableEntity
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}

// ---- JSON shapes returned to API clients (plain numbers, not protobuf's string int64) ----

type walletJSON struct {
	ID           string `json:"id"`
	UserID       string `json:"userId"`
	BalanceCents int64  `json:"balanceCents"`
	Currency     string `json:"currency"`
	Version      int32  `json:"version"`
	CreatedAt    string `json:"createdAt"`
}

type transactionJSON struct {
	ID             string `json:"id"`
	Kind           string `json:"kind"`
	FromWalletID   string `json:"fromWalletId,omitempty"`
	ToWalletID     string `json:"toWalletId"`
	AmountCents    int64  `json:"amountCents"`
	Status         string `json:"status"`
	IdempotencyKey string `json:"idempotencyKey"`
	CreatedAt      string `json:"createdAt"`
}

func toWalletJSON(w *walletv1.Wallet) walletJSON {
	return walletJSON{
		ID: w.GetId(), UserID: w.GetUserId(), BalanceCents: w.GetBalanceCents(),
		Currency: w.GetCurrency(), Version: w.GetVersion(), CreatedAt: w.GetCreatedAt(),
	}
}

func toTransactionJSON(t *walletv1.Transaction) transactionJSON {
	return transactionJSON{
		ID: t.GetId(), Kind: t.GetKind(), FromWalletID: t.GetFromWalletId(), ToWalletID: t.GetToWalletId(),
		AmountCents: t.GetAmountCents(), Status: t.GetStatus(), IdempotencyKey: t.GetIdempotencyKey(),
		CreatedAt: t.GetCreatedAt(),
	}
}