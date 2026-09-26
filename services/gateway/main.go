// The gateway is the only service the outside world talks to.
// It handles sign-up, login (JWT), rate limiting, and the REST API,
// and calls the wallet service over gRPC.
// It serves HTTP on PORT (default 8080) and ops endpoints on OPS_PORT (default 9090).
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Utkarsh-262003/wallet-ledger/internal/platform"
)

func main() {
	log := platform.NewLogger("gateway")
	dsn := platform.MustEnv("DATABASE_URL")
	redisURL := platform.MustEnv("REDIS_URL")
	walletAddr := platform.MustEnv("WALLET_GRPC_ADDR")
	jwtSecret := platform.MustEnv("JWT_SECRET")
	port := platform.EnvOr("PORT", "8080")

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Error("invalid DATABASE_URL", "err", err)
		os.Exit(1)
	}
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Error("invalid REDIS_URL", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(redisOpts)

	walletClient, walletConn, err := newWalletClient(walletAddr)
	if err != nil {
		log.Error("invalid WALLET_GRPC_ADDR", "err", err)
		os.Exit(1)
	}

	ops := platform.NewOps()
	ops.AddCheck("postgres", pool.Ping)
	// Redis is not a readiness check: the rate limiter fails open without it.
	ops.Start(log)

	a := &api{
		db:          pool,
		wallet:      walletClient,
		tokens:      tokenIssuer{secret: []byte(jwtSecret), ttl: platform.EnvDuration("JWT_TTL", 15*time.Minute)},
		log:         log,
		callTimeout: platform.EnvDuration("WALLET_CALL_TIMEOUT", 3*time.Second),
	}
	rl := rateLimiter{rdb: rdb, log: log}
	byIP := clientIP(platform.EnvOr("TRUST_PROXY", "false") == "true")
	byUser := func(r *http.Request) string { return userID(r) }
	authLimit := platform.EnvInt("RATE_LIMIT_AUTH_PER_MINUTE", 10)
	userLimit := platform.EnvInt("RATE_LIMIT_PER_MINUTE", 60)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/register", rl.limit("auth", authLimit, byIP, a.register))
	mux.HandleFunc("POST /auth/login", rl.limit("auth", authLimit, byIP, a.login))
	mux.HandleFunc("GET /wallets/me", a.requireAuth(rl.limit("user", userLimit, byUser, a.getMyWallet)))
	mux.HandleFunc("POST /wallets/me/deposits", a.requireAuth(rl.limit("user", userLimit, byUser, a.deposit)))
	mux.HandleFunc("POST /transfers", a.requireAuth(rl.limit("user", userLimit, byUser, a.transfer)))
	mux.HandleFunc("GET /transfers/{id}", a.requireAuth(rl.limit("user", userLimit, byUser, a.getTransfer)))
	// Anything else: a JSON 404 instead of Go's plain-text default.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           observe(log, mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()
	log.Info("gateway listening", "port", port, "wallet", walletAddr)

	sig := platform.WaitForSignal()
	platform.BeginShutdown(ops, log, sig)

	// Stop taking requests (finishing the ones in flight), then close connections.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	_ = walletConn.Close()
	_ = rdb.Close()
	pool.Close()
	_ = ops.Shutdown(ctx)
	log.Info("shutdown complete")
}