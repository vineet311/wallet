// The wallet service holds balances and moves money.
// It serves gRPC on GRPC_PORT (default 50051) and ops endpoints on OPS_PORT (default 9090).
package main

import (
	"context"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"

	walletv1 "github.com/Utkarsh-262003/wallet-ledger/gen/wallet/v1"
	"github.com/Utkarsh-262003/wallet-ledger/internal/platform"
)

func main() {
	log := platform.NewLogger("wallet")
	dsn := platform.MustEnv("DATABASE_URL")
	brokers := strings.Split(platform.MustEnv("KAFKA_BROKERS"), ",")
	grpcPort := platform.EnvOr("GRPC_PORT", "50051")

	// pgxpool.New checks the URL but does not connect yet.
	// The readiness check below is what proves the database is reachable.
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		log.Error("invalid DATABASE_URL", "err", err)
		os.Exit(1)
	}

	ops := platform.NewOps()
	ops.AddCheck("postgres", pool.Ping)
	// Kafka is deliberately NOT a readiness check. If Kafka is down,
	// transfers still succeed and wait in the outbox until it comes back.
	ops.Start(log)

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{}, // same key (wallet id) -> same partition -> events stay in order per wallet
		RequiredAcks:           kafka.RequireAll,
		AllowAutoTopicCreation: false,
		// kafka-go waits up to BatchTimeout (default 1s!) to fill a batch before sending.
		// We hand it a full batch ourselves, so a short timeout keeps events fast.
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}

	relay := &Relay{
		pool:      pool,
		writer:    writer,
		log:       log,
		poll:      platform.EnvDuration("OUTBOX_POLL_INTERVAL", 200*time.Millisecond),
		batchSize: platform.EnvInt("OUTBOX_BATCH_SIZE", 100),
		retention: platform.EnvDuration("OUTBOX_RETENTION", 7*24*time.Hour),
	}
	relay.Start()

	store := &Store{pool: pool, maxRetries: platform.EnvInt("OPTIMISTIC_LOCK_RETRIES", 10)}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(metricsInterceptor))
	walletv1.RegisterWalletServiceServer(grpcServer, &walletServer{store: store, log: log})

	lis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Error("cannot listen", "port", grpcPort, "err", err)
		os.Exit(1)
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("grpc server stopped", "err", err)
			os.Exit(1)
		}
	}()
	log.Info("wallet gRPC server listening", "port", grpcPort)

	sig := platform.WaitForSignal()
	platform.BeginShutdown(ops, log, sig)

	// Stop in order: stop taking RPCs (finishing the ones in flight),
	// let the relay finish its batch, then close connections.
	grpcServer.GracefulStop()
	relay.Stop()
	_ = writer.Close()
	pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ops.Shutdown(ctx)
	log.Info("shutdown complete")
}