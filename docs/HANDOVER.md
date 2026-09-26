# Handover: wallet-ledger

This document is for deploying and operating the system. It covers the architecture,
how a payment flows, what each service needs from the platform, and how the blueprint's
Kubernetes scenarios map onto the code.

Related: `docs/decisions.md` (why things are built this way),
`docs/difficulties.md` (problems hit and how they were fixed).

---

## 1. Status

All five services are built, containerised, and tested end to end.

| Proven | How |
|---|---|
| Money moves exactly once | Idempotency keys; replayed requests return the original transaction |
| Balances stay correct under concurrency | 40 simultaneous transfers in both directions between two wallets |
| Ledger matches wallets to the cent | `scripts/reconcile.sql` returns zero rows after every test |
| Kafka outage loses nothing | Transfers succeed with Kafka stopped; events wait in the outbox and publish when it returns |
| Database outage loses nothing | Ledger retries forever instead of dead-lettering; catches up and reconciles to zero |
| Redis outage loses nothing | Fraud retries; gateway rate limiter fails open |
| Duplicates and garbage are handled | Duplicate events skipped by every consumer; malformed messages go to `<topic>.dlq` |
| Graceful shutdown | Every service drains on SIGTERM and exits cleanly |
| Whole system in containers | `docker compose up -d --build` then `scripts/smoke/smoke.js`: all checks pass |

---

## 2. Architecture

```mermaid
flowchart TB
    client[Client] -->|HTTPS REST| gw[gateway<br/>Go]
    client -->|WebSocket| notif[notification<br/>Node.js]
    gw -->|gRPC| wallet[wallet<br/>Go]
    gw --> pg[(PostgreSQL)]
    gw --> redis[(Redis)]
    wallet --> pg
    wallet -->|outbox relay| kafka{{Kafka}}
    kafka -->|transfers.completed<br/>wallet.deposited| ledger[ledger<br/>Java / Spring Boot]
    kafka -->|transfers.completed| fraud[fraud<br/>Python / FastAPI]
    fraud -->|fraud.alerts| kafka
    kafka -->|all three topics| notif
    ledger --> pg
    fraud --> redis
    notif --> redis
```

| Service | Language | Role | Ports (container) |
|---|---|---|---|
| gateway | Go | Sign-up, login (JWT), rate limiting, public REST API; calls wallet over gRPC | 8080 HTTP, 9090 ops |
| wallet | Go | Balances, deposits, transfers, optimistic locking, transactional outbox + relay | 50051 gRPC, 9090 ops |
| ledger | Java 21, Spring Boot 4 | Consumes money events, writes the append-only double-entry journal | 9090 ops only |
| fraud | Python 3.13, FastAPI | Consumes transfers, applies rules, publishes `fraud.alerts` | 9090 ops only |
| notification | Node.js 22 | WebSocket server; turns events into live user messages; mock webhook | 8084 WS, 9090 ops |

Only **gateway (8080)** and **notification (8084)** should be reachable from outside the cluster.

---

## 3. How one transfer flows

1. Client calls `POST /transfers` on the gateway with a JWT and an `Idempotency-Key` header.
2. Gateway verifies the JWT, applies the per-user rate limit (Redis), calls `Transfer` on the wallet over gRPC.
3. Wallet runs **one database transaction**: checks the key was not used, checks the balance,
   updates both wallets with a version check (optimistic locking, in a fixed order to avoid deadlocks),
   inserts the transaction row, inserts an **outbox** row. Commit.
4. Gateway returns 201. The money has moved; Kafka has not been touched.
5. The wallet's outbox relay (every 200ms) publishes unsent rows to Kafka and marks them sent.
6. Three consumer groups read the event independently:
   - **ledger** writes a journal entry: DEBIT sender wallet, CREDIT recipient wallet. A deferred
     database trigger rejects the commit if debits ≠ credits.
   - **fraud** checks amount and velocity; may publish to `fraud.alerts`.
   - **notification** publishes to Redis pub/sub; every notification pod pushes to its own connected users.

Delivery is **at-least-once** everywhere. Every consumer is idempotent (ledger: unique `event_id`;
fraud: Redis dedupe keys; notification: Redis `SET NX`).

---

## 4. Run locally

```bash
cp .env.example .env
sudo iptables-legacy -P FORWARD ACCEPT     # Codespaces only, after every start (see difficulties.md)
docker compose up -d --build
cd scripts/smoke && npm install && node smoke.js && cd ../..
docker compose exec -T postgres psql -U app -d wallet < scripts/reconcile.sql
```

Stop: `docker compose down`. Reset all data: `docker compose down -v`.

---

## 5. What each service needs from the platform

### Common to every service

| Item | Value |
|---|---|
| Liveness probe | `GET /healthz` on port 9090 |
| Readiness probe | `GET /readyz` on port 9090 (503 while a dependency is down or during shutdown) |
| Metrics | `GET /metrics` on port 9090 (Prometheus format) |
| Logs | JSON, one object per line, stdout |
| State | None in the process. Every service can run any number of replicas. |
| User | Non-root in every image |
| Shutdown | Handles SIGTERM; fits inside the default `terminationGracePeriodSeconds: 30` |

The ops port 9090 must **not** be exposed through the ingress.

### Environment variables

**wallet**

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | yes | | `postgres://user:pass@host:5432/wallet?sslmode=...` |
| `KAFKA_BROKERS` | yes | | comma-separated |
| `GRPC_PORT` | | 50051 | |
| `OPS_PORT` | | 9090 | |
| `OPTIMISTIC_LOCK_RETRIES` | | 10 | |
| `OUTBOX_POLL_INTERVAL` | | 200ms | |
| `OUTBOX_BATCH_SIZE` | | 100 | |
| `OUTBOX_RETENTION` | | 168h | sent outbox rows older than this are deleted hourly |
| `SHUTDOWN_DELAY` / `SHUTDOWN_TIMEOUT` | | 5s / 20s | |
| `LOG_LEVEL` | | info | `debug` for more |

**gateway**

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | yes | | same database (schema `auth`) |
| `REDIS_URL` | yes | | `redis://host:6379` |
| `WALLET_GRPC_ADDR` | yes | | see gRPC load balancing below |
| `JWT_SECRET` | yes | | **must equal notification's** |
| `PORT` | | 8080 | |
| `OPS_PORT` | | 9090 | |
| `JWT_TTL` | | 15m | |
| `WALLET_CALL_TIMEOUT` | | 3s | deadline on every gRPC call |
| `RATE_LIMIT_PER_MINUTE` | | 60 | per user |
| `RATE_LIMIT_AUTH_PER_MINUTE` | | 10 | per client IP, login and register |
| `TRUST_PROXY` | | false | set `true` behind the ingress so the real client IP comes from `X-Forwarded-For` |
| `SHUTDOWN_DELAY` / `SHUTDOWN_TIMEOUT` | | 5s / 20s | |

**ledger**

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | yes | | same `postgres://` format; converted to JDBC internally |
| `KAFKA_BROKERS` | yes | | |
| `OPS_PORT` | | 9090 | |
| `DB_POOL_MAX` | | 10 | |
| `LEDGER_CONCURRENCY` | | 3 | consumer threads per pod. **Set to 1 when scaling by pods with KEDA** (see §6) |

**fraud**

| Variable | Required | Default | Notes |
|---|---|---|---|
| `REDIS_URL` | yes | | |
| `KAFKA_BROKERS` | yes | | |
| `FRAUD_MAX_AMOUNT_CENTS` | | 1000000 | 10,000.00 |
| `FRAUD_MAX_PER_MINUTE` | | 5 | transfers per wallet per minute |
| `LOG_LEVEL` | | info | |

Ops port is fixed at 9090 by the image's command.

**notification**

| Variable | Required | Default | Notes |
|---|---|---|---|
| `JWT_SECRET` | yes | | **must equal gateway's** |
| `REDIS_URL` | yes | | |
| `KAFKA_BROKERS` | yes | | |
| `PORT` | | 8084 | WebSocket |
| `OPS_PORT` | | 9090 | |
| `WEBHOOK_URL` | | empty | mock webhook target; skipped when empty |
| `SHUTDOWN_DELAY_MS` | | 5000 | |

### Secrets (for Vault / External Secrets Operator)

| Secret | Used by |
|---|---|
| `JWT_SECRET` | gateway, notification (same value) |
| database password (inside `DATABASE_URL`) | gateway, wallet, ledger, migrations job |

No secret is baked into any image.

---

## 6. Kubernetes notes

**Migrations run before the apps.** Run Flyway as a Job (Helm pre-upgrade hook or an Argo CD
sync wave before the apps). SQL lives in `db/migrations`. Never edit an applied file; add a new one.
For zero-downtime changes use expand/contract: add the new column in release N, switch the code
in N+1, drop the old column in N+2.

**Topics are created explicitly** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`). Use
`scripts/create-topics.sh` as a Job with `REPLICATION=3`, or Strimzi `KafkaTopic` resources:

| Topic | Partitions | Producer | Consumer groups |
|---|---|---|---|
| `transfers.completed` | 16 | wallet | ledger, fraud, notification |
| `wallet.deposited` | 6 | wallet | ledger, notification |
| `fraud.alerts` | 3 | fraud | notification |
| `*.dlq` | 1 each | ledger, fraud, notification | none (inspect by hand) |

**gRPC load balancing.** gRPC keeps one long-lived connection, so a normal ClusterIP Service
sends every call from a gateway pod to one wallet pod. Either create a headless Service and set
`WALLET_GRPC_ADDR=dns:///wallet-headless:50051` (the client already uses `round_robin`), or let
the service mesh balance per request.

**mTLS between pods is the mesh's job.** Services talk plaintext and expect Istio or Cilium to encrypt.

**WebSockets.** The ingress/gateway must allow the `Upgrade` header on `/ws` and have an idle
timeout above 30s (the server pings every 30s). No sticky sessions needed: Redis pub/sub delivers
to whichever pod holds the user's socket. On shutdown clients get close code 1001 and should reconnect.

**Readiness semantics.** Wallet readiness checks only PostgreSQL. Kafka is deliberately not a
wallet dependency: if Kafka is down, payments still succeed and events wait in the outbox.
Consumers (ledger, fraud, notification) report not ready while Kafka is unreachable but never crash.

**Go images are distroless.** No shell, so `kubectl exec` has nothing to run. Debug with logs,
`/readyz` and `/metrics`, or an ephemeral debug container (`kubectl debug`).

---

## 7. Blueprint scenarios: where to hook in

**KEDA on Kafka lag (ledger 2 to 15 pods at lag > 500).** Scale on consumer group `ledger`,
topics `transfers.completed` and `wallet.deposited`, `lagThreshold: "500"`,
`minReplicaCount: 2`, `maxReplicaCount: 15`. Set `LEDGER_CONCURRENCY=1` so one pod is one consumer.
A group uses at most one consumer per partition, which is why `transfers.completed` has 16.
Generate lag with `scripts/smoke/load.js` (raise the gateway rate limits first).

**Canary with automated rollback (5xx > 1% over 3 min).** Gateway exposes
`http_requests_total{method,route,status}`. Error-rate query for an Argo Rollouts AnalysisTemplate:

```promql
sum(rate(http_requests_total{service="gateway",status=~"5.."}[3m]))
/
sum(rate(http_requests_total{service="gateway"}[3m]))
```

**Chaos (pod kills, latency, DB failover, prove zero data loss).** The proof is
`scripts/reconcile.sql`: after the experiment, once consumer lag is zero, it must return zero rows.
Expected behaviour during failures: wallet `/readyz` 503 while the DB is down; the ledger retries
without dead-lettering; outbox rows accumulate while Kafka is down and drain afterwards.

**Post-deploy check.** `scripts/smoke/smoke.js` exits 0 or 1. Point it at the environment with
`GATEWAY_URL`, `WS_URL` and `OPS_URLS` (set `OPS_URLS=""` if ops ports are not reachable from CI).

---

## 8. Observability

**Metrics worth dashboards and alerts:**

| Metric | Service | Alert idea |
|---|---|---|
| `up == 0` | all | target down for 2m |
| `http_requests_total` (5xx ratio) | gateway | > 1% for 5m |
| `wallet_outbox_oldest_pending_seconds` | wallet | > 60 and rising: Kafka unreachable |
| `wallet_outbox_pending` | wallet | growing steadily |
| `wallet_optimistic_lock_retries_total` | wallet | rate spike: a hot wallet |
| `ledger_entries_posted_total`, `ledger_duplicate_events_total` | ledger | posted rate drops to 0 while transfers continue |
| Kafka consumer lag (group `ledger`, `fraud`, `notification`) | Kafka exporter | lag > 500 for 5m |
| Messages on `*.dlq` topics | Kafka exporter | any message: someone must look |
| `fraud_alerts_total{rule}` | fraud | informational |
| `notification_ws_connections` | notification | informational |

**Logs:** JSON on stdout from every service, ready for Loki.

**Tracing: not implemented in code.** How to add it:
- Java: attach the OpenTelemetry Java agent (`JAVA_TOOL_OPTIONS=-javaagent:...`), no code change.
- Python: run under `opentelemetry-instrument`, no code change.
- Node: `node --require @opentelemetry/auto-instrumentations-node/register`, no code change.
- Go: add `otelgrpc` handlers on gateway and wallet. To keep one trace across Kafka, store the
  trace context in `wallet.outbox.headers` (the column exists) when writing the row, and send it
  as Kafka headers from the relay.

---

## 9. Security notes

- Every image runs as non-root. Go images are distroless (no shell, no package manager).
- Passwords are bcrypt-hashed. Login returns the same error for unknown email and wrong password.
- JWTs are HS256 with a shared secret, 15-minute lifetime. Upgrade path: RS256, so only the
  gateway holds the private key.
- All SQL uses parameters (no string building), so user input cannot become SQL.
- Internal errors are logged and never returned to clients.
- Rate limiter fails open on a Redis outage (availability over strictness, a conscious choice).
- Recommended in CI: Trivy on every image, Gitleaks on every commit, `buf lint` and `buf breaking` on the proto.

---

## 10. Known limitations

- No unit tests; coverage comes from the end-to-end smoke test and the manual failure tests above.
- No refresh tokens; clients log in again after 15 minutes.
- Single currency (USD).
- Fixed-window rate limiting (allows a burst at window edges).
- Notification dedupe keys live 1 hour in Redis.
- No CI pipeline, Kubernetes manifests, or Helm charts in this repo (platform side).