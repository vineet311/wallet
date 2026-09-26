# Design decisions

Each entry: what we chose, and why.

## Money is stored as integer cents
Amounts are whole numbers of the smallest unit (cents). Never floats.
Why: floats round, and rounding loses money.

## One PostgreSQL database, one schema per service
Schemas: `auth` (gateway), `wallet` (wallet), `ledger` (ledger).
Why: each service owns its tables, and no service writes another's tables.
Splitting into separate databases later is easy because nothing crosses schemas.

## Kafka runs in KRaft mode, single broker, for local dev
KRaft means Kafka manages itself without ZooKeeper.
Why: one container instead of two. Production would run 3 brokers.

## transfers.completed has 16 partitions
A consumer group uses at most one consumer per partition.
Why: the plan is to autoscale the ledger to 15 pods with KEDA,
so the topic needs at least 15 partitions or the extra pods sit idle.

## Transactional outbox for events
The wallet writes the balance change and an outbox row in one transaction.
A relay loop publishes outbox rows to Kafka.
Why: without it, the balance could change while the Kafka publish fails,
and the ledger would never hear about it.

## Every consumer is idempotent
Why: Kafka delivers at least once, so the same event can arrive twice.

## The ledger is append-only and enforces debits = credits in the database
A deferred trigger rejects any unbalanced entry at COMMIT.
Another trigger rejects UPDATE and DELETE on journal tables.
Why: correctness is guaranteed by the database, not only by application code.

## Topic names use dots only, never underscores
Kafka metric names turn both "." and "_" into "_", so a.b and a_b would collide.
Why: sticking to one separator avoids the clash Kafka warns about.

## Services use the languages from the project blueprint
gateway and wallet in Go, ledger in Java (Spring Boot), fraud in Python (FastAPI),
notification in Node.js.
Why: the blueprint shows several languages behind one platform, so the deployment
side has to handle a different build and runtime for each.


## Both Go services share one Go module at the repo root
Why: both need the generated gRPC code. One go.mod lets them import it directly.

## gRPC contract lives in proto/, generated with buf, generated code is committed
Why: buf lints the contract and can detect breaking changes in CI.
Committing gen/ means Docker builds only need Go, not buf and the code generators.

## Every RPC has its own Request and Response message
Why: one response can gain fields later without touching any other call.

## Ops endpoints on a separate port (9090)
/healthz, /readyz and /metrics are served on OPS_PORT, not the main port.
Why: they should never be reachable through the ingress, and probes keep working
even if the main server is overloaded.

## Kafka is not a readiness check for the wallet
Why: if Kafka is down, transfers still succeed and wait in the outbox.
A Kafka outage should not take payments down.

## Graceful shutdown: 5s drain, then 20s cleanup
On SIGTERM: /readyz returns 503, wait SHUTDOWN_DELAY (5s), stop servers, close connections.
A timer force-exits after SHUTDOWN_DELAY + SHUTDOWN_TIMEOUT (25s total).
Why: the wait lets Kubernetes remove the pod from its Service before we stop taking work,
and 25s fits inside the default 30s terminationGracePeriodSeconds.


## kafka-go writer: hash balancer, all acks, 10ms batch timeout
Events are keyed by wallet id, and the hash balancer sends one key to one partition,
so events for a wallet stay in order. RequireAll waits for every replica to confirm.
Why the 10ms batch timeout: kafka-go's default waits up to 1s to fill a batch,
which would add up to a second to every event.

## Outbox relay: SKIP LOCKED batches, at-least-once
The relay locks a batch with FOR UPDATE SKIP LOCKED, publishes it, marks it sent, commits.
Several wallet pods can run the relay at once without sending the same row twice at the
same moment. A crash between publish and commit resends the batch, so consumers must dedupe.
Tested: with Kafka stopped, transfers still succeed; events publish when Kafka returns.


## Gateway: JWT (HS256, 15 min), bcrypt passwords, same error for unknown email and wrong password
Why: short-lived tokens limit damage if one leaks; one error message stops attackers
from finding out which emails are registered. Upgrade path: RS256 so other services
can verify tokens with a public key instead of sharing the secret.

## Rate limits in Redis, fixed window, fail open
10/min per IP on login and register, 60/min per user on everything else.
Counters live in Redis, so the limit is shared across all gateway pods.
Why fail open: a Redis outage should not take the whole API down.
Tested: with Redis stopped, all requests still succeed.

## Gateway returns plain JSON numbers, not protobuf JSON
protobuf's JSON format writes int64 as strings ("50000"). The gateway maps responses
to its own structs so API clients get numbers.

## gRPC client uses round_robin
Why: gRPC holds one long-lived connection. Behind a normal ClusterIP Service, all calls
from one gateway pod would hit one wallet pod. With a headless Service
(dns:///wallet-headless:50051) round_robin spreads calls across all wallet pods.


## Ledger: Spring Boot 4, one listener, idempotent by event_id
Reads transfers.completed and wallet.deposited as consumer group "ledger".
Each event becomes one journal entry; event_id is UNIQUE, so a redelivered event is skipped.
The offset is committed only after the database commit (at-least-once + dedupe = each event
recorded exactly once).

## Ledger error handling: bad messages to the DLQ, everything else retries forever
Invalid events (not JSON, missing fields) go straight to <topic>.dlq.
Any other failure, like PostgreSQL being down, retries with backoff up to 10s and never gives up.
Why: sending a real transfer to the DLQ because the database blipped would lose it from the ledger.
Tested: with PostgreSQL stopped, the ledger retried, the DLQ stayed untouched, and when the
database returned it caught up and reconciliation showed zero differences.

## Ledger reads the same DATABASE_URL as the Go services
It converts postgres://user:pass@host/db into a JDBC URL itself.
Why: one variable name for every service keeps the deployment config simple.

## Container images: multi-stage, non-root, minimal base
Go services: static binary in gcr.io/distroless/static-debian12:nonroot (no shell, no package manager).
Ledger: Maven build stage, then eclipse-temurin:21-jre-alpine as a non-root user.
Fraud: python:3.13-slim as a non-root user. Notification: node:22-slim as the built-in node user.
Why: build tools never reach production images; fewer packages means fewer CVEs;
a compromised process is not root. Trade-off: no shell in the Go images, so debug with
logs and /readyz instead of docker exec.

## One Compose file runs the whole system
Infrastructure, the two one-shot jobs (topics, migrations), then the five services.
Services start only after migrations and topic creation finish successfully.

## Smoke test as a post-deploy check
scripts/smoke/smoke.js checks every /readyz, then signs up two users, moves money,
and waits for the WebSocket notifications and the fraud alert. Exit code 0 or 1,
so a CI/CD pipeline can run it after each deploy and roll back on failure.