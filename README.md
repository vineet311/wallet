# wallet-ledger

A digital wallet and double-entry ledger built as microservices.
Users hold balances, send money to each other, and get live notifications.
Every money movement is recorded in an append-only double-entry ledger,
and a fraud worker screens transfers in real time.

## Services

| Service      | Language | Job                                              |
|--------------|----------|--------------------------------------------------|
| gateway      | Go       | Auth (JWT), rate limiting, REST API, calls wallet over gRPC |
| wallet       | Go       | Balances, transfers, optimistic locking, outbox  |
| ledger       | TJava (Spring Boot)      | Consumes events, writes double-entry journal     |
| fraud        | Python   | Consumes transfers, amount and velocity rules    |
| notification | Node.js  | WebSocket push and webhook alerts                |

## Architecture

```
       [ Client ]
           │
     [ gateway ] ──gRPC──► [ wallet ] ──► PostgreSQL
                               │
                         (outbox relay)
                               ▼
                            Kafka
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
          [ ledger ]       [ fraud ]     [ notification ]
          PostgreSQL        Redis          WebSocket
```
## Local setup

```bash
cp .env.example .env
docker compose up -d
```

**Codespaces only:** after every Codespace start, run
`sudo iptables-legacy -P FORWARD ACCEPT` before `docker compose up`.
Without it, containers cannot reach each other. See `docs/difficulties.md`.


## Status

## Status

All five services are complete, containerised, and tested end to end.

- **Deploying or operating it:** start with [`docs/HANDOVER.md`](docs/HANDOVER.md)
- **Why it's built this way:** [`docs/decisions.md`](docs/decisions.md)
- **Problems hit and how they were fixed:** [`docs/difficulties.md`](docs/difficulties.md)

Quick start:

```bash
cp .env.example .env
docker compose up -d --build
cd scripts/smoke && npm install && node smoke.js
```
