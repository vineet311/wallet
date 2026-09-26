-- Owned by the wallet service.
CREATE SCHEMA IF NOT EXISTS wallet;

CREATE TABLE wallet.wallets (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       uuid NOT NULL UNIQUE,
    balance_cents bigint NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    currency      char(3) NOT NULL DEFAULT 'USD',
    -- Optimistic locking: every balance change bumps the version.
    -- An UPDATE that expects an old version matches zero rows and the caller retries.
    version       integer NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE wallet.transactions (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid NOT NULL,                      -- who started it
    kind            text NOT NULL CHECK (kind IN ('DEPOSIT', 'TRANSFER')),
    from_wallet_id  uuid REFERENCES wallet.wallets(id), -- NULL for deposits
    to_wallet_id    uuid NOT NULL REFERENCES wallet.wallets(id),
    amount_cents    bigint NOT NULL CHECK (amount_cents > 0),
    status          text NOT NULL DEFAULT 'COMPLETED',
    idempotency_key text NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    -- The same user sending the same key twice gets the first result back.
    UNIQUE (user_id, idempotency_key),
    CHECK (from_wallet_id IS NULL OR from_wallet_id <> to_wallet_id)
);

CREATE INDEX transactions_from_wallet_idx ON wallet.transactions (from_wallet_id, created_at);
CREATE INDEX transactions_to_wallet_idx   ON wallet.transactions (to_wallet_id, created_at);

-- Transactional outbox. A row is written in the SAME database transaction
-- as the balance change, so "money moved" and "event recorded" can never disagree.
-- A relay loop in the wallet service publishes unsent rows to Kafka.
CREATE TABLE wallet.outbox (
    id           bigserial PRIMARY KEY,
    topic        text NOT NULL,
    message_key  text NOT NULL,
    payload      jsonb NOT NULL,
    headers      jsonb NOT NULL DEFAULT '{}'::jsonb,   -- carries the trace context
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

CREATE INDEX outbox_unpublished_idx ON wallet.outbox (id) WHERE published_at IS NULL;