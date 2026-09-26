-- Owned by the gateway (auth) service.
CREATE SCHEMA IF NOT EXISTS auth;

CREATE TABLE auth.users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email         text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);