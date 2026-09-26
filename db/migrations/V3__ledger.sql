-- Owned by the ledger service.
CREATE SCHEMA IF NOT EXISTS ledger;

-- One entry per business event. event_id is UNIQUE, which is what makes
-- the ledger idempotent: a duplicate Kafka delivery fails to insert and is skipped.
CREATE TABLE ledger.journal_entries (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id     text NOT NULL UNIQUE,
    event_type   text NOT NULL,
    reference_id uuid NOT NULL,          -- the wallet transaction id
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- Lines are immutable. Nothing ever UPDATEs or DELETEs them.
CREATE TABLE ledger.journal_lines (
    id           bigserial PRIMARY KEY,
    entry_id     uuid NOT NULL REFERENCES ledger.journal_entries(id),
    account      text NOT NULL,          -- e.g. wallet:<uuid> or external:cash
    direction    text NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_cents bigint NOT NULL CHECK (amount_cents > 0)
);

CREATE INDEX journal_lines_account_idx ON ledger.journal_lines (account);
CREATE INDEX journal_lines_entry_idx   ON ledger.journal_lines (entry_id);

-- The double-entry rule, enforced by the database itself:
-- for every entry, total debits must equal total credits.
-- The check is DEFERRED, so it runs at COMMIT, after all lines are inserted.
-- An unbalanced entry makes the whole transaction fail.
CREATE FUNCTION ledger.check_entry_balanced() RETURNS trigger AS $$
DECLARE
    diff bigint;
BEGIN
    SELECT COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE -amount_cents END), 0)
      INTO diff
      FROM ledger.journal_lines
     WHERE entry_id = NEW.entry_id;
    IF diff <> 0 THEN
        RAISE EXCEPTION 'journal entry % is unbalanced by % cents', NEW.entry_id, diff;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER journal_entry_balanced
    AFTER INSERT ON ledger.journal_lines
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger.check_entry_balanced();

-- Block edits and deletes on the journal. Corrections are new entries, never edits.
CREATE FUNCTION ledger.forbid_changes() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger journal is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER journal_lines_immutable
    BEFORE UPDATE OR DELETE ON ledger.journal_lines
    FOR EACH ROW EXECUTE FUNCTION ledger.forbid_changes();

CREATE TRIGGER journal_entries_immutable
    BEFORE UPDATE OR DELETE ON ledger.journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger.forbid_changes();

-- Balance of every account, derived from the journal.
-- A wallet account's balance = credits - debits.
CREATE VIEW ledger.account_balances AS
SELECT account,
       SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE -amount_cents END) AS balance_cents
  FROM ledger.journal_lines
 GROUP BY account;
 