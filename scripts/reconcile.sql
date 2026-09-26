-- Reconciliation: does the ledger agree with the wallet balances?
-- Run after load tests and chaos tests. The expected result is ZERO rows.
--
--   docker compose exec -T postgres psql -U app -d wallet < scripts/reconcile.sql
--
-- Right after heavy traffic, a few wallets can differ for a moment while events
-- are still in the outbox or in Kafka. Wait for consumer lag to reach zero, then run it.
-- A difference that stays is a real bug.

WITH ledger_side AS (
    SELECT substring(account FROM 8)::uuid AS wallet_id, balance_cents
      FROM ledger.account_balances
     WHERE account LIKE 'wallet:%'
)
SELECT w.id                               AS wallet_id,
       w.balance_cents                    AS wallet_balance,
       COALESCE(l.balance_cents, 0)       AS ledger_balance,
       w.balance_cents - COALESCE(l.balance_cents, 0) AS difference
  FROM wallet.wallets w
  LEFT JOIN ledger_side l ON l.wallet_id = w.id
 WHERE w.balance_cents <> COALESCE(l.balance_cents, 0);

-- Global check: every journal entry must balance (debits = credits).
-- The database trigger already enforces this. This query proves it.
SELECT 'unbalanced entries' AS check_name, count(*) AS count
  FROM (
    SELECT entry_id
      FROM ledger.journal_lines
     GROUP BY entry_id
    HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE -amount_cents END) <> 0
  ) x;