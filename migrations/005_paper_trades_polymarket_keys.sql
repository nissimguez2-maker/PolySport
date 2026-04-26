-- 005_paper_trades_polymarket_keys.sql
--
-- Persist the Polymarket condition_id + Yes-token id at decision time so
-- the resolver can settle paper trades against Polymarket's market
-- resolution after the match concludes. Without these we'd have to
-- re-derive the market via the matcher, which is fragile across alias /
-- name changes between decision and settlement.
--
-- Both columns are nullable so the resolver can skip historical pre-005
-- rows and continue cleanly. Migration is re-runnable.

alter table paper_trades
  add column if not exists polymarket_condition_id text,
  add column if not exists polymarket_yes_token_id text;

-- Index supports the resolver's primary query: "give me unsettled rows
-- that have keys and whose kickoff is past." `where settled_at is null`
-- keeps it tiny — index size grows with backlog, not history.
create index if not exists paper_trades_resolver_idx
  on paper_trades (kickoff)
  where settled_at is null and polymarket_condition_id is not null;
