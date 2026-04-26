-- 004_paper_trades_dedup_fix.sql
--
-- Audit 2026-04-26: the previous unique (home_team_id, away_team_id, kickoff)
-- constraint allowed Pinnacle's minute-level commence_time jitter to
-- defeat single-leg-per-match dedup. The dashboard already deduplicates
-- on (home, away) only for this exact reason. paper_trades didn't.
--
-- Fix: collapse the kickoff component to the hour. Minute drift is
-- absorbed; same teams kicking off the same hour are the same fixture;
-- the rare same-day double-header (kickoffs >=1h apart) stays distinct.

do $$
declare
  c text;
begin
  -- Drop the prior auto-named tuple-key constraint by inspecting catalog.
  -- Survives Postgres' naming convention without hard-coding the name.
  select conname into c
  from pg_constraint
  where conrelid = 'paper_trades'::regclass
    and contype = 'u'
    and pg_get_constraintdef(oid) like '%home_team_id, away_team_id, kickoff)';
  if c is not null then
    execute format('alter table paper_trades drop constraint %I', c);
  end if;
end $$;

-- Generated column with the hour-bucketed kickoff. STORED so the unique
-- index can use it directly without recomputation per row.
--
-- Note: date_trunc('hour', timestamptz) is STABLE, not IMMUTABLE (the
-- result depends on session timezone), so it can't drive a generated
-- column. Casting kickoff to plain `timestamp` via `at time zone 'UTC'`
-- yields a timezone-independent UTC wall-clock value, and
-- date_trunc('hour', timestamp) is IMMUTABLE. The resulting column is
-- `timestamp without time zone` carrying UTC semantics.
alter table paper_trades
  add column if not exists kickoff_hour timestamp
  generated always as (date_trunc('hour', kickoff at time zone 'UTC')) stored;

-- The new dedup key. Same fixture across multiple poll cycles collapses
-- to one row regardless of minute drift.
create unique index if not exists paper_trades_match_unique_idx
  on paper_trades (home_team_id, away_team_id, kickoff_hour);
