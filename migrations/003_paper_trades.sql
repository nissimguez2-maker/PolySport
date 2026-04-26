-- PolySport paper-trade tape.
-- One row per strategy-fired entry signal in shadow / pre-live mode.
-- Designed so the same row schema serves Phase 2 shadow trading: the
-- settlement columns (settled_outcome / realized_pnl / settled_at) stay
-- null until a separate post-match resolver job populates them.
--
-- Unique (home_team_id, away_team_id, kickoff) enforces STRATEGY.md's
-- "max one leg per match" rule at the database level — the logger can
-- attempt insert every cycle and the constraint silently dedupes.

create table if not exists paper_trades (
    id                      uuid primary key default gen_random_uuid(),

    -- Match identity. Composite unique constraint at the bottom.
    home_team_id            uuid not null references teams(id),
    away_team_id            uuid not null references teams(id),
    kickoff                 timestamptz not null,

    -- When the strategy fired and what the world looked like.
    decided_at              timestamptz not null default now(),
    minutes_to_kick         numeric not null,

    -- The signal moneyline.evaluate_entry produced.
    target_outcome          text not null,             -- 'home' | 'draw' | 'away'
    side                    text not null default 'buy',
    limit_price             numeric not null,
    expected_edge           numeric not null,          -- positive divergence in $
    fair                    numeric not null,
    mid                     numeric not null,
    pinnacle_staleness_sec  numeric not null,

    -- Sim accounting at decision time.
    notional_usd            numeric not null,
    sim_entry_price         numeric not null,
    sim_net_pnl_ev          numeric not null,          -- EV under Pinnacle prior

    -- Settlement (Phase 2 resolver populates).
    settled_outcome         int,                       -- 0 | 1, null = pending
    realized_pnl            numeric,
    settled_at              timestamptz,

    unique (home_team_id, away_team_id, kickoff)
);

create index if not exists paper_trades_decided_idx
    on paper_trades (decided_at desc);
create index if not exists paper_trades_kickoff_idx
    on paper_trades (kickoff);
create index if not exists paper_trades_unsettled_idx
    on paper_trades (kickoff)
    where settled_at is null;

alter table paper_trades enable row level security;

-- RLS NOTE: same deferral as 001_phase1_schema.sql. Service role bypasses
-- RLS, which is what the logger and dashboard use. Add explicit policies
-- before introducing any anon-role access path.
