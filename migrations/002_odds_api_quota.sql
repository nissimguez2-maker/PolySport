-- Singleton row tracking the most recent Odds API quota headers. Updated after
-- every paid call from the logger; read by the dashboard to render a burn-rate
-- tile. One row, id=1, because we only ever care about "latest".

create table if not exists odds_api_quota (
  id           int primary key default 1,
  remaining    int,
  used         int,
  last_cost    int,
  updated_at   timestamptz not null default now(),
  constraint odds_api_quota_single_row check (id = 1)
);

alter table odds_api_quota enable row level security;
