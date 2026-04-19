-- PolySport Phase 1 schema.
-- Design principle: raw feeds logged unconditionally; joins happen async; never guess silently.

create extension if not exists "pgcrypto";

-- ============================================================================
-- teams: canonical ground truth. Seeded once from TheSportsDB; grows as aliases
-- are added from the unresolved_entities review queue.
-- ============================================================================
create table if not exists teams (
  id              uuid primary key default gen_random_uuid(),
  canonical_name  text not null,
  country         text,
  league          text not null,    -- 'epl' | 'seriea' | 'laliga' | 'bundesliga' | 'ligue1' | 'international'
  wikidata_qid    text,
  aliases         text[] not null default '{}',
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

create unique index if not exists teams_canonical_league_uniq on teams (canonical_name, league);
create index if not exists teams_aliases_gin on teams using gin (aliases);
create index if not exists teams_league_idx on teams (league);

alter table teams enable row level security;

-- ============================================================================
-- odds_api_snapshots: raw Odds API feed, append-only.
-- One row per event per poll per bookmaker. Pinnacle is the one we care about.
-- ============================================================================
create table if not exists odds_api_snapshots (
  id              uuid primary key default gen_random_uuid(),
  event_id        text not null,
  league_key      text not null,
  home_team_raw   text not null,
  away_team_raw   text not null,
  home_team_id    uuid references teams(id),
  away_team_id    uuid references teams(id),
  commence_time   timestamptz not null,
  bookmaker       text not null,
  odds_home       numeric,
  odds_draw       numeric,
  odds_away       numeric,
  polled_at       timestamptz not null default now(),
  raw             jsonb not null
);

create index if not exists oas_event_polled_idx on odds_api_snapshots (event_id, polled_at desc);
create index if not exists oas_polled_idx       on odds_api_snapshots (polled_at desc);
create index if not exists oas_commence_idx     on odds_api_snapshots (commence_time);

alter table odds_api_snapshots enable row level security;

-- ============================================================================
-- polymarket_snapshots: raw Polymarket feed, append-only.
-- One row per outcome per poll. 3-way moneyline = 3 rows per event per poll.
-- ============================================================================
create table if not exists polymarket_snapshots (
  id                   uuid primary key default gen_random_uuid(),
  event_id             text not null,    -- polymarket event slug or id
  market_id            text not null,    -- token / conditionId
  outcome_raw          text not null,
  outcome_side         text,             -- 'home' | 'draw' | 'away', resolved at write time if possible
  home_team_id         uuid references teams(id),
  away_team_id         uuid references teams(id),
  commence_time        timestamptz,
  best_bid             numeric,
  best_ask             numeric,
  best_bid_depth_usd   numeric,          -- size_at_best_bid * best_bid
  best_ask_depth_usd   numeric,
  polled_at            timestamptz not null default now(),
  raw                  jsonb not null
);

create index if not exists pms_event_polled_idx on polymarket_snapshots (event_id, polled_at desc);
create index if not exists pms_market_polled_idx on polymarket_snapshots (market_id, polled_at desc);
create index if not exists pms_polled_idx        on polymarket_snapshots (polled_at desc);
create index if not exists pms_commence_idx      on polymarket_snapshots (commence_time);

alter table polymarket_snapshots enable row level security;

-- ============================================================================
-- match_links: the only table that joins Odds API events to Polymarket events.
-- Auto-populated only when team IDs match on both sides AND kickoff times are
-- within 5 min AND the league matches. 2/3 triggers review, not auto-accept.
-- ============================================================================
create table if not exists match_links (
  id                   uuid primary key default gen_random_uuid(),
  odds_api_event_id    text not null,
  polymarket_event_id  text not null,
  home_team_id         uuid not null references teams(id),
  away_team_id         uuid not null references teams(id),
  kickoff              timestamptz not null,
  league               text not null,
  confidence           numeric not null,
  method               text not null,    -- 'auto_three_corroborators' | 'manual'
  created_at           timestamptz not null default now(),
  updated_at           timestamptz not null default now(),
  unique (odds_api_event_id, polymarket_event_id)
);

create index if not exists ml_oa_event_idx on match_links (odds_api_event_id);
create index if not exists ml_pm_event_idx on match_links (polymarket_event_id);
create index if not exists ml_kickoff_idx  on match_links (kickoff);

alter table match_links enable row level security;

-- ============================================================================
-- unresolved_entities: names seen in feeds that didn't resolve to a team.
-- User reviews this queue and adds aliases, then historical data is re-matched.
-- ============================================================================
create table if not exists unresolved_entities (
  id                uuid primary key default gen_random_uuid(),
  source            text not null,      -- 'odds_api' | 'polymarket'
  raw_name          text not null,
  context           jsonb,
  seen_count        int not null default 1,
  first_seen        timestamptz not null default now(),
  last_seen         timestamptz not null default now(),
  resolved_at       timestamptz,
  resolved_team_id  uuid references teams(id),
  unique (source, raw_name)
);

create index if not exists ue_unresolved_idx on unresolved_entities (source, last_seen desc)
  where resolved_at is null;

alter table unresolved_entities enable row level security;
