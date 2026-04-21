# PolySport

Algorithmic trading bot for Polymarket soccer moneylines. The edge source is the
gap between Pinnacle's de-vigged fair line and Polymarket's mid price on 3-way
moneylines (Home / Draw / Away). `STRATEGY.md` is the source of truth for thesis,
thresholds, and graduation gates — this README covers how to run the code.

## Current phase

**Phase 1 — divergence logger.** No trading. The bot polls Pinnacle via The Odds
API and Polymarket CLOB prices on a fixed cadence, resolves team names to a
canonical `teams` table, and logs raw snapshots to Supabase. A FastAPI + HTMX
dashboard renders the live state. Graduation to Phase 2 (shadow strategy)
requires the 30%-touch threshold from `STRATEGY.md` Phase 1.

## Layout

```
polysport/
  feeds/        odds_api.py, polymarket.py, matcher.py
  math/         devig.py (power-method 3-way de-vigger)
  dashboard/    FastAPI app + Jinja templates + data.py view model
  cli.py        runtime entrypoint (stub; not wired yet)
migrations/     Supabase schema (001_phase1_schema.sql)
scripts/        phase1_logger.py, augment_aliases.py, seed_teams.py, ops tools
tests/          test_devig.py
```

## Local dev

1. `python -m venv .venv && source .venv/bin/activate`
2. `pip install -r requirements.txt`
3. `cp .env.example .env` and fill in Supabase + Odds API keys (see below)
4. Apply the schema: run `migrations/001_phase1_schema.sql` in the Supabase SQL editor
5. Seed teams + aliases: `python scripts/seed_teams.py` then
   `python scripts/augment_aliases.py --apply`
6. Run the logger once: `python scripts/phase1_logger.py --once`
7. Run the dashboard: `uvicorn polysport.dashboard.app:app --reload`

## Environment variables

See `.env.example`. Used in code today:

| Variable              | Used by                  | Notes                                  |
|-----------------------|--------------------------|----------------------------------------|
| `SUPABASE_URL`        | dashboard, matcher, logger | Supabase project URL                 |
| `SUPABASE_SERVICE_KEY`| dashboard, matcher, logger | service-role key (writes snapshots)  |
| `SUPABASE_ANON_KEY`   | (future) client-side     | not read by any runtime code today     |
| `ODDS_API_KEY`        | `odds_api.py`            | The Odds API Starter key (20k req/mo) |

## Deployment

- **Logger + dashboard:** Railway. Two services share this repo; per-service
  `startCommand` lives in each service's environment config (not in
  `railway.toml` — see the comment in that file).
- **Database:** Supabase (snapshots + teams).
- Vercel is not used.

## Tests

`pytest` runs from repo root. Current coverage is the de-vigger
(`tests/test_devig.py`). Expand before Phase 2 shadow mode.
