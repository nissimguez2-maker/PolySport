"""Shared snapshot loader for Pinnacle + Polymarket Phase 1 data.

Why this exists
---------------
Both analyze_divergence.py and analyze_reversion.py independently re-derived
the same paginated load against Supabase, including the same pair of bugs
(silent 1000-row truncation, no kickoff-drift dedup). The third caller
(scripts/backtest_phase1.py) made that duplication unsustainable. This
module owns the loader; existing analyze scripts can migrate when convenient.

What it returns
---------------
load_pinnacle_pm_rows(sb, days_back=30)
    -> (pinnacle_rows, polymarket_rows, team_name_by_id)

Both row lists are sorted by polled_at ascending (a side-effect of the
keyset pagination strategy on the same column). Use them as-is or
re-sort downstream — the inner loop's nearest-neighbour lookups are
typically what dictate sort order.

The 30-day window is generous headroom for Phase 1 (target: 48h+ of data).
Bump it for any longer-horizon study; the keyset paginator handles
hundreds of thousands of rows without hitting PostgREST timeouts.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any


def _keyset_all(
    query_factory: Callable[[str], Any],
    *,
    cutoff_iso: str,
    ts_col: str = "polled_at",
    page_size: int = 1000,
) -> list[dict]:
    """Paginate by ascending ts_col so each page is an indexed range scan.

    Naive offset-pagination (.range(0,999) ... .range(1000,1999) ...) hits
    PostgREST's statement_timeout on tables in the 100k+ range because
    the count-style scan doesn't index well. Keyset on polled_at is O(log n)
    per page using the natural sort index.
    """
    out: list[dict] = []
    last_ts = cutoff_iso
    while True:
        rows = query_factory(last_ts).order(ts_col).limit(page_size).execute().data
        if not rows:
            break
        out.extend(rows)
        if len(rows) < page_size:
            break
        last_ts = rows[-1][ts_col]
    return out


def load_pinnacle_pm_rows(
    sb,
    *,
    days_back: int = 30,
    pinnacle_columns: str | None = None,
    polymarket_columns: str | None = None,
) -> tuple[list[dict], list[dict], dict[str, str]]:
    """Pull Pinnacle + Polymarket snapshots for the last `days_back` days.

    pinnacle_columns / polymarket_columns let callers narrow the SELECT
    to what they actually need. Defaults to a superset that covers
    every current caller's needs (analyze_divergence, analyze_reversion,
    backtest_phase1) — overspending a few KB per row is cheaper than
    discovering a missing column mid-run.
    """
    if pinnacle_columns is None:
        pinnacle_columns = (
            "home_team_id, away_team_id, commence_time, odds_home, odds_draw, "
            "odds_away, polled_at, bookmaker, league_key"
        )
    if polymarket_columns is None:
        polymarket_columns = (
            "home_team_id, away_team_id, outcome_side, best_bid, best_ask, "
            "best_bid_depth_usd, best_ask_depth_usd, polled_at"
        )

    teams = sb.table("teams").select("id, canonical_name").execute().data
    team_name: dict[str, str] = {t["id"]: t["canonical_name"] for t in teams}

    cutoff_iso = (datetime.now(UTC) - timedelta(days=days_back)).isoformat()

    pin_rows = _keyset_all(
        lambda since: (
            sb.table("odds_api_snapshots")
            .select(pinnacle_columns)
            .eq("bookmaker", "pinnacle")
            .not_.is_("home_team_id", "null")
            .not_.is_("away_team_id", "null")
            .not_.is_("odds_home", "null")
            .gt("polled_at", since)
        ),
        cutoff_iso=cutoff_iso,
    )

    pm_rows = _keyset_all(
        lambda since: (
            sb.table("polymarket_snapshots")
            .select(polymarket_columns)
            .not_.is_("home_team_id", "null")
            .not_.is_("away_team_id", "null")
            .not_.is_("outcome_side", "null")
            .gt("polled_at", since)
        ),
        cutoff_iso=cutoff_iso,
    )

    return pin_rows, pm_rows, team_name
