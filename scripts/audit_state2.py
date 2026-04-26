"""Followup audit: ghost team IDs, PM-only pairs, and logger liveness."""

from __future__ import annotations

import os
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


def _parse_ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    now = datetime.now(UTC)
    h48 = now - timedelta(hours=48)

    # --- Pull ALL pm rows from last 48h using pagination ---
    pm_all: list[dict] = []
    page = 0
    while True:
        rows = (
            sb.table("polymarket_snapshots")
            .select(
                "home_team_id, away_team_id, outcome_side, best_bid, best_ask, "
                "polled_at, outcome_raw, raw"
            )
            .gte("polled_at", h48.isoformat())
            .range(page * 1000, page * 1000 + 999)
            .execute()
            .data
        )
        if not rows:
            break
        pm_all.extend(rows)
        if len(rows) < 1000:
            break
        page += 1

    pin_all: list[dict] = []
    page = 0
    while True:
        rows = (
            sb.table("odds_api_snapshots")
            .select("home_team_id, away_team_id, commence_time, polled_at, bookmaker")
            .eq("bookmaker", "pinnacle")
            .gte("polled_at", h48.isoformat())
            .range(page * 1000, page * 1000 + 999)
            .execute()
            .data
        )
        if not rows:
            break
        pin_all.extend(rows)
        if len(rows) < 1000:
            break
        page += 1

    print(f"Pulled {len(pin_all)} pinnacle rows, {len(pm_all)} pm rows.")

    # --- Unresolved team IDs ---
    teams = sb.table("teams").select("id, canonical_name").execute().data
    valid_ids = {t["id"] for t in teams}

    ghost_pairs: Counter = Counter()
    resolved_pairs: Counter = Counter()
    null_pm = 0
    for r in pm_all:
        h, a = r["home_team_id"], r["away_team_id"]
        if h is None or a is None:
            null_pm += 1
            continue
        if h in valid_ids and a in valid_ids:
            resolved_pairs[(h, a)] += 1
        else:
            ghost_pairs[(h, a, r.get("outcome_raw"))] += 1

    print(f"\nPolymarket: null_team_id rows = {null_pm}")
    print(f"Polymarket: rows with non-existent team_ids (ghosts) = {sum(ghost_pairs.values())}")
    print(f"Polymarket: rows with both team_ids valid = {sum(resolved_pairs.values())}")
    print("\nTop 20 ghost team_id pairs (PM rows): outcome_raw -> count")
    for (h, a, outcome), n in ghost_pairs.most_common(20):
        print(f"  {n:4d}  home_id={h}  away_id={a}  outcome={outcome!r}")

    # --- Check teams table size ---
    print(f"\nteams table has {len(valid_ids)} rows.")
    # Show some team rows
    print("sample teams:")
    for t in teams[:8]:
        print(f"  {t['id']}  {t['canonical_name']}")

    # --- Timeline of logger activity (hourly buckets over 48h) ---
    print("\n--- logger hourly activity (pinnacle rows per hour, last 48h) ---")
    buckets: Counter = Counter()
    for r in pin_all:
        pt = _parse_ts(r["polled_at"])
        bucket = pt.replace(minute=0, second=0, microsecond=0)
        buckets[bucket] += 1
    pm_buckets: Counter = Counter()
    for r in pm_all:
        pt = _parse_ts(r["polled_at"])
        bucket = pt.replace(minute=0, second=0, microsecond=0)
        pm_buckets[bucket] += 1
    for b in sorted(set(buckets) | set(pm_buckets)):
        print(f"  {b.isoformat()}  pin={buckets.get(b, 0):5d}  pm={pm_buckets.get(b, 0):5d}")

    # --- PM-only pairs: what outcomes ---
    print("\n--- ghost-id Polymarket rows: sample raw titles ---")
    sample_titles = set()
    for r in pm_all:
        h, a = r["home_team_id"], r["away_team_id"]
        if (h not in valid_ids or a not in valid_ids) and r.get("raw"):
            raw = r.get("raw") or {}
            sample_titles.add(raw.get("event_slug") or raw.get("market_slug") or "?")
        if len(sample_titles) >= 15:
            break
    for t in sorted(sample_titles):
        print(f"  {t}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
