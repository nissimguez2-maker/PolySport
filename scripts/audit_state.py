"""Ad-hoc audit: what does the DB actually contain after 2 days of logging?

Answers:
  1. When was the most recent poll on each table? (Is the logger alive?)
  2. How many rows per table, split by last 48h / last 24h / last 1h?
  3. Which matches were "in the T-120→0 window" at any point, and did
     we actually capture simultaneous Pinnacle + PM data?
  4. How many distinct matches with name-resolution failures?
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    now = datetime.now(UTC)
    h1 = now - timedelta(hours=1)
    h24 = now - timedelta(hours=24)
    h48 = now - timedelta(hours=48)

    print("=" * 78)
    print(f"AUDIT @ {now.isoformat()}")
    print("=" * 78)

    # ---- Row counts per table ----
    for table in ("odds_api_snapshots", "polymarket_snapshots", "unresolved_entities"):
        try:
            total = sb.table(table).select("id", count="exact").limit(1).execute()
            tot = total.count
        except Exception as exc:
            print(f"{table}: ERROR {exc!r}")
            continue

        ts_col = "polled_at" if table != "unresolved_entities" else "last_seen"
        last_hour = (
            sb.table(table)
            .select("id", count="exact")
            .gte(ts_col, h1.isoformat())
            .limit(1)
            .execute()
            .count
        )
        last_24 = (
            sb.table(table)
            .select("id", count="exact")
            .gte(ts_col, h24.isoformat())
            .limit(1)
            .execute()
            .count
        )
        last_48 = (
            sb.table(table)
            .select("id", count="exact")
            .gte(ts_col, h48.isoformat())
            .limit(1)
            .execute()
            .count
        )
        print(
            f"\n{table:<25} total={tot:7d}  "
            f"last_48h={last_48:7d}  last_24h={last_24:7d}  last_1h={last_hour:7d}"
        )

    # ---- Freshness: most recent poll ----
    print("\n--- most recent rows ---")
    for table in ("odds_api_snapshots", "polymarket_snapshots"):
        r = (
            sb.table(table)
            .select("polled_at")
            .order("polled_at", desc=True)
            .limit(1)
            .execute()
            .data
        )
        if r:
            ts = _parse_ts(r[0]["polled_at"])
            age = (now - ts).total_seconds() / 60.0
            print(f"  {table:<25} last_poll={ts.isoformat()} ({age:.1f}min ago)")

    # ---- Pinnacle coverage per match ----
    pin = (
        sb.table("odds_api_snapshots")
        .select("home_team_id, away_team_id, commence_time, polled_at")
        .eq("bookmaker", "pinnacle")
        .not_.is_("home_team_id", "null")
        .not_.is_("away_team_id", "null")
        .gte("polled_at", h48.isoformat())
        .execute()
        .data
    )
    pm = (
        sb.table("polymarket_snapshots")
        .select("home_team_id, away_team_id, polled_at, outcome_side, best_bid, best_ask")
        .not_.is_("home_team_id", "null")
        .not_.is_("away_team_id", "null")
        .gte("polled_at", h48.isoformat())
        .execute()
        .data
    )

    print(f"\n--- 48h sample: pinnacle rows={len(pin)}  polymarket rows={len(pm)} ---")

    # Organise PM rows by (home, away).
    pm_by_pair: dict[tuple, list[dict]] = defaultdict(list)
    for r in pm:
        pm_by_pair[(r["home_team_id"], r["away_team_id"])].append(r)

    match_summary: dict[tuple, dict] = defaultdict(
        lambda: {
            "pin_polls": 0,
            "pm_polls": 0,
            "in_window_polls": 0,
            "pm_with_book": 0,
            "kickoff": None,
            "names": None,
            "pm_sides_seen": set(),
        }
    )

    # Teams index
    teams = sb.table("teams").select("id, canonical_name").execute().data
    name_of = {t["id"]: t["canonical_name"] for t in teams}

    for r in pin:
        kt = _parse_ts(r["commence_time"])
        key = (r["home_team_id"], r["away_team_id"])
        s = match_summary[key]
        s["pin_polls"] += 1
        s["kickoff"] = kt
        s["names"] = (
            name_of.get(r["home_team_id"], r["home_team_id"]),
            name_of.get(r["away_team_id"], r["away_team_id"]),
        )
        pt = _parse_ts(r["polled_at"])
        if kt and pt and (kt - timedelta(minutes=120)) <= pt <= kt:
            s["in_window_polls"] += 1

    for r in pm:
        key = (r["home_team_id"], r["away_team_id"])
        s = match_summary[key]
        s["pm_polls"] += 1
        s["pm_sides_seen"].add(r["outcome_side"])
        if r.get("best_bid") is not None and r.get("best_ask") is not None:
            s["pm_with_book"] += 1

    print("\n--- per-match coverage (last 48h) ---")
    print(
        f"{'Match':<55}  {'Kickoff':<18}  {'pin':>5} {'pin_win':>7} "
        f"{'pm':>6} {'pm_book':>7} {'sides'}"
    )
    for _key, s in sorted(match_summary.items(), key=lambda kv: kv[1]["kickoff"] or now):
        names = s["names"] or ("?", "?")
        kt = s["kickoff"].strftime("%m-%d %H:%M UTC") if s["kickoff"] else "?"
        label = f"{names[0][:24]:<24} vs {names[1][:24]:<24}"
        sides = ",".join(sorted(s["pm_sides_seen"])) or "-"
        print(
            f"{label:<55}  {kt:<18}  {s['pin_polls']:5d} {s['in_window_polls']:7d} "
            f"{s['pm_polls']:6d} {s['pm_with_book']:7d} {sides}"
        )

    # ---- Unresolved entities ----
    u = (
        sb.table("unresolved_entities")
        .select("raw_name, league_hint, source, occurrences, last_seen_at")
        .order("occurrences", desc=True)
        .limit(20)
        .execute()
        .data
    )
    print("\n--- unresolved entities (top 20 by occurrences) ---")
    for r in u:
        print(
            f"  {r['occurrences']:5d}  [{r['source']:<10}/{r['league_hint'] or '?':<8}] "
            f"{r['raw_name']!r}"
        )

    # ---- Sanity check: pin-pm overlap count ----
    pm_pairs = set(pm_by_pair.keys())
    pin_pairs = {(r["home_team_id"], r["away_team_id"]) for r in pin}
    both = pm_pairs & pin_pairs
    pin_only = pin_pairs - pm_pairs
    pm_only = pm_pairs - pin_pairs
    print("\n--- pair overlap ---")
    print(f"  pairs seen in Pinnacle only : {len(pin_only)}")
    print(f"  pairs seen in Polymarket only: {len(pm_only)}")
    print(f"  pairs seen in both           : {len(both)}")

    # ---- Most recent T-120 window poll attempt ----
    r = (
        sb.table("odds_api_snapshots")
        .select("home_team_id, away_team_id, commence_time, polled_at")
        .order("polled_at", desc=True)
        .limit(50)
        .execute()
        .data
    )
    recent_in_window = []
    for row in r:
        kt = _parse_ts(row["commence_time"])
        pt = _parse_ts(row["polled_at"])
        if kt and pt and (kt - timedelta(minutes=120)) <= pt <= kt:
            recent_in_window.append((pt, row))
    print(
        f"\n--- recent (last 50 rows) odds_api polls during an in-window match: "
        f"{len(recent_in_window)} ---"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
