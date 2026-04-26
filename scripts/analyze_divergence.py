"""Phase 1 verdict: does ≥30% of monitored matches touch |div| ≥ 2¢ in T-120→0min?

Pipeline:
  1. Load polymarket_snapshots grouped by (home_team_id, away_team_id, commence_ref).
     A "match" is a unique (home, away, kickoff) triple. Polymarket doesn't give
     a reliable kickoff, so we anchor on the Odds API commence_time for each event.
  2. For each match, align every Polymarket poll moment with the nearest Pinnacle
     snapshot (from odds_api_snapshots, bookmaker='pinnacle') within ±STALENESS_SEC.
  3. For each aligned poll, compute:
       - Pinnacle fair_i per outcome (power-method de-vig)
       - Polymarket mid_i per outcome (best_bid + best_ask)/2
       - divergence_i = fair_i - mid_i
       - max_abs_div = max over i of |divergence_i|
  4. Filter to T-120min ≤ polled_at ≤ kickoff.
  5. A match "touches" if any qualifying poll has max_abs_div ≥ 2¢.
  6. Report pct of matches touching + full distribution.

Can be run at any time during or after the 48h log. Early runs will show
"waiting for more data" messages rather than incomplete verdicts.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.math.devig import devig_3way

STALENESS_SEC = 60  # STRATEGY: skip decision if Pinnacle snapshot > 60s old
DIVERGENCE_THRESHOLD = 0.02  # STRATEGY: |div| ≥ 2¢
WINDOW_BEFORE_KICKOFF = timedelta(minutes=120)  # T-120 → T-0
PHASE1_TARGET_PCT = 30.0  # STRATEGY: ≥30% of matches must touch


@dataclass
class Match:
    home_id: str
    away_id: str
    kickoff: datetime
    canonical_home: str
    canonical_away: str
    pinnacle_polls: list[dict]  # raw rows, sorted by polled_at
    polymarket_outcomes: dict[str, list[dict]]  # outcome_side -> sorted rows


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_data(sb) -> tuple[dict[tuple, Match], dict[str, str]]:
    """Load Pinnacle + Polymarket snapshots and organise by match key.

    Match key: (home_team_id, away_team_id, kickoff_rounded_to_minute).
    Round the kickoff to the minute to survive minor clock skew between feeds.
    """
    # Teams index for pretty-printing.
    teams = sb.table("teams").select("id, canonical_name").execute().data
    team_name: dict[str, str] = {t["id"]: t["canonical_name"] for t in teams}

    # Paginated reads — supabase-py caps each .execute() at 1000 rows. A naive
    # offset-pagination over polymarket_snapshots (260k+) hits PostgREST's
    # statement_timeout. Use keyset pagination on polled_at + a 30-day window
    # (more than enough for Phase 1's 48h sanity check).
    cutoff = (datetime.now(UTC) - timedelta(days=30)).isoformat()

    def _keyset_all(
        query_factory: Callable[[str], Any], ts_col: str = "polled_at", page_size: int = 1000
    ) -> list[dict]:
        out: list[dict] = []
        last_ts = cutoff
        while True:
            rows = (
                query_factory(last_ts)
                .order(ts_col)
                .limit(page_size)
                .execute()
                .data
            )
            if not rows:
                break
            out.extend(rows)
            if len(rows) < page_size:
                break
            # Advance watermark; gt to avoid re-pulling the boundary row.
            last_ts = rows[-1][ts_col]
        return out

    pin_rows = _keyset_all(
        lambda since: sb.table("odds_api_snapshots")
        .select(
            "home_team_id, away_team_id, commence_time, odds_home, odds_draw, "
            "odds_away, polled_at, bookmaker"
        )
        .eq("bookmaker", "pinnacle")
        .not_.is_("home_team_id", "null")
        .not_.is_("away_team_id", "null")
        .not_.is_("odds_home", "null")
        .gt("polled_at", since)
    )

    pm_rows = _keyset_all(
        lambda since: sb.table("polymarket_snapshots")
        .select(
            "home_team_id, away_team_id, outcome_side, best_bid, best_ask, polled_at, commence_time"
        )
        .not_.is_("home_team_id", "null")
        .not_.is_("away_team_id", "null")
        .not_.is_("outcome_side", "null")
        .gt("polled_at", since)
    )

    print(f"Loaded {len(pin_rows)} pinnacle rows, {len(pm_rows)} polymarket rows.", flush=True)

    # Build match index from Pinnacle (it has trustworthy kickoff).
    matches: dict[tuple, Match] = {}
    for r in pin_rows:
        kt = _parse_ts(r["commence_time"])
        if not kt:
            continue
        key = (r["home_team_id"], r["away_team_id"], kt.replace(second=0, microsecond=0))
        if key not in matches:
            matches[key] = Match(
                home_id=r["home_team_id"],
                away_id=r["away_team_id"],
                kickoff=kt,
                canonical_home=team_name.get(r["home_team_id"], r["home_team_id"]),
                canonical_away=team_name.get(r["away_team_id"], r["away_team_id"]),
                pinnacle_polls=[],
                polymarket_outcomes=defaultdict(list),
            )
        matches[key].pinnacle_polls.append(r)

    # Attach Polymarket polls to the matching (home, away) pair. Polymarket's
    # commence_time is event creation, not kickoff — we cannot use it for
    # timing, only team ids.
    for r in pm_rows:
        # Find a match with same (home, away) teams. Kickoff from Pinnacle.
        # In rare cases of repeat fixtures, this picks the earliest; for Phase 1
        # that's acceptable since all logged data is within the 72h horizon.
        cand = [k for k in matches if k[0] == r["home_team_id"] and k[1] == r["away_team_id"]]
        if not cand:
            continue
        key = min(cand, key=lambda k: k[2])
        matches[key].polymarket_outcomes[r["outcome_side"]].append(r)

    # Sort all rows by polled_at for efficient nearest-neighbour lookups.
    for m in matches.values():
        m.pinnacle_polls.sort(
            key=lambda r: _parse_ts(r["polled_at"]) or datetime.max.replace(tzinfo=UTC)
        )
        for side in m.polymarket_outcomes:
            m.polymarket_outcomes[side].sort(
                key=lambda r: _parse_ts(r["polled_at"]) or datetime.max.replace(tzinfo=UTC)
            )

    return matches, team_name


def _nearest(rows: list[dict], t: datetime) -> dict | None:
    """Linear search for the poll row whose polled_at is closest to t. Fine for
    thousands of rows per match; switch to bisect if we ever push into millions."""
    if not rows:
        return None
    best: dict | None = None
    best_dt: float | None = None
    for r in rows:
        rt = _parse_ts(r["polled_at"])
        if not rt:
            continue
        dt = abs((rt - t).total_seconds())
        if best_dt is None or dt < best_dt:
            best, best_dt = r, dt
    return best


def analyse_match(m: Match) -> dict:
    """Compute divergence time-series for a single match."""
    window_start = m.kickoff - WINDOW_BEFORE_KICKOFF

    # Pick a reference Polymarket poll stream — we need all 3 outcomes at (roughly)
    # the same poll moment. Use the HOME outcome's poll timestamps as the anchor,
    # and within each cycle find the nearest draw/away + nearest Pinnacle.
    home_polls = m.polymarket_outcomes.get("home", [])
    draw_polls = m.polymarket_outcomes.get("draw", [])
    away_polls = m.polymarket_outcomes.get("away", [])

    divergences: list[dict] = []
    skipped_incomplete = 0
    skipped_stale = 0
    skipped_bad_book = 0
    skipped_out_of_window = 0

    for pm_home in home_polls:
        pm_time = _parse_ts(pm_home["polled_at"])
        if not pm_time:
            continue
        if not (window_start <= pm_time <= m.kickoff):
            skipped_out_of_window += 1
            continue

        pm_draw = _nearest(draw_polls, pm_time)
        pm_away = _nearest(away_polls, pm_time)
        if not (pm_draw and pm_away):
            skipped_incomplete += 1
            continue

        pin = _nearest(m.pinnacle_polls, pm_time)
        if not pin:
            skipped_stale += 1
            continue
        pin_time = _parse_ts(pin["polled_at"])
        if pin_time is None or abs((pin_time - pm_time).total_seconds()) > STALENESS_SEC:
            skipped_stale += 1
            continue

        mid_h = _mid(pm_home)
        mid_d = _mid(pm_draw)
        mid_a = _mid(pm_away)
        if mid_h is None or mid_d is None or mid_a is None:
            skipped_bad_book += 1
            continue

        try:
            fair = devig_3way(
                float(pin["odds_home"]), float(pin["odds_draw"]), float(pin["odds_away"])
            )
        except (ValueError, TypeError):
            skipped_bad_book += 1
            continue

        div_h = fair.home - mid_h
        div_d = fair.draw - mid_d
        div_a = fair.away - mid_a
        max_abs = max(abs(div_h), abs(div_d), abs(div_a))
        divergences.append(
            {
                "polled_at": pm_time,
                "minutes_to_kickoff": (m.kickoff - pm_time).total_seconds() / 60.0,
                "fair": (fair.home, fair.draw, fair.away),
                "mid": (mid_h, mid_d, mid_a),
                "div": (div_h, div_d, div_a),
                "max_abs_div": max_abs,
            }
        )

    return {
        "match": m,
        "points": divergences,
        "touches": any(d["max_abs_div"] >= DIVERGENCE_THRESHOLD for d in divergences),
        "n_points": len(divergences),
        "skipped": {
            "incomplete": skipped_incomplete,
            "stale_pinnacle": skipped_stale,
            "bad_book": skipped_bad_book,
            "out_of_window": skipped_out_of_window,
        },
    }


def _mid(row: dict) -> float | None:
    b = row.get("best_bid")
    a = row.get("best_ask")
    if b is None or a is None:
        return None
    return (float(b) + float(a)) / 2.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print the max-divergence point for every match.",
    )
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    matches, _ = load_data(sb)
    if not matches:
        print("No matches found in snapshots yet. Let the logger run a bit longer.")
        return 0

    print("=" * 78)
    print(f"PHASE 1 DIVERGENCE ANALYSIS  ({len(matches)} matches indexed)")
    print("=" * 78)

    results = [analyse_match(m) for m in matches.values()]
    # Only matches with ≥1 qualifying point contribute to the verdict.
    graded = [r for r in results if r["n_points"] > 0]
    touching = [r for r in graded if r["touches"]]
    not_graded = [r for r in results if r["n_points"] == 0]

    pct = len(touching) / len(graded) * 100 if graded else 0.0
    verdict = "PASS ✓" if pct >= PHASE1_TARGET_PCT else "not yet"

    print(f"\nMatches with any qualifying poll in T-120→T-0 window: {len(graded)} / {len(matches)}")
    print(
        f"Matches that touched |div| ≥ {DIVERGENCE_THRESHOLD * 100:.0f}¢:           "
        f"{len(touching)} / {len(graded)}  ({pct:.1f}%)"
    )
    print(f"Phase 1 target: ≥ {PHASE1_TARGET_PCT:.0f}%  →  {verdict}")

    if graded:
        # Grab each match's single largest |div|, not all points, for distribution.
        per_match_max = [max(p["max_abs_div"] for p in r["points"]) for r in graded]
        print("\nPer-match max |div| distribution (¢):")
        print(f"  median   {median(per_match_max) * 100:6.2f}¢")
        print(f"  max      {max(per_match_max) * 100:6.2f}¢")
        print(f"  min      {min(per_match_max) * 100:6.2f}¢")

    if not_graded:
        print(
            f"\n{len(not_graded)} matches have no qualifying poll yet "
            f"(outside T-120→0 window, incomplete book, or Pinnacle > {STALENESS_SEC}s stale)."
        )

    # Why are polls being rejected? Aggregate skip counters across all matches.
    total_skips = {"incomplete": 0, "stale_pinnacle": 0, "bad_book": 0, "out_of_window": 0}
    for r in results:
        for k, v in r["skipped"].items():
            total_skips[k] += v
    total_skipped = sum(total_skips.values())
    if total_skipped:
        print("\n--- skip reasons (aggregate across all matches) ---")
        for reason, count in sorted(total_skips.items(), key=lambda kv: -kv[1]):
            pct_skip = count / total_skipped * 100 if total_skipped else 0.0
            print(f"  {reason:<18} {count:8d}  ({pct_skip:5.1f}%)")

    print("\n" + "-" * 78)
    print("PER-MATCH DETAIL")
    print("-" * 78)
    for r in sorted(results, key=lambda r: r["match"].kickoff):
        m: Match = r["match"]
        hit = "★" if r["touches"] else " "
        max_div = max(p["max_abs_div"] for p in r["points"]) if r["points"] else 0.0
        mins_to_kick = (m.kickoff - datetime.now(UTC)).total_seconds() / 60.0
        when = m.kickoff.strftime("%Y-%m-%d %H:%M UTC")
        print(
            f"  {hit} {when}  {m.canonical_home:<24} vs {m.canonical_away:<24} "
            f" pts={r['n_points']:3d}  max|div|={max_div * 100:5.2f}¢  "
            f"(T{mins_to_kick:+.0f}min)"
        )
        if args.verbose and r["points"]:
            p = max(r["points"], key=lambda x: x["max_abs_div"])
            print(
                f"      at T{p['minutes_to_kickoff']:+.1f}min: "
                f"fair={tuple(round(x, 3) for x in p['fair'])}  "
                f"mid={tuple(round(x, 3) for x in p['mid'])}  "
                f"div={tuple(round(x * 100, 2) for x in p['div'])}¢"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
