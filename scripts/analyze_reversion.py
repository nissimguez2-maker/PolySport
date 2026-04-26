"""Does Polymarket mean-revert toward Pinnacle fair before kickoff?

For the flip strategy we need to know: when |div| crosses 2¢ at time t0, what
does Polymarket's mid do over the next 5 / 15 / 30 / 60 / 120 minutes? If
Polymarket drifts toward Pinnacle within an hour, flip works. If it drifts
away or sits still, flip is just hold-with-extra-steps — we'd be better off
holding to settlement and capturing the full fair-vs-entry spread.

Per-match-outcome sampling:
  - Walk the divergence time-series (reuses analyze_divergence logic).
  - Find FIRST poll in T-120→T-0 where |div_i| >= 2¢ on outcome i.
  - That's one "entry". Sample the nearest poll at t0 + horizon for each
    horizon in HORIZONS_MIN. Measure favourable move = signed shift of
    Polymarket mid in the direction that would close the gap.
  - Aggregate across entries: median / p25 / p75 favourable move per horizon,
    P(favourable move >= 1.5¢) = prob of hitting the flip sell target.

One entry per match-outcome, capped at 3 per match. This avoids one persistent
gap dominating the stats. If we ever want multiple entries per match-outcome,
they need to be separated by >= 30 min for independence.
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

ENTRY_THRESHOLD = 0.02
WINDOW_BEFORE_KICKOFF = timedelta(minutes=120)
STALENESS_SEC = 60
HORIZONS_MIN = [5, 15, 30, 60, 120]
# Accept follow-up polls up to ±30% of the horizon (e.g. +30min can land
# anywhere in [21, 39] min). Keeps sparse-poll matches usable without blurring
# short horizons into long ones.
HORIZON_TOLERANCE = 0.30
# Flip strategy wants a 1.5¢ favourable move to hit its sell target.
FLIP_TARGET_CENTS = 1.5


@dataclass
class Match:
    home_id: str
    away_id: str
    kickoff: datetime
    canonical_home: str
    canonical_away: str
    pinnacle_polls: list[dict]
    polymarket_outcomes: dict[str, list[dict]]


def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _mid(row):
    b, a = row.get("best_bid"), row.get("best_ask")
    if b is None or a is None:
        return None
    return (float(b) + float(a)) / 2.0


def _nearest(rows, t, max_dt_sec=None):
    best, best_dt = None, None
    for r in rows:
        rt = _parse_ts(r["polled_at"])
        if not rt:
            continue
        dt = abs((rt - t).total_seconds())
        if max_dt_sec is not None and dt > max_dt_sec:
            continue
        if best is None or dt < best_dt:
            best, best_dt = r, dt
    return best


def load_data(sb) -> dict[tuple, Match]:
    teams = sb.table("teams").select("id, canonical_name").execute().data
    team_name = {t["id"]: t["canonical_name"] for t in teams}

    # Paginated keyset reads — supabase-py caps each .execute() at 1000 rows
    # and the pm table is 260k+; naive offset-pagination times out on the
    # PostgREST side. Same approach as analyze_divergence.py.
    cutoff = (datetime.now(UTC) - timedelta(days=30)).isoformat()

    def _keyset_all(
        query_factory: Callable[[str], Any], ts_col: str = "polled_at", page_size: int = 1000
    ) -> list[dict]:
        out: list[dict] = []
        last_ts = cutoff
        while True:
            rows = query_factory(last_ts).order(ts_col).limit(page_size).execute().data
            if not rows:
                break
            out.extend(rows)
            if len(rows) < page_size:
                break
            last_ts = rows[-1][ts_col]
        return out

    pin_rows = _keyset_all(
        lambda since: (
            sb.table("odds_api_snapshots")
            .select(
                "home_team_id, away_team_id, commence_time, odds_home, "
                "odds_draw, odds_away, polled_at, bookmaker"
            )
            .eq("bookmaker", "pinnacle")
            .not_.is_("home_team_id", "null")
            .not_.is_("away_team_id", "null")
            .not_.is_("odds_home", "null")
            .gt("polled_at", since)
        )
    )

    pm_rows = _keyset_all(
        lambda since: (
            sb.table("polymarket_snapshots")
            .select("home_team_id, away_team_id, outcome_side, best_bid, best_ask, polled_at")
            .not_.is_("home_team_id", "null")
            .not_.is_("away_team_id", "null")
            .not_.is_("outcome_side", "null")
            .gt("polled_at", since)
        )
    )

    print(f"Loaded {len(pin_rows)} pinnacle rows, {len(pm_rows)} polymarket rows.", flush=True)

    # Dedup matches whose Pinnacle-reported kickoffs drift ≤10min between
    # successive polls (same fixture, slightly-different commence_time string).
    KICKOFF_DEDUP_WINDOW = timedelta(minutes=10)
    matches: dict[tuple, Match] = {}
    for r in pin_rows:
        kt = _parse_ts(r["commence_time"])
        if not kt:
            continue
        existing_key = next(
            (
                k
                for k in matches
                if k[0] == r["home_team_id"]
                and k[1] == r["away_team_id"]
                and abs((k[2] - kt).total_seconds()) <= KICKOFF_DEDUP_WINDOW.total_seconds()
            ),
            None,
        )
        if existing_key is not None:
            matches[existing_key].pinnacle_polls.append(r)
            continue
        key = (r["home_team_id"], r["away_team_id"], kt)
        matches[key] = Match(
            home_id=r["home_team_id"],
            away_id=r["away_team_id"],
            kickoff=kt,
            canonical_home=team_name.get(r["home_team_id"], r["home_team_id"]),
            canonical_away=team_name.get(r["away_team_id"], r["away_team_id"]),
            pinnacle_polls=[r],
            polymarket_outcomes=defaultdict(list),
        )

    for r in pm_rows:
        cand = [k for k in matches if k[0] == r["home_team_id"] and k[1] == r["away_team_id"]]
        if not cand:
            continue
        key = min(cand, key=lambda k: k[2])
        matches[key].polymarket_outcomes[r["outcome_side"]].append(r)

    for m in matches.values():
        m.pinnacle_polls.sort(
            key=lambda r: _parse_ts(r["polled_at"]) or datetime.max.replace(tzinfo=UTC)
        )
        for side in m.polymarket_outcomes:
            m.polymarket_outcomes[side].sort(
                key=lambda r: _parse_ts(r["polled_at"]) or datetime.max.replace(tzinfo=UTC)
            )

    return matches


def _pinnacle_fair_at(m: Match, t: datetime):
    """Return (fair_h, fair_d, fair_a) at time t, or None if no fresh Pinnacle."""
    pin = _nearest(m.pinnacle_polls, t, max_dt_sec=STALENESS_SEC)
    if not pin:
        return None
    try:
        fair = devig_3way(float(pin["odds_home"]), float(pin["odds_draw"]), float(pin["odds_away"]))
    except (ValueError, TypeError):
        return None
    return (fair.home, fair.draw, fair.away)


def _polymarket_mid_at(m: Match, t: datetime, tolerance_sec: float):
    """Return {side: mid} for all 3 outcomes at time t, or None if any missing."""
    out = {}
    for side in ("home", "draw", "away"):
        rows = m.polymarket_outcomes.get(side, [])
        row = _nearest(rows, t, max_dt_sec=tolerance_sec)
        if not row:
            return None
        mid = _mid(row)
        if mid is None:
            return None
        out[side] = mid
    return out


def find_entries(m: Match) -> list[dict]:
    """First poll in the window where |div| >= threshold, per outcome.

    Anchors on home-outcome poll timestamps (same as analyze_divergence) since
    all 3 Polymarket outcomes poll within the same cycle. An "entry" is recorded
    the first time any outcome crosses the threshold, but we record *per outcome*
    so one match can contribute up to 3 entries (home, draw, away — each
    the first time that specific side crosses threshold).
    """
    window_start = m.kickoff - WINDOW_BEFORE_KICKOFF
    home_polls = m.polymarket_outcomes.get("home", [])
    entries_by_side: dict[str, dict] = {}

    for pm_home in home_polls:
        t = _parse_ts(pm_home["polled_at"])
        if not t or not (window_start <= t <= m.kickoff):
            continue
        fair = _pinnacle_fair_at(m, t)
        if fair is None:
            continue
        mids = _polymarket_mid_at(m, t, tolerance_sec=STALENESS_SEC)
        if mids is None:
            continue
        for side_idx, side in enumerate(("home", "draw", "away")):
            if side in entries_by_side:
                continue
            div = fair[side_idx] - mids[side]
            if abs(div) >= ENTRY_THRESHOLD:
                entries_by_side[side] = {
                    "t0": t,
                    "side": side,
                    "div0": div,
                    "mid0": mids[side],
                    "fair0": fair[side_idx],
                    "minutes_to_kickoff": (m.kickoff - t).total_seconds() / 60.0,
                }
    return list(entries_by_side.values())


def sample_forward(m: Match, entry: dict) -> dict:
    """For each horizon, measure favourable move of Polymarket mid."""
    results: dict[int, dict[str, Any] | None] = {}
    for horizon in HORIZONS_MIN:
        t_target = entry["t0"] + timedelta(minutes=horizon)
        tolerance_sec = horizon * 60 * HORIZON_TOLERANCE
        rows = m.polymarket_outcomes.get(entry["side"], [])
        row = _nearest(rows, t_target, max_dt_sec=tolerance_sec)
        if not row:
            results[horizon] = None
            continue
        mid_k = _mid(row)
        if mid_k is None:
            results[horizon] = None
            continue
        # div0 > 0 means Polymarket is below fair (cheap). Favourable = mid rises.
        # div0 < 0 means Polymarket is above fair (expensive). Favourable = mid falls.
        raw_move = mid_k - entry["mid0"]
        favourable = raw_move if entry["div0"] > 0 else -raw_move
        results[horizon] = {
            "mid_k": mid_k,
            "raw_move_cents": raw_move * 100,
            "favourable_cents": favourable * 100,
        }
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    matches = load_data(sb)
    if not matches:
        print("No matches in snapshots yet.")
        return 0

    all_entries: list[tuple[Match, dict, dict]] = []
    for m in matches.values():
        for entry in find_entries(m):
            forward = sample_forward(m, entry)
            all_entries.append((m, entry, forward))

    print("=" * 78)
    print("FLIP FEASIBILITY: pre-match mean-reversion analysis")
    print("=" * 78)
    print(f"\nMatches indexed:  {len(matches)}")
    print(
        f"Qualifying entries (|div0| >= {ENTRY_THRESHOLD * 100:.0f}¢ in T-120→0):  "
        f"{len(all_entries)}"
    )

    if not all_entries:
        print(
            "\nNo qualifying entries yet. Logger needs more matches in the "
            "pre-match window. Re-run when the 48h log has progressed."
        )
        return 0

    print(
        "\nPer-horizon follow-through (favourable move, signed so positive = "
        "Polymarket moved toward Pinnacle fair):\n"
    )
    header = (
        f"  {'horizon':>9}  {'n':>3}  {'median':>8}  {'p25':>7}  "
        f"{'p75':>7}  {'P(≥1.5¢)':>9}  {'P(<0)':>7}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))

    for horizon in HORIZONS_MIN:
        vals = [f[horizon]["favourable_cents"] for _, _, f in all_entries if f[horizon] is not None]
        if not vals:
            print(f"  {horizon:>6}min  {'—':>3}  (no samples at this horizon yet)")
            continue
        p_target = sum(1 for v in vals if v >= FLIP_TARGET_CENTS) / len(vals)
        p_adverse = sum(1 for v in vals if v < 0) / len(vals)
        vs = sorted(vals)
        p25 = vs[len(vs) // 4]
        p75 = vs[(3 * len(vs)) // 4]
        print(
            f"  {horizon:>6}min  {len(vals):>3}  "
            f"{median(vals):>+7.2f}¢  {p25:>+6.2f}¢  {p75:>+6.2f}¢  "
            f"{p_target * 100:>7.1f}%  {p_adverse * 100:>5.1f}%"
        )

    print("\nReading:")
    print("  median  — typical favourable move at the horizon (cents)")
    print("  P(≥1.5¢) — probability of hitting the flip sell target")
    print("  P(<0)   — probability Polymarket moved AGAINST us")
    print("\nFlip is viable if a horizon in [15, 60] min shows P(≥1.5¢) >= 40%")
    print("and P(<0) <= 35%.")

    if args.verbose:
        print("\n" + "-" * 78)
        print("PER-ENTRY DETAIL")
        print("-" * 78)
        for m, entry, forward in all_entries:
            print(
                f"\n  {m.canonical_home} vs {m.canonical_away} "
                f"({m.kickoff.strftime('%m-%d %H:%M')} UTC)"
            )
            print(
                f"    entry @ T{-entry['minutes_to_kickoff']:+.0f}min  "
                f"side={entry['side']:>4}  "
                f"div0={entry['div0'] * 100:+.2f}¢  "
                f"mid0={entry['mid0']:.3f}  fair0={entry['fair0']:.3f}"
            )
            for h in HORIZONS_MIN:
                f = forward[h]
                if f is None:
                    print(f"    +{h:>3}min  (no sample in window)")
                else:
                    print(
                        f"    +{h:>3}min  mid={f['mid_k']:.3f}  "
                        f"favourable={f['favourable_cents']:+.2f}¢"
                    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
