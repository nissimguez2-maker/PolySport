"""Phase 1 backtest harness — replay logged snapshots through the full
strategy + honest-fill simulator pipeline.

What this answers
-----------------
analyze_divergence.py asks: did any poll see |div| >= 2c in the trade
window? That's the Phase 1 gate. But "touched 2c" doesn't mean the entry
rule actually fires — the full rule layers in spread, depth, Pinnacle
freshness, fav-prob, and existing-position gates. The backtest replays
the full rule against the same data and measures:

  1. Entry signals fired (vs touch rate)
  2. Per-rejection breakdown (which gate is doing the rejecting?)
  3. Per-entry simulated round-trip net PnL via honest_fill
  4. Per-match best signal observed

This is the cheapest validation of the strategy code we have today,
and it surfaces strategy/sim drift before any real money moves.

Pipeline
--------
  1. Load 30 days of paginated snapshots (polysport.data.snapshots).
  2. Index by match (home_id, away_id) — kickoff-drift dedup window
     of 10 minutes.
  3. For each match, walk the home-outcome PM polls (densest signal)
     and at each poll moment:
        a. Snap to the nearest PM draw + away polls.
        b. Snap to the nearest Pinnacle poll. Compute staleness.
        c. Build moneyline.Outcome triple. Devig Pinnacle for fair.
        d. Call moneyline.evaluate_entry. If it fires, record signal.
        e. For each fired signal, build sim.EntrySignal and call
           simulate_round_trip with hold-to-settlement (settlement=None
           => expected PnL using fair as p_win prior).
  4. Aggregate and print.

The expected-PnL semantics matter: for shadow mode we don't know which
side wins, but we know the prior (Pinnacle's de-vigged fair). EV(net_pnl)
under that prior is the right honest number for shadow accounting. Phase
2 will replace this with realized settlements once enough matches have
played out post-log.

Limitations
-----------
- Uses each match's one largest-edge entry; doesn't model the
  "max one leg per match, expire at T-5" lifecycle yet.
- All entries assume hold-to-settlement (the primary track). Hybrid
  fallback / early exit branches not exercised.
- Single notional ($5 default, matching Stage 1 sizing).

Cross-check
-----------
Touch rate (analyze_divergence.py) >= entry rate (this script). Difference
is gates: spread/depth/staleness/fav-prob. If touch rate is meaningful
but entry rate is near-zero, that's a signal that one of those gates is
killing every potential trade — worth investigating before Phase 2.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import mean, median

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.data.snapshots import load_pinnacle_pm_rows
from polysport.math.devig import devig_3way
from polysport.sim.honest_fill import (
    EntrySignal as SimEntrySignal,
)
from polysport.sim.honest_fill import (
    ExitPlan,
    simulate_round_trip,
)
from polysport.strategy.moneyline import (
    EntryRejected,
    EntrySignal,
    Outcome,
    evaluate_entry,
)

# Match the analysis-script's settings.
WINDOW_BEFORE_KICKOFF = timedelta(minutes=120)
KICKOFF_DEDUP_WINDOW = timedelta(minutes=10)
PIN_FRESHNESS_PAIRING_SEC = 60.0  # ±60s when picking nearest Pinnacle for a PM moment
DEFAULT_NOTIONAL_USD = 5.0  # STRATEGY.md Stage 1


@dataclass
class MatchBucket:
    home_id: str
    away_id: str
    kickoff: datetime
    home_name: str
    away_name: str
    pinnacle_polls: list[dict]
    pm_by_outcome: dict[str, list[dict]]


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _nearest(rows: list[dict], t: datetime) -> dict | None:
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


def _index_matches(
    pin_rows: list[dict], pm_rows: list[dict], team_name: dict[str, str]
) -> dict[tuple, MatchBucket]:
    """Same dedup logic as analyze_divergence.py — 10min kickoff-drift window."""
    buckets: dict[tuple, MatchBucket] = {}
    for r in pin_rows:
        kt = _parse_ts(r["commence_time"])
        if not kt:
            continue
        existing_key = next(
            (
                k
                for k in buckets
                if k[0] == r["home_team_id"]
                and k[1] == r["away_team_id"]
                and abs((k[2] - kt).total_seconds()) <= KICKOFF_DEDUP_WINDOW.total_seconds()
            ),
            None,
        )
        if existing_key is not None:
            buckets[existing_key].pinnacle_polls.append(r)
            continue
        key = (r["home_team_id"], r["away_team_id"], kt)
        buckets[key] = MatchBucket(
            home_id=r["home_team_id"],
            away_id=r["away_team_id"],
            kickoff=kt,
            home_name=team_name.get(r["home_team_id"], r["home_team_id"]),
            away_name=team_name.get(r["away_team_id"], r["away_team_id"]),
            pinnacle_polls=[r],
            pm_by_outcome=defaultdict(list),
        )

    for r in pm_rows:
        cand = [k for k in buckets if k[0] == r["home_team_id"] and k[1] == r["away_team_id"]]
        if not cand:
            continue
        key = min(cand, key=lambda k: k[2])
        buckets[key].pm_by_outcome[r["outcome_side"]].append(r)

    for b in buckets.values():
        b.pinnacle_polls.sort(
            key=lambda r: _parse_ts(r["polled_at"]) or datetime.max.replace(tzinfo=UTC)
        )
        for side in b.pm_by_outcome:
            b.pm_by_outcome[side].sort(
                key=lambda r: _parse_ts(r["polled_at"]) or datetime.max.replace(tzinfo=UTC)
            )
    return buckets


def _depth_min_usd(pm_row: dict) -> float | None:
    b = pm_row.get("best_bid_depth_usd")
    a = pm_row.get("best_ask_depth_usd")
    if b is None or a is None:
        return None
    return min(float(b), float(a))


@dataclass
class BacktestEntry:
    match_label: str
    kickoff: datetime
    poll_time: datetime
    minutes_to_kick: float
    signal: EntrySignal
    sim_net_pnl: float
    sim_gross_pnl: float
    fair: float
    mid: float


def _replay_match(
    bucket: MatchBucket, notional_usd: float
) -> tuple[list[BacktestEntry], Counter, int]:
    """Replay one match. Returns (one BacktestEntry per fired signal,
    rejection counter, total polls evaluated).

    Strategy spec is single-leg-per-match; we collect every poll's
    decision but report only the *first* fired signal per match
    downstream so the sim doesn't double-count.
    """
    fires: list[BacktestEntry] = []
    rejections: Counter = Counter()
    polls_evaluated = 0

    home_polls = bucket.pm_by_outcome.get("home", [])
    draw_polls = bucket.pm_by_outcome.get("draw", [])
    away_polls = bucket.pm_by_outcome.get("away", [])

    window_start = bucket.kickoff - WINDOW_BEFORE_KICKOFF

    for pm_home in home_polls:
        pm_time = _parse_ts(pm_home["polled_at"])
        if not pm_time or not (window_start <= pm_time <= bucket.kickoff):
            continue

        pm_draw = _nearest(draw_polls, pm_time)
        pm_away = _nearest(away_polls, pm_time)
        if not (pm_draw and pm_away):
            continue

        pin = _nearest(bucket.pinnacle_polls, pm_time)
        if not pin:
            continue
        pin_time = _parse_ts(pin["polled_at"])
        if pin_time is None:
            continue
        pin_staleness = abs((pin_time - pm_time).total_seconds())
        if pin_staleness > PIN_FRESHNESS_PAIRING_SEC:
            # We only consider polls where Pinnacle was actually fresh at
            # decision time. evaluate_entry would also reject, but we want
            # to count *all* poll moments where the strategy could have
            # decided — not poll moments where we lacked the inputs.
            continue

        try:
            fair = devig_3way(
                float(pin["odds_home"]), float(pin["odds_draw"]), float(pin["odds_away"])
            )
        except (ValueError, TypeError):
            continue

        outcomes_by_side: dict[str, Outcome] = {}
        valid = True
        for side, pm_row, fair_p in (
            ("home", pm_home, fair.home),
            ("draw", pm_draw, fair.draw),
            ("away", pm_away, fair.away),
        ):
            bid = pm_row.get("best_bid")
            ask = pm_row.get("best_ask")
            if bid is None or ask is None:
                valid = False
                break
            depth = _depth_min_usd(pm_row)
            if depth is None:
                # No depth column = we can't evaluate the depth gate. Skip.
                valid = False
                break
            outcomes_by_side[side] = Outcome(
                side=side,  # type: ignore[arg-type]
                fair=fair_p,
                best_bid=float(bid),
                best_ask=float(ask),
                depth_usd=depth,
            )
        if not valid:
            continue

        polls_evaluated += 1

        decision = evaluate_entry(
            outcomes_by_side,  # type: ignore[arg-type]
            pinnacle_staleness_sec=pin_staleness,
            has_position=False,  # backtest never holds across polls
        )

        if isinstance(decision, EntryRejected):
            for r in decision.reasons:
                rejections[r] += 1
            continue

        # Fired. Build sim signal and simulate hold-to-settlement (EV).
        target = outcomes_by_side[decision.target_outcome]
        sim_sig = SimEntrySignal(
            match_id=f"{bucket.home_name}_{bucket.away_name}_{int(bucket.kickoff.timestamp())}",
            side="buy",
            outcome_side=decision.target_outcome,
            polymarket_mid=target.mid,
            polymarket_best_ask=target.best_ask,
            polymarket_best_bid=target.best_bid,
            pinnacle_fair=target.fair,
            notional_usd=notional_usd,
            t_minutes_to_kick=(bucket.kickoff - pm_time).total_seconds() / 60.0,
        )
        result = simulate_round_trip(entry=sim_sig, exit_plan=ExitPlan(kind="hold-to-settlement"))
        fires.append(
            BacktestEntry(
                match_label=f"{bucket.home_name} vs {bucket.away_name}",
                kickoff=bucket.kickoff,
                poll_time=pm_time,
                minutes_to_kick=(bucket.kickoff - pm_time).total_seconds() / 60.0,
                signal=decision,
                sim_net_pnl=result.net_pnl,
                sim_gross_pnl=result.gross_pnl,
                fair=target.fair,
                mid=target.mid,
            )
        )

    return fires, rejections, polls_evaluated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--notional", type=float, default=DEFAULT_NOTIONAL_USD, help="Stake per trade in USD."
    )
    parser.add_argument(
        "--days-back", type=int, default=30, help="History window for snapshot load."
    )
    parser.add_argument(
        "--first-only",
        action="store_true",
        default=True,
        help="Per match, count only the FIRST fired signal — matches the "
        "single-leg-per-match strategy rule (default on).",
    )
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    print(f"Loading {args.days_back} days of snapshots…", flush=True)
    pin_rows, pm_rows, team_name = load_pinnacle_pm_rows(sb, days_back=args.days_back)
    print(f"  pinnacle rows: {len(pin_rows)}\n  polymarket rows: {len(pm_rows)}", flush=True)

    buckets = _index_matches(pin_rows, pm_rows, team_name)
    print(f"  matches indexed: {len(buckets)}", flush=True)

    all_fires: list[BacktestEntry] = []
    rejections: Counter = Counter()
    total_polls = 0
    matches_with_signal = 0
    per_match_first: list[BacktestEntry] = []

    for bucket in buckets.values():
        fires, rej, polls = _replay_match(bucket, notional_usd=args.notional)
        rejections.update(rej)
        total_polls += polls
        if fires:
            matches_with_signal += 1
            per_match_first.append(fires[0])
        all_fires.extend(fires)

    print("\n" + "=" * 78)
    print("PHASE 1 BACKTEST RESULTS")
    print("=" * 78)

    print(f"\nMatches with at least one fired entry: {matches_with_signal} / {len(buckets)}")
    if buckets:
        pct = 100 * matches_with_signal / len(buckets)
        print(f"  Match-level entry rate: {pct:.1f}%")
    print(f"Polls evaluated: {total_polls}")
    print(f"Total signals fired (any poll): {len(all_fires)}")

    if rejections:
        total_rej = sum(rejections.values())
        print(f"\n--- rejection breakdown ({total_rej} rejected polls) ---")
        for reason, count in rejections.most_common():
            r = str(reason).replace("EntryRejection.", "")
            print(f"  {r:<30} {count:6d}  ({100 * count / total_rej:5.1f}%)")

    entries_for_pnl = per_match_first if args.first_only else all_fires
    if entries_for_pnl:
        edges = [e.signal.expected_edge * 100 for e in entries_for_pnl]
        net_pnls = [e.sim_net_pnl for e in entries_for_pnl]
        wins = sum(1 for p in net_pnls if p > 0)
        print(
            f"\n--- per-entry summary (n={len(entries_for_pnl)}, "
            f"{'first fire per match' if args.first_only else 'every fire'}) ---"
        )
        print(f"  expected_edge cents: median={median(edges):5.2f}  mean={mean(edges):5.2f}")
        print(
            f"  sim net_pnl @ ${args.notional:.0f} stake: "
            f"median={median(net_pnls):+.3f}  mean={mean(net_pnls):+.3f}  "
            f"sum={sum(net_pnls):+.3f}"
        )
        print(f"  positive-EV entries: {wins} / {len(entries_for_pnl)}")

    if per_match_first:
        print("\n--- per-match first-fire detail ---")
        for e in sorted(per_match_first, key=lambda x: x.kickoff):
            kt = e.kickoff.strftime("%Y-%m-%d %H:%M UTC")
            print(
                f"  {kt}  T-{e.minutes_to_kick:5.1f}m  "
                f"{e.match_label[:46]:<46}  edge={e.signal.expected_edge * 100:5.2f}c  "
                f"target={e.signal.target_outcome:<4}  "
                f"price={e.signal.limit_price:.4f}  "
                f"sim_pnl={e.sim_net_pnl:+.3f}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
