"""Tests for polysport.strategy.moneyline.

Coverage targets every gate + every exit branch + every sizing bracket from
STRATEGY.md. Failures here mean the strategy is no longer faithful to the
spec — fix the strategy, do not loosen the test thresholds.
"""

from __future__ import annotations

import pytest
from polysport.strategy.moneyline import (
    DEPTH_MIN_USD,
    EARLY_EXIT_PIN_CONFIRM,
    EARLY_EXIT_PM_MOVE,
    EntryRejected,
    EntryRejection,
    EntrySignal,
    Outcome,
    PendingAction,
    PositionAction,
    compute_stake_and_cap,
    evaluate_entry,
    evaluate_pending_order,
    evaluate_position,
)

# --- Helpers ---------------------------------------------------------------


def _outcome(
    side: str = "home",
    fair: float = 0.50,
    bid: float = 0.47,
    ask: float = 0.49,
    depth: float = 1000.0,
) -> Outcome:
    """Build an Outcome with sensible defaults; override per-test."""
    return Outcome(
        side=side,  # type: ignore[arg-type]
        fair=fair,
        best_bid=bid,
        best_ask=ask,
        depth_usd=depth,
    )


def _three_way(
    home_fair: float = 0.50,
    draw_fair: float = 0.30,
    away_fair: float = 0.20,
    home_bid: float = 0.47,
    home_ask: float = 0.49,
    draw_bid: float = 0.28,
    draw_ask: float = 0.30,
    away_bid: float = 0.18,
    away_ask: float = 0.20,
    depth: float = 1000.0,
) -> dict:
    return {
        "home": _outcome("home", home_fair, home_bid, home_ask, depth),
        "draw": _outcome("draw", draw_fair, draw_bid, draw_ask, depth),
        "away": _outcome("away", away_fair, away_bid, away_ask, depth),
    }


# --- Outcome math ----------------------------------------------------------


def test_outcome_mid_and_divergence() -> None:
    o = _outcome(fair=0.55, bid=0.48, ask=0.50)
    assert o.mid == pytest.approx(0.49)
    assert o.divergence == pytest.approx(0.06)
    assert o.spread == pytest.approx(0.02)


# --- Entry rule ------------------------------------------------------------


def test_entry_fires_when_underpriced_outcome_clears_all_gates() -> None:
    # Home fair=0.55, mid=0.49 → div=+6c. Plenty of edge.
    sig = evaluate_entry(
        _three_way(home_fair=0.55, home_bid=0.48, home_ask=0.50),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(sig, EntrySignal)
    assert sig.target_outcome == "home"
    assert sig.side == "buy"
    # 0.5c inside best ask = 0.50 − 0.005 = 0.495.
    assert sig.limit_price == pytest.approx(0.495)
    assert sig.expected_edge == pytest.approx(0.06)


def test_entry_rejects_below_2c_threshold() -> None:
    # 1.5c divergence — below 2c threshold.
    rej = evaluate_entry(
        _three_way(home_fair=0.505, home_bid=0.48, home_ask=0.50),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.DIVERGENCE_BELOW_THRESHOLD in rej.reasons


def test_entry_rejects_when_all_outcomes_overpriced() -> None:
    # Sum of fair = 0.95 < sum of mids ≈ 1.05 → every outcome overpriced.
    rej = evaluate_entry(
        _three_way(
            home_fair=0.40,
            draw_fair=0.30,
            away_fair=0.25,
            home_bid=0.48,
            home_ask=0.50,
            draw_bid=0.32,
            draw_ask=0.34,
            away_bid=0.27,
            away_ask=0.29,
        ),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.NO_POSITIVE_DIV in rej.reasons


def test_entry_rejects_wide_spread() -> None:
    # 4c spread on home (max is 3c).
    rej = evaluate_entry(
        _three_way(home_fair=0.55, home_bid=0.46, home_ask=0.50),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.SPREAD_TOO_WIDE in rej.reasons


def test_entry_rejects_thin_book() -> None:
    rej = evaluate_entry(
        _three_way(home_fair=0.55, home_bid=0.48, home_ask=0.50, depth=DEPTH_MIN_USD - 1),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.INSUFFICIENT_DEPTH in rej.reasons


def test_entry_rejects_stale_pinnacle() -> None:
    rej = evaluate_entry(
        _three_way(home_fair=0.55, home_bid=0.48, home_ask=0.50),
        pinnacle_staleness_sec=61.0,
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.PINNACLE_STALE in rej.reasons


def test_entry_rejects_extreme_favourite() -> None:
    # Home fair >= 0.80 → skip whole match (numerical instability).
    # Underdog has 6c divergence so otherwise this would be a buy.
    rej = evaluate_entry(
        _three_way(
            home_fair=0.85,
            draw_fair=0.10,
            away_fair=0.05,
            home_bid=0.83,
            home_ask=0.85,
            draw_bid=0.08,
            draw_ask=0.10,
            away_bid=0.04,
            away_ask=0.06,
        ),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.EXTREME_FAVOURITE in rej.reasons


def test_entry_rejects_existing_position() -> None:
    rej = evaluate_entry(
        _three_way(home_fair=0.55, home_bid=0.48, home_ask=0.50),
        pinnacle_staleness_sec=10.0,
        has_position=True,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.EXISTING_POSITION in rej.reasons


def test_entry_picks_max_positive_divergence_across_outcomes() -> None:
    # Home: div=+1c (under threshold). Draw: div=+3c (qualifies and is the max).
    sig = evaluate_entry(
        _three_way(
            home_fair=0.50,
            draw_fair=0.32,
            away_fair=0.20,
            home_bid=0.48,
            home_ask=0.50,  # mid=0.49, div=+1c
            draw_bid=0.27,
            draw_ask=0.29,  # mid=0.28, div=+4c (above threshold) → wait, 0.32-0.28=0.04
            away_bid=0.18,
            away_ask=0.20,  # mid=0.19, div=+1c
        ),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(sig, EntrySignal)
    assert sig.target_outcome == "draw"
    assert sig.expected_edge == pytest.approx(0.04, abs=0.001)


def test_entry_aggregates_multiple_rejection_reasons() -> None:
    rej = evaluate_entry(
        _three_way(
            home_fair=0.55,
            home_bid=0.46,
            home_ask=0.50,  # spread = 4c (too wide)
            depth=100.0,  # too thin
        ),
        pinnacle_staleness_sec=120.0,  # stale
        has_position=False,
    )
    assert isinstance(rej, EntryRejected)
    assert EntryRejection.SPREAD_TOO_WIDE in rej.reasons
    assert EntryRejection.INSUFFICIENT_DEPTH in rej.reasons
    assert EntryRejection.PINNACLE_STALE in rej.reasons


# --- Pending-order decision ------------------------------------------------


def test_pending_holds_far_from_kickoff() -> None:
    o = _outcome(fair=0.55, bid=0.48, ask=0.50)
    d = evaluate_pending_order(
        minutes_to_kickoff=60.0, current_outcome=o, pinnacle_moved_toward_pm=False
    )
    assert d.action == PendingAction.HOLD


def test_pending_expires_at_t_minus_5() -> None:
    o = _outcome(fair=0.55, bid=0.48, ask=0.50)
    d = evaluate_pending_order(
        minutes_to_kickoff=4.0, current_outcome=o, pinnacle_moved_toward_pm=False
    )
    assert d.action == PendingAction.EXPIRE_AND_SKIP


def test_pending_hybrid_fok_at_t_minus_10_when_edge_persists() -> None:
    # Div still 3c, Pinnacle hasn't chased.
    o = _outcome(fair=0.53, bid=0.48, ask=0.52)  # mid=0.50, div=+3c
    d = evaluate_pending_order(
        minutes_to_kickoff=8.0, current_outcome=o, pinnacle_moved_toward_pm=False
    )
    assert d.action == PendingAction.HYBRID_FOK_TAKER


def test_pending_skips_at_t_minus_10_when_pinnacle_chased() -> None:
    o = _outcome(fair=0.53, bid=0.48, ask=0.52)
    d = evaluate_pending_order(
        minutes_to_kickoff=8.0, current_outcome=o, pinnacle_moved_toward_pm=True
    )
    assert d.action == PendingAction.EXPIRE_AND_SKIP


def test_pending_skips_at_t_minus_10_when_divergence_collapsed() -> None:
    # Div now 1c — below threshold, no point chasing.
    o = _outcome(fair=0.51, bid=0.48, ask=0.52)
    d = evaluate_pending_order(
        minutes_to_kickoff=8.0, current_outcome=o, pinnacle_moved_toward_pm=False
    )
    assert d.action == PendingAction.EXPIRE_AND_SKIP


# --- Position decision -----------------------------------------------------


def test_position_holds_by_default() -> None:
    o = _outcome(fair=0.55, bid=0.48, ask=0.50)  # mid 0.49
    d = evaluate_position(entry_mid=0.49, entry_fair=0.55, current_outcome=o, current_fair=0.55)
    assert d.action == PositionAction.HOLD


def test_position_holds_when_only_pm_moves_against() -> None:
    # PM dropped 6c but Pinnacle didn't confirm.
    o = _outcome(fair=0.55, bid=0.42, ask=0.44)  # mid=0.43, was 0.49 → -6c
    d = evaluate_position(entry_mid=0.49, entry_fair=0.55, current_outcome=o, current_fair=0.55)
    assert d.action == PositionAction.HOLD


def test_position_holds_when_only_pinnacle_moves() -> None:
    o = _outcome(fair=0.50, bid=0.48, ask=0.50)  # mid same
    d = evaluate_position(entry_mid=0.49, entry_fair=0.55, current_outcome=o, current_fair=0.50)
    assert d.action == PositionAction.HOLD


def test_position_early_exits_when_both_feeds_confirm() -> None:
    # PM mid drops 6c (>= 5c threshold). Pinnacle drops 4c (>= 3c confirm).
    o = _outcome(fair=0.51, bid=0.42, ask=0.44)  # mid = 0.43
    d = evaluate_position(entry_mid=0.49, entry_fair=0.55, current_outcome=o, current_fair=0.51)
    assert d.action == PositionAction.EARLY_FOK_EXIT
    assert d.detail["pm_move"] <= -EARLY_EXIT_PM_MOVE
    assert d.detail["pin_move"] <= -EARLY_EXIT_PIN_CONFIRM


def test_position_just_past_thresholds_exits() -> None:
    # PM down 6c, Pinnacle confirms by 4c. Both clearly past threshold,
    # avoiding the FP boundary at exactly -0.05 / -0.03.
    o = _outcome(fair=0.51, bid=0.42, ask=0.44)  # mid = 0.43, was 0.49 → -6c
    d = evaluate_position(entry_mid=0.49, entry_fair=0.55, current_outcome=o, current_fair=0.51)
    assert d.action == PositionAction.EARLY_FOK_EXIT


def test_position_holds_just_short_of_thresholds() -> None:
    # PM down 4c (under 5c PM threshold), Pinnacle down 4c — PM doesn't
    # confirm even though Pinnacle does. No exit.
    o = _outcome(fair=0.51, bid=0.44, ask=0.46)  # mid = 0.45, was 0.49 → -4c
    d = evaluate_position(entry_mid=0.49, entry_fair=0.55, current_outcome=o, current_fair=0.51)
    assert d.action == PositionAction.HOLD


# --- Sizing ---------------------------------------------------------------


# --- Strategy ↔ Simulator price alignment ----------------------------------
# Audit 2026-04-26: the strategy and simulator must compute the same maker
# entry price. Drift here silently biases shadow PnL.


def test_strategy_and_sim_compute_same_entry_price() -> None:
    from polysport.sim.honest_fill import EntrySignal as SimEntrySignal
    from polysport.sim.honest_fill import _compute_entry_price

    sig = evaluate_entry(
        _three_way(home_fair=0.55, home_bid=0.48, home_ask=0.50),
        pinnacle_staleness_sec=10.0,
        has_position=False,
    )
    assert isinstance(sig, EntrySignal)

    sim_sig = SimEntrySignal(
        match_id="test",
        side="buy",
        outcome_side=sig.target_outcome,
        polymarket_mid=(0.48 + 0.50) / 2,
        polymarket_best_ask=0.50,
        polymarket_best_bid=0.48,
        pinnacle_fair=0.55,
        notional_usd=5.0,
        t_minutes_to_kick=60.0,
    )
    sim_entry = _compute_entry_price(sim_sig)

    assert sig.limit_price == pytest.approx(sim_entry), (
        f"Strategy posts at {sig.limit_price}, sim fills at {sim_entry}. "
        "These must be identical or paper_trades will record biased PnL."
    )


# --- Sizing ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("bankroll", "expected_stake", "expected_cap"),
    [
        (50.0, 0.0, 0),
        (99.99, 0.0, 0),
        (100.0, 5.0, 3),
        (250.0, 5.0, 3),
        (499.99, 5.0, 3),
        (500.0, 10.0, 3),
        (999.99, 10.0, 3),
        (1_000.0, 20.0, 4),
        (2_500.0, 50.0, 4),
        (4_999.99, 100.00, 4),  # 2% of 4999.99 ≈ 99.9998 → rounds to 100.00
        (5_000.0, 100.0, 5),
        (10_000.0, 100.0, 5),  # capped
        (1_000_000.0, 100.0, 5),  # still capped
    ],
)
def test_compute_stake_and_cap_brackets(
    bankroll: float, expected_stake: float, expected_cap: int
) -> None:
    d = compute_stake_and_cap(bankroll)
    assert d.stake_usd == pytest.approx(expected_stake)
    assert d.max_concurrent == expected_cap
