"""Hold-track entry/exit logic for 3-way soccer moneyline.

Pure functions over snapshot inputs — no I/O, no state, no time-of-day awareness
(callers pass the relevant timestamps in). This makes the strategy module
trivially testable against logged Phase 1 data and against the honest-fill
simulator.

The shape mirrors STRATEGY.md sections "Entry rule" and "Exit rule (hold
track, primary)". Three decision points in the order's lifecycle:

  1. evaluate_entry          — should we post a new maker order?
  2. evaluate_pending_order  — what do we do with an unfilled maker order?
  3. evaluate_position       — what do we do with a filled position?

Plus compute_stake_and_cap() implementing the bankroll-tier sizing table.

All thresholds are sourced from STRATEGY.md. Do not loosen any of them
without calibrated data justifying the change (this is a non-negotiable
project rule, see STRATEGY.md "Non-negotiable rules").
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Literal

# --- Constants pulled directly from STRATEGY.md -----------------------------

DIVERGENCE_THRESHOLD = 0.02  # |fair_i − mid_i| >= 2c
SPREAD_MAX = 0.03  # best_ask − best_bid <= 3c
DEPTH_MIN_USD = 500.0  # >= $500 of size at best price
PINNACLE_STALENESS_MAX_SEC = 60.0  # Pinnacle snapshot must be < 60s old
FAV_PROB_MAX = 0.80  # skip extreme favourites (numerical instability)
MAKER_OFFSET = 0.005  # post limit 0.5c inside the current best

# Order lifecycle times relative to kickoff.
ORDER_EXPIRE_BEFORE_KICKOFF = timedelta(minutes=5)  # cancel unfilled maker at T-5
HYBRID_FALLBACK_BEFORE_KICKOFF = timedelta(minutes=10)  # T-10 fallback decision

# Adverse-move thresholds for early exit on a filled position.
EARLY_EXIT_PM_MOVE = 0.05  # PM mid moved >= 5c against position
EARLY_EXIT_PIN_CONFIRM = 0.03  # AND Pinnacle confirms >= 3c same direction


OutcomeSide = Literal["home", "draw", "away"]
Side = Literal["buy", "sell"]


# --- Inputs -----------------------------------------------------------------


@dataclass(frozen=True)
class Outcome:
    """Single 3-way outcome at a specific poll moment."""

    side: OutcomeSide
    fair: float  # Pinnacle de-vigged fair probability
    best_bid: float  # Polymarket best bid for YES token
    best_ask: float  # Polymarket best ask for YES token
    depth_usd: float  # USD size at best price (worst of bid/ask)

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def divergence(self) -> float:
        """fair − mid. Positive => underpriced on Polymarket (buy YES)."""
        return self.fair - self.mid

    @property
    def spread(self) -> float:
        return self.best_ask - self.best_bid


# --- Entry decision ---------------------------------------------------------


class EntryRejection(StrEnum):
    NO_POSITIVE_DIV = "no_positive_divergence"  # all outcomes overpriced
    DIVERGENCE_BELOW_THRESHOLD = "divergence_below_threshold"
    SPREAD_TOO_WIDE = "spread_too_wide"
    INSUFFICIENT_DEPTH = "insufficient_depth"
    PINNACLE_STALE = "pinnacle_stale"
    EXTREME_FAVOURITE = "extreme_favourite"
    EXISTING_POSITION = "existing_position"


@dataclass(frozen=True)
class EntrySignal:
    """A green-lit entry. Caller posts a GTC post_only maker limit at limit_price."""

    target_outcome: OutcomeSide
    side: Side  # always "buy" — strategy only takes the underpriced leg
    limit_price: float
    expected_edge: float  # |divergence| at decision time
    fair: float
    mid: float


@dataclass(frozen=True)
class EntryRejected:
    reasons: tuple[EntryRejection, ...]
    detail: dict[str, float]  # numeric context for logging / dashboards


def evaluate_entry(
    outcomes: dict[OutcomeSide, Outcome],
    pinnacle_staleness_sec: float,
    has_position: bool,
) -> EntrySignal | EntryRejected:
    """STRATEGY.md "Entry rule" steps 1-6.

    Pick the most-underpriced outcome (max positive divergence). Apply all
    gates. Return either a posted-order recipe or a structured rejection.

    The strategy intentionally never short-sells: we always BUY the YES of
    an underpriced outcome rather than sell the YES of an overpriced one.
    This keeps single-leg-per-match and matches the maker-rebate path on
    Polymarket. If every outcome is overpriced (rare but possible — global
    Polymarket vig), we skip the match entirely.
    """
    if not outcomes:
        return EntryRejected(reasons=(EntryRejection.NO_POSITIVE_DIV,), detail={})

    # Step 4: pick the most-underpriced outcome.
    underpriced = [o for o in outcomes.values() if o.divergence > 0]
    if not underpriced:
        return EntryRejected(
            reasons=(EntryRejection.NO_POSITIVE_DIV,),
            detail={"max_div": max(o.divergence for o in outcomes.values())},
        )
    target = max(underpriced, key=lambda o: o.divergence)

    # Step 5: gates.
    reasons: list[EntryRejection] = []
    if target.divergence < DIVERGENCE_THRESHOLD:
        reasons.append(EntryRejection.DIVERGENCE_BELOW_THRESHOLD)
    if target.spread > SPREAD_MAX:
        reasons.append(EntryRejection.SPREAD_TOO_WIDE)
    if target.depth_usd < DEPTH_MIN_USD:
        reasons.append(EntryRejection.INSUFFICIENT_DEPTH)
    if pinnacle_staleness_sec >= PINNACLE_STALENESS_MAX_SEC:
        reasons.append(EntryRejection.PINNACLE_STALE)
    fav_prob = max(o.fair for o in outcomes.values())
    if fav_prob >= FAV_PROB_MAX:
        reasons.append(EntryRejection.EXTREME_FAVOURITE)
    if has_position:
        reasons.append(EntryRejection.EXISTING_POSITION)

    if reasons:
        return EntryRejected(
            reasons=tuple(reasons),
            detail={
                "divergence": target.divergence,
                "spread": target.spread,
                "depth_usd": target.depth_usd,
                "pinnacle_staleness_sec": pinnacle_staleness_sec,
                "fav_prob": fav_prob,
            },
        )

    # Step 6: post 0.5c inside best ask, on the underpriced side (= buy YES).
    limit_price = target.best_ask - MAKER_OFFSET
    return EntrySignal(
        target_outcome=target.side,
        side="buy",
        limit_price=limit_price,
        expected_edge=target.divergence,
        fair=target.fair,
        mid=target.mid,
    )


# --- Pending-order decision -------------------------------------------------


class PendingAction(StrEnum):
    HOLD = "hold"  # leave maker working
    EXPIRE_AND_SKIP = "expire_and_skip"  # T-5min cancel, no taker
    HYBRID_FOK_TAKER = "hybrid_fok_taker"  # T-10min fallback per STRATEGY


@dataclass(frozen=True)
class PendingDecision:
    action: PendingAction
    detail: dict[str, float]


def evaluate_pending_order(
    minutes_to_kickoff: float,
    current_outcome: Outcome,
    pinnacle_moved_toward_pm: bool,
) -> PendingDecision:
    """STRATEGY.md "Exit rule (hold track, primary)" — pre-fill arm.

    pinnacle_moved_toward_pm is True iff the Pinnacle fair has shifted
    toward the Polymarket mid since entry (i.e., the gap is closing from
    Pinnacle's side, meaning the edge wasn't real — Pinnacle was just
    catching up to PM, not the other way). When True we DO NOT chase
    with a taker, because the divergence the maker is parked against
    no longer represents an edge.
    """
    if minutes_to_kickoff <= ORDER_EXPIRE_BEFORE_KICKOFF.total_seconds() / 60:
        return PendingDecision(
            action=PendingAction.EXPIRE_AND_SKIP,
            detail={"minutes_to_kickoff": minutes_to_kickoff},
        )

    if minutes_to_kickoff <= HYBRID_FALLBACK_BEFORE_KICKOFF.total_seconds() / 60:
        # T-10 fallback evaluation.
        div_now = current_outcome.divergence
        if div_now >= DIVERGENCE_THRESHOLD and not pinnacle_moved_toward_pm:
            return PendingDecision(
                action=PendingAction.HYBRID_FOK_TAKER,
                detail={
                    "minutes_to_kickoff": minutes_to_kickoff,
                    "divergence_now": div_now,
                },
            )
        return PendingDecision(
            action=PendingAction.EXPIRE_AND_SKIP,
            detail={
                "minutes_to_kickoff": minutes_to_kickoff,
                "divergence_now": div_now,
                "pinnacle_chased": float(pinnacle_moved_toward_pm),
            },
        )

    return PendingDecision(action=PendingAction.HOLD, detail={})


# --- Position decision ------------------------------------------------------


class PositionAction(StrEnum):
    HOLD = "hold"
    EARLY_FOK_EXIT = "early_fok_exit"


@dataclass(frozen=True)
class PositionDecision:
    action: PositionAction
    detail: dict[str, float]


def evaluate_position(
    entry_mid: float,
    entry_fair: float,
    current_outcome: Outcome,
    current_fair: float,
) -> PositionDecision:
    """STRATEGY.md "Exit rule" — early exit arm.

    Adverse move definition (long position on YES):
      - PM mid drops by EARLY_EXIT_PM_MOVE since entry, AND
      - Pinnacle fair has confirmed by dropping >= EARLY_EXIT_PIN_CONFIRM.

    Both conditions required: PM-only moves can be noise; Pinnacle moves
    without PM following are not actionable; only when both feeds agree
    do we bail.
    """
    pm_move = current_outcome.mid - entry_mid  # negative => against us
    pin_move = current_fair - entry_fair  # negative => fair dropped

    pm_adverse = pm_move <= -EARLY_EXIT_PM_MOVE
    pin_confirms = pin_move <= -EARLY_EXIT_PIN_CONFIRM

    if pm_adverse and pin_confirms:
        return PositionDecision(
            action=PositionAction.EARLY_FOK_EXIT,
            detail={"pm_move": pm_move, "pin_move": pin_move},
        )
    return PositionDecision(
        action=PositionAction.HOLD, detail={"pm_move": pm_move, "pin_move": pin_move}
    )


# --- Sizing -----------------------------------------------------------------


@dataclass(frozen=True)
class SizingDecision:
    stake_usd: float
    max_concurrent: int


def compute_stake_and_cap(bankroll_usd: float) -> SizingDecision:
    """STRATEGY.md "Sizing" table.

    Brackets are inclusive on the lower bound. Below $100 the bot is below
    its operating floor and stake is 0 — caller should not be entering
    trades. We surface that as stake=0/max=0 rather than raising so the
    decision stays in pure-function land.
    """
    if bankroll_usd < 100:
        return SizingDecision(stake_usd=0.0, max_concurrent=0)
    if bankroll_usd < 500:
        return SizingDecision(stake_usd=5.0, max_concurrent=3)
    if bankroll_usd < 1_000:
        return SizingDecision(stake_usd=10.0, max_concurrent=3)
    if bankroll_usd < 5_000:
        return SizingDecision(stake_usd=round(0.02 * bankroll_usd, 2), max_concurrent=4)
    # >= $5,000: 2% capped at $100.
    return SizingDecision(stake_usd=min(0.02 * bankroll_usd, 100.0), max_concurrent=5)
