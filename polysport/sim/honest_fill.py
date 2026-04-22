"""Honest-fill simulator — the thing that kept PolyGuez from dying live.

Per STRATEGY.md: the single most likely cause of PolyGuez's shadow-vs-live
divergence (+$11k sim vs -$218 live) was spread-crossing bias in the fill
model. Shadow assumed mid-price fills; live required crossing the spread on
the maker-fallback FOK path. This module exists so the Phase 2 shadow run
uses a fill model that matches what actually happens on Polymarket.

Scaffold status: the fee function is the only piece we cannot pin down yet
because of the ongoing Polymarket docs / API / on-chain contract mismatch
(see conversation log). Injected as a parameter `fee_fn(p, notional) -> fee`
so that once one live test fill reveals the true formula, plugging it in is
one line — no refactor, no rerun of strategy logic.

What this module simulates today:
    A single trade's round trip given:
      - an entry signal (match state at T-X)
      - an entry plan (maker limit at entry_mid - 0.005 on the buy side)
      - an exit plan (hold-to-settlement or FOK taker at T-10min)
      - a fee function

    Returns a TradeResult with gross edge, fees paid per leg, net PnL, and
    enough diagnostics to populate the shadow-vs-honest-fill-sim PnL
    divergence metric in the graduation gate.

What this module does NOT simulate (yet):
    - Maker queue position / fill probability (Phase 2 will model this)
    - Partial fills (all-or-nothing for scaffold)
    - Polymarket order-book depth changes during the hold period
    - Pinnacle feed drops / staleness-gated skips
    - Flip-track exit fills (separate shell, Phase 2)

Usage sketch (strategy.moneyline.simulate_shadow_trade will call this):

    result = simulate_round_trip(
        entry=EntrySignal(
            match_id="...",
            side="buy",
            outcome_side="home",
            polymarket_mid=0.48,
            polymarket_best_ask=0.485,
            pinnacle_fair=0.50,
            notional_usd=5.0,
            t_minutes_to_kick=90,
        ),
        exit_plan=ExitPlan(kind="hold-to-settlement"),
        fee_fn=polymarket_fee_from_docs,   # <-- the one line to swap later
    )
    print(result.net_pnl, result.fee_entry, result.fee_exit)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal

# ─── Fee function hook ────────────────────────────────────────────────────
# Any callable matching this signature. Phase 1 placeholder uses the
# "peak 0.75% at p=0.50" formula (effective_fee = 3% * p * (1-p) of notional)
# which is consistent with the Polymarket help-center worked example. The
# real formula is disputed — docs, /fee-rate endpoint, and the on-chain
# CalculatorHelper.sol currently disagree (see GitHub issue, April 2026).
# Do a single live test fill before trusting the simulator's PnL output.

FeeFn = Callable[[float, float], float]  # fee_fn(p, notional_usd) -> fee_usd


def polymarket_fee_placeholder(p: float, notional_usd: float) -> float:
    """Placeholder consistent with Polymarket's help-center worked example
    (100 shares @ $0.50 → ~$0.38 fee, i.e. ~0.75% of $50 notional, which
    is the peak). Symmetric around p=0.50."""
    return notional_usd * 0.03 * p * (1 - p)


def zero_fee(_p: float, _notional_usd: float) -> float:
    """For maker legs under Polymarket's post-2026 maker-free tier."""
    return 0.0


# ─── Trade primitives ─────────────────────────────────────────────────────

Side         = Literal["buy", "sell"]
OutcomeSide  = Literal["home", "draw", "away"]
ExitKind     = Literal["hold-to-settlement", "fok-taker-fallback",
                       "early-exit-taker", "flip-maker-sell"]


@dataclass(frozen=True)
class EntrySignal:
    """Match state at the moment an entry plan is evaluated."""
    match_id:              str
    side:                  Side
    outcome_side:          OutcomeSide
    polymarket_mid:        float     # (best_bid + best_ask) / 2
    polymarket_best_ask:   float     # for FOK taker price
    polymarket_best_bid:   float     # for FOK taker price on sells
    pinnacle_fair:         float     # de-vigged fair
    notional_usd:          float
    t_minutes_to_kick:     float


@dataclass(frozen=True)
class ExitPlan:
    """How the trade exits. kind drives which branch simulate_round_trip
    takes; extra fields carry branch-specific parameters."""
    kind:                  ExitKind
    # For fok-taker-fallback: assumed exit price at T-10m. In scaffold
    # we default to entering-side best_ask / best_bid. Phase 2 will thread
    # a realistic book-snapshot from the honest shadow feed.
    exit_price_override:   float | None = None
    # For settlement exits, the resolved outcome (0 or 1). None = not
    # resolved yet in sim — treated as unknown; scaffold uses p_win prior.
    settlement:            int | None = None


@dataclass
class TradeResult:
    """Output of one simulated round trip."""
    match_id:            str
    side:                Side
    outcome_side:        OutcomeSide
    entry_price:         float
    exit_price:          float
    notional_usd:        float
    shares:              float
    gross_pnl:           float
    fee_entry:           float
    fee_exit:            float
    net_pnl:             float
    exit_kind:           ExitKind
    diagnostics:         dict = field(default_factory=dict)


# ─── Simulation ───────────────────────────────────────────────────────────

def simulate_round_trip(
    *,
    entry:       EntrySignal,
    exit_plan:   ExitPlan,
    fee_fn:      FeeFn = polymarket_fee_placeholder,
    maker_fee_fn: FeeFn = zero_fee,
) -> TradeResult:
    """Simulate one trade from entry to exit with honest fee accounting.

    Phase 1 semantics (extended in Phase 2):
      - Entry is always a maker limit at (polymarket_mid - 0.005) on the
        buy side, or (polymarket_mid + 0.005) on the sell side.
      - Entry fill is assumed (queue-position modelling is Phase 2). The
        maker_fee_fn path is where a maker rebate would plug in; default
        zero_fee reflects Polymarket's post-Feb-2026 maker-free tier.
      - Exit branches by exit_plan.kind:
          * hold-to-settlement: no exit fee; PnL = (1 - entry) * shares
            on a win, -entry * shares on a loss. If settlement is None,
            we use the Pinnacle fair as p_win and return the expected PnL.
          * fok-taker-fallback: exit at best_bid for a prior buy,
            best_ask for a prior sell. Taker fee via fee_fn on notional.
          * flip-maker-sell / early-exit-taker: scaffolds return a best-
            effort estimate with the same maker/taker split as above.
    """
    entry_price = _compute_entry_price(entry)
    shares = entry.notional_usd / entry_price
    fee_entry = maker_fee_fn(entry_price, entry.notional_usd)

    exit_price, exit_fee, exit_kind = _compute_exit(
        entry=entry, plan=exit_plan, shares=shares,
        fee_fn=fee_fn, maker_fee_fn=maker_fee_fn,
    )

    gross_pnl = _gross_pnl(entry.side, entry_price, exit_price, shares,
                           settlement=exit_plan.settlement,
                           prior_p_win=entry.pinnacle_fair)
    net_pnl = gross_pnl - fee_entry - exit_fee

    return TradeResult(
        match_id=entry.match_id,
        side=entry.side,
        outcome_side=entry.outcome_side,
        entry_price=entry_price,
        exit_price=exit_price,
        notional_usd=entry.notional_usd,
        shares=shares,
        gross_pnl=gross_pnl,
        fee_entry=fee_entry,
        fee_exit=exit_fee,
        net_pnl=net_pnl,
        exit_kind=exit_kind,
        diagnostics={
            "pinnacle_fair":    entry.pinnacle_fair,
            "polymarket_mid":   entry.polymarket_mid,
            "divergence_cents": (entry.pinnacle_fair - entry.polymarket_mid) * 100,
        },
    )


# ─── Private helpers ──────────────────────────────────────────────────────

def _compute_entry_price(entry: EntrySignal) -> float:
    """Maker limit 0.5c inside the mid, on the correct side."""
    offset = 0.005
    if entry.side == "buy":
        return entry.polymarket_mid - offset
    return entry.polymarket_mid + offset


def _compute_exit(*, entry: EntrySignal, plan: ExitPlan, shares: float,
                  fee_fn: FeeFn, maker_fee_fn: FeeFn
                  ) -> tuple[float, float, ExitKind]:
    """Return (exit_price, exit_fee_usd, exit_kind). Exit semantics live
    here so simulate_round_trip stays a readable orchestrator."""
    if plan.kind == "hold-to-settlement":
        # Settlement has no exit transaction (on-chain auto-resolve).
        # When settlement is realized we return the nominal 0/1 so the
        # PnL branch computes actual outcome. When unresolved we return
        # a sentinel equal to entry_price so the EV branch in _gross_pnl
        # takes over using pinnacle_fair as the prior.
        if plan.settlement is None:
            return _compute_entry_price(entry), 0.0, plan.kind
        nominal_exit = 1.0 if plan.settlement == 1 else 0.0
        return nominal_exit, 0.0, plan.kind

    if plan.kind == "fok-taker-fallback":
        exit_price = (plan.exit_price_override
                      if plan.exit_price_override is not None
                      else (entry.polymarket_best_bid if entry.side == "buy"
                            else entry.polymarket_best_ask))
        exit_notional = exit_price * shares
        return exit_price, fee_fn(exit_price, exit_notional), plan.kind

    if plan.kind == "flip-maker-sell":
        # +1.5c above entry, maker fee (zero under post-2026 tier).
        exit_price = _compute_entry_price(entry) + 0.015
        exit_notional = exit_price * shares
        return exit_price, maker_fee_fn(exit_price, exit_notional), plan.kind

    # early-exit-taker: same as fok fallback today; Phase 2 may split them.
    exit_price = (plan.exit_price_override
                  if plan.exit_price_override is not None
                  else (entry.polymarket_best_bid if entry.side == "buy"
                        else entry.polymarket_best_ask))
    exit_notional = exit_price * shares
    return exit_price, fee_fn(exit_price, exit_notional), plan.kind


def _gross_pnl(side: Side, entry_price: float, exit_price: float,
               shares: float, *, settlement: int | None,
               prior_p_win: float) -> float:
    """Gross PnL before fees.

    For settlement exits with no realized outcome, fall back to expected
    PnL using pinnacle_fair as p_win prior. For non-settlement exits,
    compute directly from entry/exit prices.
    """
    if settlement is not None:
        realized = 1.0 if settlement == 1 else 0.0
        delta = realized - entry_price if side == "buy" else entry_price - realized
        return delta * shares

    if entry_price == exit_price:
        # Expected-value branch for hold-to-settlement with unknown outcome.
        if side == "buy":
            return (prior_p_win - entry_price) * shares
        return (entry_price - prior_p_win) * shares

    delta = exit_price - entry_price if side == "buy" else entry_price - exit_price
    return delta * shares
