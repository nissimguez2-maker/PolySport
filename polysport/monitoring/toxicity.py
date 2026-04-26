"""Maker toxicity tracker — adverse-selection monitoring for maker fills.

Definition (from STRATEGY.md):
    Maker toxicity rate = fraction of maker fills where Pinnacle fair moves
    > 0.5c against the filled position within 60s post-fill.

Kill-switch thresholds (activated only after baseline calibration, see below):
    - > 55%  : abandon maker mode, go taker-only
    - 40-55%: tighten divergence threshold to >= 2.5c
    - < 40%  : healthy

Why the thresholds are not active yet
-------------------------------------
The STRATEGY.md draft assumed a 50% random-noise baseline. The financial
audit (see conversation log) argued that a post-only limit priced 0.5c
inside the best is not facing a 50% baseline — your price improvement
systematically reduces uninformed-taker flow (fewer noise takers bother to
route to a passive limit) while informed-flow adverse-selection is
unaffected. Expected empirical baseline is 52-55%, which collapses the
signal-to-noise window for the 55% kill trigger.

Correct move: calibrate the baseline empirically from the first 50-100
shadow-mode maker fills (unbounded — no thresholds enforced), then set
thresholds at (empirical_baseline + 3pp).

This module provides:
    record_fill(...)     -> write one row per maker fill with the raw
                            60s-post-fill Pinnacle move, no threshold
                            evaluation
    empirical_baseline() -> once n >= MIN_CALIBRATION_FILLS, return the
                            observed adverse-move rate
    toxicity_status()    -> None while in calibration mode; after that,
                            returns "healthy" | "tighten" | "abandon-maker"
                            using the calibrated baseline

Intentionally thin. Phase 2 will flesh out persistence against a
maker_fills table; for now the scaffold keeps everything in-process so the
strategy module can import and wire it without touching Supabase schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# Minimum fills required before the module will return a baseline or a
# threshold-based status. See STRATEGY.md — the audit recommended 50-100;
# we default to the low end so calibration finishes in ~1 week of
# shadow-mode maker activity at typical fill rates.
MIN_CALIBRATION_FILLS = 50

# Delta above the empirical baseline at which toxicity is considered
# elevated. From the audit: "raise the toxicity kill threshold to
# baseline + 3pp, and the tighten-spread threshold to baseline + 2pp".
TIGHTEN_DELTA_PP = 2
KILL_DELTA_PP = 3

# How far Pinnacle fair must move against the filled position within the
# post-fill window for the fill to count as "toxic". 0.5c per STRATEGY.md.
ADVERSE_MOVE_CENTS = 0.5

# Post-fill window in seconds during which the adverse move is measured.
POST_FILL_WINDOW_SEC = 60


@dataclass(frozen=True)
class FillRecord:
    """One maker fill, with just enough for toxicity accounting.

    Adverse-selection verdict is computed on read, not at write time, so
    the definition can evolve without rewriting history.
    """

    filled_at_iso: str  # ISO-8601, fill timestamp
    side: Literal["buy", "sell"]
    price: float  # fill price, 0–1
    pinnacle_fair_at_fill: float  # fair value at moment of fill
    pinnacle_fair_60s_after: float | None  # fair 60s post-fill, or None if feed gap
    match_id: str  # for join with snapshot tables later
    outcome_side: Literal["home", "draw", "away"]


TOXIC_VERDICT = Literal["toxic", "benign", "unknown"]


def verdict_for(record: FillRecord) -> TOXIC_VERDICT:
    """Classify one fill.

    "toxic"   — fair moved >= ADVERSE_MOVE_CENTS against our side within 60s
    "benign"  — fair didn't move that far against us (includes moves in our
                favor — those are good fills, not just non-toxic)
    "unknown" — no post-fill fair reading, can't tell
    """
    if record.pinnacle_fair_60s_after is None:
        return "unknown"
    delta = record.pinnacle_fair_60s_after - record.pinnacle_fair_at_fill
    # For a buy fill, adverse = fair moved DOWN (our long is underwater).
    # For a sell fill, adverse = fair moved UP.
    adverse = -delta if record.side == "buy" else delta
    return "toxic" if adverse * 100 >= ADVERSE_MOVE_CENTS else "benign"


@dataclass
class ToxicityTracker:
    """In-process maker-fill log. Phase 2 will back this with Supabase;
    for scaffold purposes we keep it as a list and expose the two reads
    the strategy + dashboard actually need."""

    fills: list[FillRecord]

    @classmethod
    def empty(cls) -> ToxicityTracker:
        return cls(fills=[])

    def record_fill(self, record: FillRecord) -> None:
        """Append one fill to the log. No threshold evaluation — this is
        the 'unbounded collection mode' the audit asked for during the
        calibration window."""
        self.fills.append(record)

    def _classified(self) -> list[TOXIC_VERDICT]:
        return [verdict_for(f) for f in self.fills if verdict_for(f) != "unknown"]

    def empirical_baseline(self) -> float | None:
        """Fraction of classifiable fills that came in as toxic.

        Returns None until MIN_CALIBRATION_FILLS classifiable records exist.
        Once calibrated, this is the baseline against which the kill /
        tighten thresholds are set at the call site.
        """
        classified = self._classified()
        if len(classified) < MIN_CALIBRATION_FILLS:
            return None
        return sum(1 for v in classified if v == "toxic") / len(classified)

    def toxicity_status(self) -> Literal["calibrating", "healthy", "tighten", "abandon-maker"]:
        """Dashboard/strategy-consumable verdict. Intentionally returns
        'calibrating' (not a threshold-driven status) until the baseline
        is established, so the kill switch cannot fire on noise."""
        baseline = self.empirical_baseline()
        if baseline is None:
            return "calibrating"
        recent = self._classified()[-MIN_CALIBRATION_FILLS:]
        if not recent:
            return "calibrating"
        recent_rate = sum(1 for v in recent if v == "toxic") / len(recent)
        if recent_rate * 100 >= baseline * 100 + KILL_DELTA_PP:
            return "abandon-maker"
        if recent_rate * 100 >= baseline * 100 + TIGHTEN_DELTA_PP:
            return "tighten"
        return "healthy"
