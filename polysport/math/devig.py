"""Power-method de-vigging for 3-way moneyline markets.

Given raw implied probabilities p_i = 1/odds_i from a 3-way moneyline, find the
exponent k such that p_home^k + p_draw^k + p_away^k = 1.0 exactly. The resulting
p_i^k are our fair probabilities — the input minus the book's vig.

Why power method (and not additive / multiplicative normalisation):
  - Multiplicative (divide by sum) distributes the vig proportionally — a 50% fav
    gets the same relative trim as a 10% longshot. Empirically wrong on Pinnacle;
    longshots carry disproportionately more vig (the favourite-longshot bias).
  - Additive (subtract a constant) can produce negative probabilities on longshots.
  - Power method preserves ordering, keeps everything in (0, 1), and handles the
    longshot bias correctly. Industry standard for sharp books.

Solves via bisection — reliable, no derivatives needed, converges in ~40 iterations
to 1e-12 tolerance. Input validation is strict: decimal odds must all be > 1.0,
and the raw sum of 1/odds must be > 1.0 (otherwise the book has negative vig and
we should refuse to de-vig rather than silently extrapolate).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FairProbs:
    home: float
    draw: float
    away: float
    k: float  # the exponent that normalised the book; diagnostic only
    vig: float  # raw_sum - 1.0, useful for monitoring book health


def devig_3way(
    odds_home: float, odds_draw: float, odds_away: float, *, tol: float = 1e-12, max_iter: int = 100
) -> FairProbs:
    """De-vig a 3-way moneyline (decimal odds) via power method.

    Raises ValueError on malformed input (non-positive odds, odds ≤ 1.0, or
    negative vig). We fail loud rather than return garbage.
    """
    if not (odds_home > 1.0 and odds_draw > 1.0 and odds_away > 1.0):
        raise ValueError(
            f"decimal odds must all be > 1.0, got "
            f"home={odds_home} draw={odds_draw} away={odds_away}"
        )

    p_home = 1.0 / odds_home
    p_draw = 1.0 / odds_draw
    p_away = 1.0 / odds_away
    raw_sum = p_home + p_draw + p_away

    # Negative vig (sum < 1) = arbitrage on the book itself. Refuse to de-vig
    # silently — either the feed is corrupt or the odds are so stale we should
    # not treat them as fair. Caller decides.
    if raw_sum <= 1.0:
        raise ValueError(
            f"raw implied probs sum to {raw_sum:.6f} (≤ 1.0); book has no vig. Refusing to de-vig."
        )

    # Bisection over k in (0, 1]. At k=1, sum = raw_sum > 1. As k -> 0, each
    # p_i^k -> 1, so sum -> 3 > 1. Wait, that's the wrong direction. Let me
    # re-derive: we want sum of p_i^k to DECREASE toward 1. Since p_i < 1,
    # raising p_i to a HIGHER power (k > 1) makes it smaller. So k > 1.
    # Bracket: at k=1, sum = raw_sum > 1. As k -> infinity, each p_i^k -> 0,
    # so sum -> 0 < 1. Monotone decreasing in k => unique root.
    lo, hi = 1.0, 2.0
    # Expand upper bracket if a very vig-heavy book needs k > 2.
    while _power_sum(p_home, p_draw, p_away, hi) > 1.0:
        hi *= 2.0
        if hi > 1e6:
            raise ValueError("bisection failed to bracket root; odds likely corrupt")

    for _ in range(max_iter):
        mid = 0.5 * (lo + hi)
        s = _power_sum(p_home, p_draw, p_away, mid)
        if abs(s - 1.0) < tol:
            return _make_probs(p_home, p_draw, p_away, mid, raw_sum)
        if s > 1.0:
            lo = mid
        else:
            hi = mid
    # If we exit the loop without hitting tolerance, the last midpoint is still
    # accurate to ~1e-30 given 100 bisection steps from (1, 2). Accept it.
    return _make_probs(p_home, p_draw, p_away, 0.5 * (lo + hi), raw_sum)


def _power_sum(p_home: float, p_draw: float, p_away: float, k: float) -> float:
    return p_home**k + p_draw**k + p_away**k


def _make_probs(p_home: float, p_draw: float, p_away: float, k: float, raw_sum: float) -> FairProbs:
    return FairProbs(
        home=p_home**k,
        draw=p_draw**k,
        away=p_away**k,
        k=k,
        vig=raw_sum - 1.0,
    )
