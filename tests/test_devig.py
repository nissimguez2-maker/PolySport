"""Tests for the 3-way power-method de-vigger."""

from __future__ import annotations

import math

import pytest

from polysport.math.devig import FairProbs, devig_3way


def _assert_sums_to_one(f: FairProbs, tol: float = 1e-9) -> None:
    s = f.home + f.draw + f.away
    assert abs(s - 1.0) < tol, f"fair probs sum to {s}, expected 1.0"


def test_fair_book_raises() -> None:
    """A book with no vig (sum = 1) should be refused, not silently de-vigged."""
    # 3.0/3.0/3.0 -> raw sum = 1.0 exactly. Should refuse.
    with pytest.raises(ValueError, match="no vig"):
        devig_3way(3.0, 3.0, 3.0)


def test_rejects_sub_unity_odds() -> None:
    with pytest.raises(ValueError, match="must all be > 1.0"):
        devig_3way(0.9, 3.0, 3.0)


def test_typical_epl_match() -> None:
    """Man City -1.5 vs Arsenal-ish 3-way. Raw sum ~1.05 (5% vig)."""
    f = devig_3way(1.80, 3.90, 4.20)
    _assert_sums_to_one(f)
    # Favourite > draw > underdog ordering preserved.
    assert f.home > f.draw
    assert f.home > f.away
    # k > 1 because we're shrinking probs to compensate for positive vig.
    assert f.k > 1.0
    # Vig is positive and sensible (~5%).
    assert 0.04 < f.vig < 0.06


def test_tight_line() -> None:
    """All three outcomes near 33% with ~4% vig. Vig distributes ~evenly."""
    f = devig_3way(2.90, 2.95, 3.00)
    _assert_sums_to_one(f)
    # All fair probs within ~1.5pp of raw 1/odds because the line is balanced.
    raw = [1 / 2.90, 1 / 2.95, 1 / 3.00]
    for r, fair in zip(raw, (f.home, f.draw, f.away)):
        assert abs(fair - r) < 0.02


def test_heavy_favourite_longshot_bias() -> None:
    """Power method should trim the longshot more (proportionally) than the fav."""
    # 1.20 favourite, 8.0 draw, 15.0 longshot. Raw sum ~1.067.
    f = devig_3way(1.20, 8.0, 15.0)
    _assert_sums_to_one(f)
    p_home_raw = 1 / 1.20
    p_away_raw = 1 / 15.0
    # Relative trim on fav is smaller than relative trim on longshot.
    trim_home = (p_home_raw - f.home) / p_home_raw
    trim_away = (p_away_raw - f.away) / p_away_raw
    assert trim_away > trim_home, \
        f"expected longshot trimmed more than fav (got {trim_away:.4f} vs {trim_home:.4f})"


def test_high_vig_book_still_converges() -> None:
    """A 15%-vig exotic book should still de-vig cleanly without blowing up."""
    f = devig_3way(1.50, 3.50, 4.00)
    _assert_sums_to_one(f)
    assert f.vig > 0.10
    assert f.k > 1.0
    assert math.isfinite(f.k)


def test_ordering_invariance() -> None:
    """De-vigged probs have the same ordering as raw implied probs."""
    f = devig_3way(2.50, 3.40, 2.90)
    raw = [(1 / 2.50, f.home), (1 / 3.40, f.draw), (1 / 2.90, f.away)]
    raw_sorted = sorted(raw, key=lambda x: x[0])
    fair_sorted = sorted(raw, key=lambda x: x[1])
    assert raw_sorted == fair_sorted, "ordering between raw and fair probs diverged"


def test_pinnacle_style_realistic_sample() -> None:
    """Representative Pinnacle line: Liverpool vs Everton derby."""
    # Pinnacle's pre-match on big EPL games runs 2–4% vig. Home favoured.
    f = devig_3way(1.45, 4.75, 7.50)
    _assert_sums_to_one(f)
    assert 0.015 < f.vig < 0.040
    assert f.home > 0.65
    assert f.away < 0.15


def test_returns_fairprobs_dataclass() -> None:
    f = devig_3way(2.00, 3.50, 3.80)
    assert isinstance(f, FairProbs)
    # Frozen dataclass — mutation should fail.
    with pytest.raises((AttributeError, TypeError)):
        f.home = 0.5  # type: ignore[misc]
