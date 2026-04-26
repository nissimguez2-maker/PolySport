"""Supabase -> dashboard view model.

One call: `get_live_state(sb)` returns everything the index page needs.
Window: last 10 minutes of polls, capped at 72h of upcoming matches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from polysport.math.devig import devig_3way

ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")
RECENT_WINDOW = timedelta(minutes=10)
UPCOMING_HORIZON = timedelta(days=7)
PAST_HORIZON = timedelta(hours=4)

# STRATEGY.md entry gates, cents as floats.
ENTRY_DIV_THRESHOLD = 0.02
MAX_SPREAD = 0.03
MIN_DEPTH_USD = 500.0
MAX_FAV_PROB = 0.80


@dataclass
class Outcome:
    fair: float
    mid: float | None
    bid: float | None
    ask: float | None
    spread: float | None
    depth_min_usd: float | None
    div: float | None
    pm_age_sec: float | None


@dataclass
class MatchRow:
    kickoff_local: str
    minutes_to_kick: float
    in_window: bool
    home: str
    away: str
    league: str
    outcomes: dict[str, Outcome] = field(default_factory=dict)
    best_side: str | None = None  # outcome with max positive div
    best_div_cents: float = 0.0  # signed; positive = underpriced on PM
    max_abs_div_cents: float = 0.0
    action_kind: str = "skip"  # "buy" | "sell" | "skip"
    action_side: str | None = None  # "home" | "draw" | "away"
    action_reason: str = ""  # "no edge" | "spread" | "depth" | "heavy fav" | "fire"
    pin_age_sec: float | None = None


def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _fmt_t(minutes_to_kick: float) -> str:
    """Countdown to kickoff as H:MM (e.g. 1:53, 0:45, 0:00).

    Post-kickoff rows are filtered upstream in get_live_state, so this path
    is effectively always non-negative. The negative branch stays as a
    defensive fallback.
    """
    if minutes_to_kick < 0:
        return f"+{-int(round(minutes_to_kick))}m"
    h, m = divmod(int(round(minutes_to_kick)), 60)
    return f"{h}:{m:02d}"


def _decide_action(m: MatchRow) -> tuple[str, str | None, str]:
    """Return (kind, side, reason) given match state + STRATEGY.md gates."""
    positive_sides = [(s, o) for s, o in m.outcomes.items() if o.div is not None and o.div > 0]
    if not positive_sides:
        return "skip", None, "no underpriced outcome"

    side, outcome = max(positive_sides, key=lambda kv: kv[1].div)
    if outcome.div < ENTRY_DIV_THRESHOLD:
        return "skip", side, f"gap {outcome.div * 100:.2f}¢ < 2¢"
    if outcome.spread is None or outcome.spread > MAX_SPREAD:
        return "skip", side, f"spread {(outcome.spread or 0) * 100:.1f}¢ > 3¢"
    if outcome.depth_min_usd is None or outcome.depth_min_usd < MIN_DEPTH_USD:
        depth = outcome.depth_min_usd or 0
        return "skip", side, f"depth ${depth:.0f} < $500"
    if outcome.fair >= MAX_FAV_PROB:
        return "skip", side, f"fav prob {outcome.fair:.2f} ≥ 0.80"
    return "buy", side, "all gates pass"


def get_live_state(sb) -> dict:
    now = datetime.now(UTC)
    window_start = now - RECENT_WINDOW
    horizon_future = now + UPCOMING_HORIZON
    horizon_past = now - PAST_HORIZON

    teams = sb.table("teams").select("id, canonical_name").execute().data
    team_name = {t["id"]: t["canonical_name"] for t in teams}

    pin_rows = (
        sb.table("odds_api_snapshots")
        .select(
            "home_team_id, away_team_id, commence_time, odds_home, "
            "odds_draw, odds_away, polled_at, league_key"
        )
        .eq("bookmaker", "pinnacle")
        .gte("polled_at", window_start.isoformat())
        .gte("commence_time", horizon_past.isoformat())
        .lte("commence_time", horizon_future.isoformat())
        .not_.is_("home_team_id", "null")
        .not_.is_("away_team_id", "null")
        .not_.is_("odds_home", "null")
        .order("polled_at", desc=True)
        .execute()
        .data
    )

    pm_rows = (
        sb.table("polymarket_snapshots")
        .select(
            "home_team_id, away_team_id, outcome_side, best_bid, "
            "best_ask, best_bid_depth_usd, best_ask_depth_usd, "
            "polled_at"
        )
        .gte("polled_at", window_start.isoformat())
        .not_.is_("home_team_id", "null")
        .not_.is_("outcome_side", "null")
        .order("polled_at", desc=True)
        .execute()
        .data
    )

    unresolved_count = (
        sb.table("unresolved_entities")
        .select("id", count="exact")
        .is_("resolved_at", "null")
        .execute()
    ).count or 0

    # Best-effort quota read. The migration may not have been applied yet on
    # a fresh deploy; the dashboard must still render. Same defensive pattern
    # as the logger's _persist_quota.
    quota: dict | None = None
    try:
        qrows = (
            sb.table("odds_api_quota")
            .select("remaining, used, last_cost, updated_at")
            .eq("id", 1)
            .limit(1)
            .execute()
        ).data
        if qrows:
            quota = qrows[0]
    except Exception:
        quota = None

    # Pinnacle's commence_time jitters minute-to-minute for a match that's
    # about to start, so keying dedup on (home, away, commence_time) treats
    # each jittered value as a separate match. The query is bounded to a
    # 7d-future / 4h-past window, so a same-team pairing collision is
    # effectively impossible; keying on (home, away) alone is safe.
    # pin_rows is ordered polled_at desc → first entry wins = latest poll.
    pin_by_match: dict[tuple, dict] = {}
    for r in pin_rows:
        if not _parse_ts(r["commence_time"]):
            continue
        key = (r["home_team_id"], r["away_team_id"])
        if key not in pin_by_match:
            pin_by_match[key] = r

    pm_by_outcome: dict[tuple, dict] = {}
    for r in pm_rows:
        key = (r["home_team_id"], r["away_team_id"], r["outcome_side"])
        if key not in pm_by_outcome:
            pm_by_outcome[key] = r

    match_rows: list[MatchRow] = []
    for (home_id, away_id), pin in pin_by_match.items():
        kt = _parse_ts(pin["commence_time"])
        polled_pin = _parse_ts(pin["polled_at"])

        # Phase 1 only trades the T−120 → T−0 window. After kickoff Pinnacle
        # stops updating pre-match odds, so pin_age_sec grows unboundedly and
        # the row is not actionable. Hide them from the dashboard entirely.
        minutes_to_kick = (kt - now).total_seconds() / 60.0
        if minutes_to_kick < 0:
            continue

        try:
            fair = devig_3way(
                float(pin["odds_home"]), float(pin["odds_draw"]), float(pin["odds_away"])
            )
        except (ValueError, TypeError):
            continue

        fair_by_side = {"home": fair.home, "draw": fair.draw, "away": fair.away}
        outcomes: dict[str, Outcome] = {}
        for side in ("home", "draw", "away"):
            pm = pm_by_outcome.get((home_id, away_id, side))
            f = fair_by_side[side]
            if not pm:
                outcomes[side] = Outcome(
                    fair=f,
                    mid=None,
                    bid=None,
                    ask=None,
                    spread=None,
                    depth_min_usd=None,
                    div=None,
                    pm_age_sec=None,
                )
                continue
            bid, ask = pm.get("best_bid"), pm.get("best_ask")
            if bid is None or ask is None:
                outcomes[side] = Outcome(
                    fair=f,
                    mid=None,
                    bid=None,
                    ask=None,
                    spread=None,
                    depth_min_usd=None,
                    div=None,
                    pm_age_sec=None,
                )
                continue
            bid_f, ask_f = float(bid), float(ask)
            mid = (bid_f + ask_f) / 2.0
            spread = ask_f - bid_f
            d_bid = pm.get("best_bid_depth_usd")
            d_ask = pm.get("best_ask_depth_usd")
            depth_min = None
            if d_bid is not None and d_ask is not None:
                depth_min = min(float(d_bid), float(d_ask))
            pt = _parse_ts(pm["polled_at"])
            outcomes[side] = Outcome(
                fair=f,
                mid=mid,
                bid=bid_f,
                ask=ask_f,
                spread=spread,
                depth_min_usd=depth_min,
                div=(f - mid),
                pm_age_sec=(now - pt).total_seconds() if pt else None,
            )

        signed_divs = [(s, o.div) for s, o in outcomes.items() if o.div is not None]
        max_abs_div = max((abs(d) for _, d in signed_divs), default=0.0)
        best_side, best_div = (None, 0.0)
        if signed_divs:
            best_side, best_div = max(signed_divs, key=lambda kv: kv[1])

        row = MatchRow(
            kickoff_local=kt.astimezone(ISRAEL_TZ).strftime("%a %d %b %H:%M"),
            minutes_to_kick=minutes_to_kick,
            in_window=0 <= minutes_to_kick <= 120,
            home=team_name.get(home_id, home_id[:8]),
            away=team_name.get(away_id, away_id[:8]),
            league=pin.get("league_key", ""),
            outcomes=outcomes,
            best_side=best_side,
            best_div_cents=best_div * 100,
            max_abs_div_cents=max_abs_div * 100,
            pin_age_sec=(now - polled_pin).total_seconds() if polled_pin else None,
        )
        kind, side, reason = _decide_action(row)
        row.action_kind, row.action_side, row.action_reason = kind, side, reason
        match_rows.append(row)

    match_rows.sort(
        key=lambda m: (
            0 if m.in_window else (1 if m.minutes_to_kick > 120 else 2),
            m.minutes_to_kick if m.minutes_to_kick >= 0 else -m.minutes_to_kick + 1e6,
        )
    )

    # Per-feed freshness — the original `last_poll_age` was the max of both
    # feeds, which means a Pinnacle-only stream with a dead Polymarket leg
    # would still report logger_healthy=True. Surface each feed's last poll
    # separately so the dashboard can flag a one-sided outage.
    last_pin = _parse_ts(pin_rows[0]["polled_at"]) if pin_rows else None
    last_pm = _parse_ts(pm_rows[0]["polled_at"]) if pm_rows else None
    pin_age_sec = (now - last_pin).total_seconds() if last_pin else None
    pm_age_sec = (now - last_pm).total_seconds() if last_pm else None
    last_poll = max((p for p in (last_pin, last_pm) if p is not None), default=None)
    last_poll_age_sec = (now - last_poll).total_seconds() if last_poll else None

    n_would_fire = sum(1 for m in match_rows if m.action_kind == "buy")
    n_in_window = sum(1 for m in match_rows if m.in_window)
    best_gap = max((m.best_div_cents for m in match_rows), default=0.0)

    # Matcher join health: the matcher is the single join between Pinnacle
    # (odds_api_snapshots) and Polymarket (polymarket_snapshots). If it drops
    # pairs silently, every downstream number is wrong.
    #
    #   n_expected = distinct Pinnacle matches in the window
    #   n_matched  = of those, how many have any Polymarket outcome joined
    #   n_incomplete = 1 or 2 sides present (should be 0 or 3; anything in
    #                  between means Polymarket reshaped a question or the
    #                  join dropped an outcome)
    n_expected = len(pin_by_match)
    n_matched = 0
    n_incomplete = 0
    for home_id, away_id in pin_by_match:
        sides_present = sum(
            1 for side in ("home", "draw", "away") if (home_id, away_id, side) in pm_by_outcome
        )
        if sides_present >= 1:
            n_matched += 1
        if 0 < sides_present < 3:
            n_incomplete += 1

    # The Odds API quota resets monthly, so "% of monthly budget used" is a
    # cleaner signal than a days-remaining countdown (which can mislead when
    # the reset happens before the countdown ends). Total = remaining + used,
    # so the percentage auto-adjusts if the plan changes.
    quota_total: int | None = None
    quota_pct_used: float | None = None
    if quota and quota.get("remaining") is not None and quota.get("used") is not None:
        quota_total = quota["remaining"] + quota["used"]
        if quota_total > 0:
            quota_pct_used = 100.0 * quota["used"] / quota_total

    # Both feeds must be fresh for logger to be considered healthy. A stale
    # Polymarket leg with a fresh Pinnacle leg means every divergence reading
    # is computed against an old PM mid — silently invalid.
    feeds_healthy = (
        pin_age_sec is not None
        and pin_age_sec < 120
        and pm_age_sec is not None
        and pm_age_sec < 120
    )

    return {
        "now_local": now.astimezone(ISRAEL_TZ).strftime("%H:%M:%S"),
        "last_poll_age_sec": last_poll_age_sec,
        "pin_age_sec": pin_age_sec,
        "pm_age_sec": pm_age_sec,
        "logger_healthy": feeds_healthy,
        "n_matches": len(match_rows),
        "n_in_window": n_in_window,
        "n_would_fire": n_would_fire,
        "best_gap_cents": best_gap,
        "unresolved_count": unresolved_count,
        "n_matched": n_matched,
        "n_expected": n_expected,
        "n_incomplete_markets": n_incomplete,
        "quota_remaining": quota["remaining"] if quota else None,
        "quota_used": quota["used"] if quota else None,
        "quota_total": quota_total,
        "quota_pct_used": quota_pct_used,
        "pinnacle_stale_sec": 60,
        "matches": match_rows,
        "fmt_t": _fmt_t,
        "entry_div_cents": ENTRY_DIV_THRESHOLD * 100,
    }
