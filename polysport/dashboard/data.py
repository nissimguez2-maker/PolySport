"""Supabase -> dashboard view model.

One call: `get_live_state(sb)` returns everything the index page needs.
Window: last 10 minutes of polls, capped at 72h of upcoming matches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from polysport.math.devig import devig_3way

ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")
RECENT_WINDOW = timedelta(minutes=10)
UPCOMING_HORIZON = timedelta(hours=72)
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
    best_side: str | None = None       # outcome with max positive div
    best_div_cents: float = 0.0        # signed; positive = underpriced on PM
    max_abs_div_cents: float = 0.0
    action_kind: str = "skip"          # "buy" | "sell" | "skip"
    action_side: str | None = None     # "home" | "draw" | "away"
    action_reason: str = ""            # "no edge" | "spread" | "depth" | "heavy fav" | "fire"
    pin_age_sec: float | None = None


def _parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _fmt_t(minutes_to_kick: float) -> str:
    """Sports convention: T-30m = kickoff in 30 min; T+15m = match 15 min in."""
    if minutes_to_kick >= 0:
        return f"T-{minutes_to_kick:.0f}m"
    return f"T+{-minutes_to_kick:.0f}m"


def _decide_action(m: MatchRow) -> tuple[str, str | None, str]:
    """Return (kind, side, reason) given match state + STRATEGY.md gates."""
    positive_sides = [(s, o) for s, o in m.outcomes.items()
                      if o.div is not None and o.div > 0]
    if not positive_sides:
        return "skip", None, "no underpriced outcome"

    side, outcome = max(positive_sides, key=lambda kv: kv[1].div)
    if outcome.div < ENTRY_DIV_THRESHOLD:
        return "skip", side, f"gap {outcome.div*100:.2f}¢ < 2¢"
    if outcome.spread is None or outcome.spread > MAX_SPREAD:
        return "skip", side, f"spread {(outcome.spread or 0)*100:.1f}¢ > 3¢"
    if outcome.depth_min_usd is None or outcome.depth_min_usd < MIN_DEPTH_USD:
        depth = outcome.depth_min_usd or 0
        return "skip", side, f"depth ${depth:.0f} < $500"
    if outcome.fair >= MAX_FAV_PROB:
        return "skip", side, f"fav prob {outcome.fair:.2f} ≥ 0.80"
    return "buy", side, "all gates pass"


def get_live_state(sb) -> dict:
    now = datetime.now(timezone.utc)
    window_start = now - RECENT_WINDOW
    horizon_future = now + UPCOMING_HORIZON
    horizon_past = now - PAST_HORIZON

    teams = sb.table("teams").select("id, canonical_name").execute().data
    team_name = {t["id"]: t["canonical_name"] for t in teams}

    pin_rows = (sb.table("odds_api_snapshots")
                .select("home_team_id, away_team_id, commence_time, odds_home, "
                        "odds_draw, odds_away, polled_at, league_key")
                .eq("bookmaker", "pinnacle")
                .gte("polled_at", window_start.isoformat())
                .gte("commence_time", horizon_past.isoformat())
                .lte("commence_time", horizon_future.isoformat())
                .not_.is_("home_team_id", "null")
                .not_.is_("away_team_id", "null")
                .not_.is_("odds_home", "null")
                .order("polled_at", desc=True)
                .execute().data)

    pm_rows = (sb.table("polymarket_snapshots")
               .select("home_team_id, away_team_id, outcome_side, best_bid, "
                       "best_ask, best_bid_depth_usd, best_ask_depth_usd, "
                       "polled_at")
               .gte("polled_at", window_start.isoformat())
               .not_.is_("home_team_id", "null")
               .not_.is_("outcome_side", "null")
               .order("polled_at", desc=True)
               .execute().data)

    unresolved_count = (sb.table("unresolved_entities")
                        .select("id", count="exact")
                        .is_("resolved_at", "null")
                        .execute()).count or 0

    pin_by_match: dict[tuple, dict] = {}
    for r in pin_rows:
        kt = _parse_ts(r["commence_time"])
        if not kt:
            continue
        key = (r["home_team_id"], r["away_team_id"],
               kt.replace(second=0, microsecond=0))
        if key not in pin_by_match:
            pin_by_match[key] = r

    pm_by_outcome: dict[tuple, dict] = {}
    for r in pm_rows:
        key = (r["home_team_id"], r["away_team_id"], r["outcome_side"])
        if key not in pm_by_outcome:
            pm_by_outcome[key] = r

    match_rows: list[MatchRow] = []
    for (home_id, away_id, _), pin in pin_by_match.items():
        kt = _parse_ts(pin["commence_time"])
        polled_pin = _parse_ts(pin["polled_at"])

        try:
            fair = devig_3way(float(pin["odds_home"]),
                              float(pin["odds_draw"]),
                              float(pin["odds_away"]))
        except (ValueError, TypeError):
            continue

        fair_by_side = {"home": fair.home, "draw": fair.draw, "away": fair.away}
        outcomes: dict[str, Outcome] = {}
        for side in ("home", "draw", "away"):
            pm = pm_by_outcome.get((home_id, away_id, side))
            f = fair_by_side[side]
            if not pm:
                outcomes[side] = Outcome(fair=f, mid=None, bid=None, ask=None,
                                         spread=None, depth_min_usd=None,
                                         div=None, pm_age_sec=None)
                continue
            bid, ask = pm.get("best_bid"), pm.get("best_ask")
            if bid is None or ask is None:
                outcomes[side] = Outcome(fair=f, mid=None, bid=None, ask=None,
                                         spread=None, depth_min_usd=None,
                                         div=None, pm_age_sec=None)
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
                fair=f, mid=mid, bid=bid_f, ask=ask_f, spread=spread,
                depth_min_usd=depth_min, div=(f - mid),
                pm_age_sec=(now - pt).total_seconds() if pt else None,
            )

        signed_divs = [(s, o.div) for s, o in outcomes.items() if o.div is not None]
        max_abs_div = max((abs(d) for _, d in signed_divs), default=0.0)
        best_side, best_div = (None, 0.0)
        if signed_divs:
            best_side, best_div = max(signed_divs, key=lambda kv: kv[1])

        minutes_to_kick = (kt - now).total_seconds() / 60.0
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

    match_rows.sort(key=lambda m: (
        0 if m.in_window else (1 if m.minutes_to_kick > 120 else 2),
        m.minutes_to_kick if m.minutes_to_kick >= 0 else -m.minutes_to_kick + 1e6,
    ))

    last_poll = None
    if pin_rows:
        last_poll = _parse_ts(pin_rows[0]["polled_at"])
    if pm_rows:
        p = _parse_ts(pm_rows[0]["polled_at"])
        if p and (last_poll is None or p > last_poll):
            last_poll = p
    last_poll_age_sec = (now - last_poll).total_seconds() if last_poll else None

    n_would_fire = sum(1 for m in match_rows if m.action_kind == "buy")
    n_in_window = sum(1 for m in match_rows if m.in_window)
    best_gap = max((m.best_div_cents for m in match_rows), default=0.0)

    return {
        "now_local": now.astimezone(ISRAEL_TZ).strftime("%H:%M:%S"),
        "last_poll_age_sec": last_poll_age_sec,
        "logger_healthy": last_poll_age_sec is not None and last_poll_age_sec < 120,
        "n_matches": len(match_rows),
        "n_in_window": n_in_window,
        "n_would_fire": n_would_fire,
        "best_gap_cents": best_gap,
        "unresolved_count": unresolved_count,
        "matches": match_rows,
        "fmt_t": _fmt_t,
        "entry_div_cents": ENTRY_DIV_THRESHOLD * 100,
    }
