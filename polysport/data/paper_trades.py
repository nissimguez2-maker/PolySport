"""Persistence layer for the paper-trade tape.

Writer: scripts/phase1_logger.py — at the end of each cycle, evaluate the
        strategy against fresh data and INSERT one row per fired signal.
        The unique (home_team_id, away_team_id, kickoff_hour) constraint
        (migration 004) enforces single-leg-per-match while absorbing
        Pinnacle's minute-level commence_time jitter. Subsequent
        same-match cycles silently no-op.

Reader: polysport/dashboard/data.py — builds a 7-day summary AND a
        per-position list (live MTM + realized PnL) for the dashboard.

Resolver: scripts/resolve_paper_trades.py — runs as a cron / Railway
        scheduled task. Looks up Polymarket market resolution after
        kickoff, computes realized PnL, writes settled_outcome /
        realized_pnl / settled_at via resolve_signal().
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

# A position whose latest Polymarket snapshot is older than this is treated
# as stale: we report current_mid=None / live_pnl_usd=None rather than
# pricing against a dead feed. Matches the dashboard's RECENT_WINDOW.
LIVE_MTM_STALENESS_MAX = timedelta(minutes=10)


@dataclass(frozen=True)
class PaperTradeSummary:
    """7-day snapshot consumed by the dashboard. None values mean
    no data; zero values mean explicitly zero."""

    n_total: int
    n_pending: int  # kickoff still in the future
    n_settled: int  # kickoff in the past (whether or not realized PnL is set)
    n_realized: int  # rows with realized_pnl populated
    cumulative_ev_usd: float
    cumulative_realized_pnl_usd: float | None
    best_edge_cents: float
    last_decided_at: datetime | None


@dataclass(frozen=True)
class Position:
    """One paper trade as the dashboard renders it.

    `status` is derived: 'open' (kickoff in the future), 'pending_settle'
    (past kickoff, no realized PnL yet), 'settled' (realized PnL written).
    `live_pnl_usd` is the mark-to-market on the latest Polymarket mid;
    None when the PM snapshot is stale or absent.
    """

    paper_trade_id: str
    decided_at: datetime
    kickoff: datetime
    home: str
    away: str
    league: str
    target_outcome: str  # 'home' | 'draw' | 'away'
    side: str  # 'buy' | 'sell'
    entry_price: float
    notional_usd: float
    expected_edge_cents: float
    current_mid: float | None
    pm_snapshot_age_sec: float | None
    live_pnl_usd: float | None
    status: str  # 'open' | 'pending_settle' | 'settled'
    realized_pnl_usd: float | None
    settled_at: datetime | None


def record_signal(
    sb,
    *,
    home_team_id: str,
    away_team_id: str,
    kickoff: datetime,
    minutes_to_kick: float,
    target_outcome: str,
    side: str,
    limit_price: float,
    expected_edge: float,
    fair: float,
    mid: float,
    pinnacle_staleness_sec: float,
    notional_usd: float,
    sim_entry_price: float,
    sim_net_pnl_ev: float,
    polymarket_condition_id: str | None = None,
    polymarket_yes_token_id: str | None = None,
) -> bool:
    """Insert one paper-trade row.

    Returns True if a new row landed; False if the unique constraint
    hit (i.e. this match already has a recorded entry — single-leg rule).
    Any other DB error propagates so the caller can surface it.

    polymarket_condition_id / polymarket_yes_token_id (added 2026-04-26 via
    migration 005) feed the resolver. Optional for backwards-compat with
    pre-005 callers; resolver skips rows missing them.
    """
    payload = {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "kickoff": kickoff.isoformat(),
        "minutes_to_kick": minutes_to_kick,
        "target_outcome": target_outcome,
        "side": side,
        "limit_price": limit_price,
        "expected_edge": expected_edge,
        "fair": fair,
        "mid": mid,
        "pinnacle_staleness_sec": pinnacle_staleness_sec,
        "notional_usd": notional_usd,
        "sim_entry_price": sim_entry_price,
        "sim_net_pnl_ev": sim_net_pnl_ev,
    }
    if polymarket_condition_id is not None:
        payload["polymarket_condition_id"] = polymarket_condition_id
    if polymarket_yes_token_id is not None:
        payload["polymarket_yes_token_id"] = polymarket_yes_token_id
    try:
        sb.table("paper_trades").insert(payload).execute()
        return True
    except Exception as exc:
        msg = str(exc).lower()
        # Postgres error code 23505 = unique_violation; postgrest surfaces
        # the human-readable message. Match generously to survive minor
        # wording drift across postgrest versions.
        if "duplicate" in msg or "unique" in msg or "23505" in msg:
            return False
        raise


def resolve_signal(
    sb,
    *,
    paper_trade_id: str,
    settled_outcome: int,
    realized_pnl: float,
) -> bool:
    """Settle one paper trade. Idempotent: WHERE settled_at IS NULL guard
    means a duplicate resolver run on the same row is a no-op.

    Returns True if the row was updated, False if it was already settled.
    """
    res = (
        sb.table("paper_trades")
        .update(
            {
                "settled_outcome": settled_outcome,
                "realized_pnl": realized_pnl,
                "settled_at": datetime.now(UTC).isoformat(),
            }
        )
        .eq("id", paper_trade_id)
        .is_("settled_at", "null")
        .execute()
    )
    return bool(res.data)


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def summary(sb, *, days_back: int = 7) -> PaperTradeSummary:
    """Aggregate the last N days of paper trades for the dashboard.

    Pulls everything in one shot; at sub-1-fire-per-day rates this is
    well under any pagination concern. If we ever exceed 1k rows in 7
    days, switch to the keyset paginator from polysport.data.snapshots.
    """
    cutoff = (datetime.now(UTC) - timedelta(days=days_back)).isoformat()
    rows = (
        sb.table("paper_trades")
        .select("decided_at, kickoff, expected_edge, sim_net_pnl_ev, realized_pnl, settled_at")
        .gte("decided_at", cutoff)
        .order("decided_at", desc=True)
        .execute()
        .data
    )
    if not rows:
        return PaperTradeSummary(
            n_total=0,
            n_pending=0,
            n_settled=0,
            n_realized=0,
            cumulative_ev_usd=0.0,
            cumulative_realized_pnl_usd=None,
            best_edge_cents=0.0,
            last_decided_at=None,
        )

    now = datetime.now(UTC)
    n_pending = 0
    n_settled = 0
    n_realized = 0
    cum_ev = 0.0
    realized_total = 0.0
    best_edge = 0.0

    for r in rows:
        kt = _parse_ts(r.get("kickoff"))
        if kt is not None and kt > now:
            n_pending += 1
        else:
            n_settled += 1

        ev = r.get("sim_net_pnl_ev")
        if ev is not None:
            cum_ev += float(ev)

        rp = r.get("realized_pnl")
        if rp is not None:
            n_realized += 1
            realized_total += float(rp)

        ec = r.get("expected_edge")
        if ec is not None:
            ec_cents = float(ec) * 100
            if ec_cents > best_edge:
                best_edge = ec_cents

    last_decided = _parse_ts(rows[0].get("decided_at"))

    return PaperTradeSummary(
        n_total=len(rows),
        n_pending=n_pending,
        n_settled=n_settled,
        n_realized=n_realized,
        cumulative_ev_usd=cum_ev,
        cumulative_realized_pnl_usd=realized_total if n_realized else None,
        best_edge_cents=best_edge,
        last_decided_at=last_decided,
    )


def list_positions(sb, *, days_back: int = 30) -> list[Position]:
    """Per-row positions for the dashboard.

    Two queries: paper_trades within window, then the matching latest
    Polymarket snapshots for live MTM. Joined in Python — at sub-1k rows
    this is faster than wrangling postgrest into a server-side join.

    MTM math (buy side): shares = notional / entry; current value =
    shares * mid; live_pnl = current_value - notional. Sells invert.
    """
    now = datetime.now(UTC)
    cutoff_iso = (now - timedelta(days=days_back)).isoformat()
    pm_window_start = (now - LIVE_MTM_STALENESS_MAX).isoformat()

    trade_rows = (
        sb.table("paper_trades")
        .select(
            "id, decided_at, kickoff, home_team_id, away_team_id, "
            "target_outcome, side, sim_entry_price, notional_usd, "
            "expected_edge, realized_pnl, settled_at"
        )
        .gte("decided_at", cutoff_iso)
        .order("decided_at", desc=True)
        .execute()
        .data
    )
    if not trade_rows:
        return []

    team_ids: set[str] = set()
    for r in trade_rows:
        team_ids.add(r["home_team_id"])
        team_ids.add(r["away_team_id"])
    team_rows = (
        sb.table("teams")
        .select("id, canonical_name, league")
        .in_("id", list(team_ids))
        .execute()
        .data
    )
    team_name = {t["id"]: t["canonical_name"] for t in team_rows}
    team_league = {t["id"]: t.get("league", "") for t in team_rows}

    # Latest PM snapshot per (home, away, outcome_side) — only fetch
    # snapshots fresh enough to use as live MTM. Returned ordered desc by
    # polled_at so first-seen-wins captures the newest per key.
    pm_rows = (
        sb.table("polymarket_snapshots")
        .select("home_team_id, away_team_id, outcome_side, best_bid, best_ask, polled_at")
        .in_("home_team_id", [r["home_team_id"] for r in trade_rows])
        .gt("polled_at", pm_window_start)
        .not_.is_("outcome_side", "null")
        .order("polled_at", desc=True)
        .execute()
        .data
    )
    pm_latest: dict[tuple, dict] = {}
    for r in pm_rows:
        key = (r["home_team_id"], r["away_team_id"], r["outcome_side"])
        if key not in pm_latest:
            pm_latest[key] = r

    positions: list[Position] = []
    for r in trade_rows:
        kt = _parse_ts(r["kickoff"])
        if kt is None:
            continue
        decided = _parse_ts(r["decided_at"]) or now

        entry = float(r["sim_entry_price"])
        notional = float(r["notional_usd"])
        side = r.get("side", "buy")
        target = r["target_outcome"]

        pm = pm_latest.get((r["home_team_id"], r["away_team_id"], target))
        current_mid: float | None = None
        pm_age_sec: float | None = None
        if pm and pm.get("best_bid") is not None and pm.get("best_ask") is not None:
            bid_f = float(pm["best_bid"])
            ask_f = float(pm["best_ask"])
            current_mid = (bid_f + ask_f) / 2.0
            pt = _parse_ts(pm["polled_at"])
            if pt is not None:
                pm_age_sec = (now - pt).total_seconds()

        live_pnl: float | None = None
        if current_mid is not None and entry > 0:
            shares = notional / entry
            value_now = shares * current_mid
            live_pnl = value_now - notional
            if side == "sell":
                live_pnl = -live_pnl

        realized = r.get("realized_pnl")
        realized_f: float | None = float(realized) if realized is not None else None
        settled_at = _parse_ts(r.get("settled_at"))

        if settled_at is not None:
            status = "settled"
        elif kt > now:
            status = "open"
        else:
            status = "pending_settle"

        edge = r.get("expected_edge")
        edge_cents = float(edge) * 100 if edge is not None else 0.0

        league = team_league.get(r["home_team_id"]) or team_league.get(r["away_team_id"]) or ""

        positions.append(
            Position(
                paper_trade_id=r["id"],
                decided_at=decided,
                kickoff=kt,
                home=team_name.get(r["home_team_id"], r["home_team_id"][:8]),
                away=team_name.get(r["away_team_id"], r["away_team_id"][:8]),
                league=league,
                target_outcome=target,
                side=side,
                entry_price=entry,
                notional_usd=notional,
                expected_edge_cents=edge_cents,
                current_mid=current_mid,
                pm_snapshot_age_sec=pm_age_sec,
                live_pnl_usd=live_pnl,
                status=status,
                realized_pnl_usd=realized_f,
                settled_at=settled_at,
            )
        )

    # Sort: open first (soonest kickoff first), then pending_settle then
    # settled (most recently decided first). Single numeric secondary key
    # keeps the tuple sort safely uniform-typed.
    status_rank = {"open": 0, "pending_settle": 1, "settled": 2}

    def _sort_key(p: Position) -> tuple[int, float]:
        secondary = p.kickoff.timestamp() if p.status == "open" else -p.decided_at.timestamp()
        return status_rank.get(p.status, 3), secondary

    positions.sort(key=_sort_key)
    return positions
