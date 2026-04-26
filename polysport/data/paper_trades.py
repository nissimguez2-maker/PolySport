"""Persistence layer for the paper-trade tape.

Writer: scripts/phase1_logger.py — at the end of each cycle, evaluate the
        strategy against fresh data and INSERT one row per fired signal.
        The unique (home_team_id, away_team_id, kickoff_hour) constraint
        (migration 004) enforces single-leg-per-match while absorbing
        Pinnacle's minute-level commence_time jitter. Subsequent
        same-match cycles silently no-op.

Reader: polysport/dashboard/data.py — builds a 7-day summary for the
        dashboard's "Paper trades" panel.

Resolver (future): a Phase 2 job will set settled_outcome / realized_pnl /
        settled_at after kickoff so we can compute realized PnL in
        addition to the EV under Pinnacle prior we record at decision
        time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


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
) -> bool:
    """Insert one paper-trade row.

    Returns True if a new row landed; False if the unique constraint
    hit (i.e. this match already has a recorded entry — single-leg rule).
    Any other DB error propagates so the caller can surface it.
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
