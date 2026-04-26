"""Paper-trade settlement against Polymarket market resolution.

The CLI wrapper at scripts/resolve_paper_trades.py and the logger's
in-process hourly hook both call resolve_batch() — keeping one
implementation avoids drift between standalone-cron and embedded modes.

Settlement honesty: only finalise a paper trade when both `closed: true`
AND `umaResolutionStatus == "resolved"`. A `closed` market with UMA
mid-flight can show prices like 0.0005 / 0.9995 that look extreme but
are not yet authoritative.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx

from polysport.data.paper_trades import resolve_signal

GAMMA_BASE = "https://gamma-api.polymarket.com"

# Wait this long after kickoff before attempting to settle. Most matches
# finish + settle within 2h; the extra hour absorbs UMA dispute-window lag.
SETTLEMENT_DELAY = timedelta(hours=3)

# Gamma's condition_ids param accepts comma-separated values. Keep batches
# small enough to dodge URL length limits and to keep error blast radius
# tight (one bad batch ≠ all rows skipped).
BATCH_SIZE = 25


@dataclass(frozen=True)
class ResolveCounts:
    settled: int
    skipped_unresolved: int
    skipped_missing: int
    errors: int


def fetch_markets(http: httpx.Client, condition_ids: list[str]) -> dict[str, dict]:
    """Map conditionId -> market dict. Missing IDs simply absent from
    the result; caller treats absence as 'not yet resolved, retry later'."""
    if not condition_ids:
        return {}
    resp = http.get(
        f"{GAMMA_BASE}/markets",
        params={"condition_ids": ",".join(condition_ids), "limit": len(condition_ids)},
        timeout=30.0,
    )
    resp.raise_for_status()
    out: dict[str, dict] = {}
    for m in resp.json():
        cid = m.get("conditionId")
        if cid:
            out[cid] = m
    return out


def is_resolved(market: dict) -> bool:
    """True when the market is finalised and we can trust outcomePrices.

    Both signals required: `closed` alone can be true while UMA is still
    processing (we've seen prices like 0.0005 in that window). Waiting
    for `umaResolutionStatus == "resolved"` keeps us honest.
    """
    return bool(market.get("closed")) and market.get("umaResolutionStatus") == "resolved"


def resolved_yes_price(market: dict) -> float | None:
    """Pull the resolved Yes-token price from a finalised market.

    outcomePrices is sometimes a real list, sometimes a JSON-encoded
    string. Position 0 is Yes, position 1 is No. Returns None on
    unexpected shape so the caller can skip rather than mis-settle.
    """
    raw = market.get("outcomePrices")
    if raw is None:
        return None
    try:
        prices = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(prices, list) or len(prices) < 1:
            return None
        return float(prices[0])
    except (ValueError, TypeError):
        return None


def compute_realized_pnl(
    *, side: str, entry_price: float, notional_usd: float, resolved_yes_price: float
) -> float:
    """PnL on a buy: shares × (resolved − entry). Sells invert.

    Encoded so the sign is unambiguous: a winning $5 buy at 0.45
    resolved at 1.0 returns +$6.11; a losing buy at 0.45 resolved at 0.0
    returns −$5.00.
    """
    if entry_price <= 0:
        return 0.0
    shares = notional_usd / entry_price
    pnl = shares * (resolved_yes_price - entry_price)
    if side == "sell":
        pnl = -pnl
    return pnl


def load_unsettled_rows(sb, *, max_rows: int, paper_trade_id: str | None = None) -> list[dict]:
    cutoff_iso = (datetime.now(UTC) - SETTLEMENT_DELAY).isoformat()
    q = (
        sb.table("paper_trades")
        .select(
            "id, home_team_id, away_team_id, kickoff, target_outcome, side, "
            "sim_entry_price, notional_usd, polymarket_condition_id, "
            "polymarket_yes_token_id"
        )
        .is_("settled_at", "null")
        .not_.is_("polymarket_condition_id", "null")
    )
    q = q.eq("id", paper_trade_id) if paper_trade_id else q.lt("kickoff", cutoff_iso)
    q = q.order("kickoff", desc=False).limit(max_rows)
    return q.execute().data or []


def resolve_batch(
    sb,
    http: httpx.Client,
    *,
    max_rows: int = 50,
    dry_run: bool = False,
    paper_trade_id: str | None = None,
    log: bool = True,
) -> ResolveCounts:
    """Settle eligible paper trades. Per-row exceptions are isolated so
    one bad market doesn't tank the rest of the batch."""
    rows = load_unsettled_rows(sb, max_rows=max_rows, paper_trade_id=paper_trade_id)
    if not rows:
        return ResolveCounts(0, 0, 0, 0)

    n_settled = n_skipped_unresolved = n_skipped_missing = n_errors = 0

    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch = rows[batch_start : batch_start + BATCH_SIZE]
        cids = [r["polymarket_condition_id"] for r in batch]
        try:
            markets = fetch_markets(http, cids)
        except httpx.HTTPError as exc:
            if log:
                print(f"  [batch {batch_start}] gamma fetch failed: {exc}", flush=True)
            n_errors += len(batch)
            continue

        for r in batch:
            cid = r["polymarket_condition_id"]
            market = markets.get(cid)
            if market is None:
                n_skipped_missing += 1
                continue
            if not is_resolved(market):
                n_skipped_unresolved += 1
                continue
            yes_price = resolved_yes_price(market)
            if yes_price is None:
                n_skipped_unresolved += 1
                continue
            pnl = compute_realized_pnl(
                side=r.get("side", "buy"),
                entry_price=float(r["sim_entry_price"]),
                notional_usd=float(r["notional_usd"]),
                resolved_yes_price=yes_price,
            )
            settled_outcome = 1 if yes_price >= 0.5 else 0
            if log:
                tag = "DRY" if dry_run else "settled"
                print(
                    f"  [{tag}] {r['id'][:8]} {r['target_outcome']:>4} "
                    f"entry={float(r['sim_entry_price']):.4f} "
                    f"resolved_yes={yes_price:.4f} pnl=${pnl:+.2f}",
                    flush=True,
                )
            if dry_run:
                continue
            try:
                if resolve_signal(
                    sb,
                    paper_trade_id=r["id"],
                    settled_outcome=settled_outcome,
                    realized_pnl=pnl,
                ):
                    n_settled += 1
            except Exception as exc:
                n_errors += 1
                if log:
                    print(f"  [{r['id'][:8]}] resolve_signal failed: {exc}", flush=True)

    return ResolveCounts(n_settled, n_skipped_unresolved, n_skipped_missing, n_errors)
