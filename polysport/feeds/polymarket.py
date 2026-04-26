"""Polymarket Gamma + CLOB client.

Gamma (REST) lists events and markets; CLOB (REST) serves the order book.
A 3-way soccer moneyline appears in Gamma as 3 Yes/No markets per event
(home-wins, draw, away-wins), each with a conditionId and a pair of
clobTokenIds = [yes_token, no_token]. We price the "Yes" side since that's
what a bettor buys to back an outcome.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal

import httpx

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

OutcomeSide = Literal["home", "draw", "away"]

_QUESTION_WIN_PATTERN = re.compile(r"^Will\s+(.+?)\s+win\s+on\s+", flags=re.IGNORECASE)

# Map our internal league slug -> list of Polymarket tag slugs used for that league.
# Polymarket is inconsistent (e.g., "champions-league" + "uefa-champions-league" +
# "ucl"), so we union across all known aliases to avoid missing events.
LEAGUE_TAG_ALIASES: dict[str, list[str]] = {
    "epl": ["EPL", "premier-league"],
    "ucl": ["champions-league", "uefa-champions-league", "ucl"],
    "uel": ["uel", "europa-league", "uefa-europa-league"],
    "seriea": ["serie-a"],
    "laliga": ["la-liga"],
    "bundesliga": ["bundesliga"],
    "ligue1": ["ligue-1"],
    "worldcup": ["fifa-world-cup", "world-cup"],
    "mls": ["mls"],
    # Polymarket abbreviates Eredivisie as 'ere' — verified empirically
    # 2026-04-26 (other obvious slugs like 'eredivisie' / 'dutch-eredivisie'
    # return zero events).
    "eredivisie": ["ere"],
    "primeira": ["primeira-liga"],
}


def list_events_by_tag(
    client: httpx.Client,
    *,
    tag_slug: str,
    limit: int = 500,
    closed: bool = False,
    active: bool = True,
) -> list[dict]:
    """Fetch events matching a specific tag slug from Gamma API."""
    resp = client.get(
        f"{GAMMA_BASE}/events",
        params={
            "tag_slug": tag_slug,
            "closed": "true" if closed else "false",
            "active": "true" if active else "false",
            "limit": limit,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


def list_league_events(client: httpx.Client, league_slug: str) -> list[dict]:
    """Fetch all events for a league, deduped across Polymarket's tag aliases."""
    tags = LEAGUE_TAG_ALIASES.get(league_slug)
    if not tags:
        raise ValueError(f"Unknown league slug: {league_slug}")

    seen_ids: set = set()
    out: list[dict] = []
    for tag in tags:
        for e in list_events_by_tag(client, tag_slug=tag):
            eid = e.get("id")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                out.append(e)
    return out


@dataclass(frozen=True)
class MoneylineMarket:
    """One side of a 3-way moneyline: a Yes/No market priced on Polymarket CLOB."""

    outcome_side: OutcomeSide  # 'home' | 'draw' | 'away'
    condition_id: str
    yes_token_id: str  # token we price (cost to bet Yes on this outcome)
    no_token_id: str
    slug: str
    question: str


def extract_moneyline_markets(event: dict, home_raw: str, away_raw: str) -> list[MoneylineMarket]:
    """Pick the 3 moneyline markets out of an event and tag each with home/draw/away.

    Each market has outcomes=["Yes","No"] and clobTokenIds=[yes, no]. We ignore
    secondary markets (exact score, halftime, etc.) by requiring the question to
    match either "Will <team> win on <date>" or "... end in a draw".

    Returns [] if the event doesn't have all 3 sides recognisable — the caller
    should log the event and skip rather than half-match (never guess silently).
    """
    home_key = home_raw.strip().lower()
    away_key = away_raw.strip().lower()
    collected: dict[OutcomeSide, MoneylineMarket] = {}

    for m in event.get("markets", []) or []:
        q = (m.get("question") or "").strip()
        if not q or not m.get("active") or m.get("closed"):
            continue

        # Token IDs come as either a list or a JSON-encoded string in Gamma payloads.
        tokens = m.get("clobTokenIds")
        if isinstance(tokens, str):
            try:
                tokens = json.loads(tokens)
            except json.JSONDecodeError:
                continue
        if not (isinstance(tokens, list) and len(tokens) == 2):
            continue

        side: OutcomeSide | None = None
        if re.search(r"end in a draw", q, flags=re.IGNORECASE):
            side = "draw"
        else:
            win_match = _QUESTION_WIN_PATTERN.match(q)
            if win_match:
                team_in_q = win_match.group(1).strip().lower()
                if team_in_q == home_key:
                    side = "home"
                elif team_in_q == away_key:
                    side = "away"

        if side is None or side in collected:
            # Unrecognised market type, or duplicate side — skip defensively.
            continue

        collected[side] = MoneylineMarket(
            outcome_side=side,
            condition_id=m.get("conditionId") or "",
            yes_token_id=tokens[0],
            no_token_id=tokens[1],
            slug=m.get("slug") or "",
            question=q,
        )

    # Require all 3 sides; otherwise return [] so caller treats this event as unusable.
    if len(collected) == 3:
        return [collected["home"], collected["draw"], collected["away"]]
    return []


@dataclass(frozen=True)
class BookTop:
    """Best bid/ask plus size at the top of book for a single CLOB token."""

    token_id: str
    best_bid: float | None
    best_ask: float | None
    bid_size_shares: float | None  # shares on Polymarket == USD at $1 notional
    ask_size_shares: float | None
    raw: dict[str, Any]


def fetch_book(client: httpx.Client, token_id: str) -> BookTop:
    """GET /book for one token and return top-of-book. Never raises on empty book."""
    resp = client.get(f"{CLOB_BASE}/book", params={"token_id": token_id}, timeout=15.0)
    resp.raise_for_status()
    payload = resp.json()

    # Polymarket CLOB returns bids sorted ascending by price, asks ascending too.
    # Best bid = highest buyer price, best ask = lowest seller price.
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    best_bid = _price(max(bids, key=lambda b: float(b["price"]))) if bids else None
    best_ask = _price(min(asks, key=lambda a: float(a["price"]))) if asks else None
    bid_size = _size(max(bids, key=lambda b: float(b["price"]))) if bids else None
    ask_size = _size(min(asks, key=lambda a: float(a["price"]))) if asks else None

    return BookTop(
        token_id=token_id,
        best_bid=best_bid,
        best_ask=best_ask,
        bid_size_shares=bid_size,
        ask_size_shares=ask_size,
        raw=payload,
    )


def _price(level: dict) -> float | None:
    try:
        return float(level["price"])
    except (KeyError, TypeError, ValueError):
        return None


def _size(level: dict) -> float | None:
    try:
        return float(level["size"])
    except (KeyError, TypeError, ValueError):
        return None
