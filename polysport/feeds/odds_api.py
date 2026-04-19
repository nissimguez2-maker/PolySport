"""The Odds API client — Pinnacle-first for the sharp benchmark.

Phase 1 only needs the /odds endpoint per league. No event-level lookups; the
poll loop hits one endpoint per league per cycle and extracts Pinnacle 3-way
moneylines from the response.

Quota discipline: 20,000 req/month on Starter ($30/mo). At 30s poll × 7 leagues
× 60 min/hr × 24 hr × 30 days = 604,800 raw polls, far over quota. We mitigate
by (a) only polling in the T-180min→T+0 and T+45→T+60 windows, and (b) coalescing
leagues the API already bundles. Budget math lives in scripts/phase1_logger.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Our internal league slug -> The Odds API sport_key.
# Matches the target leagues from preflight_matcher.py so join logic stays consistent.
LEAGUE_TO_SPORT_KEY: dict[str, str] = {
    "epl":        "soccer_epl",
    "ucl":        "soccer_uefa_champs_league",
    "uel":        "soccer_uefa_europa_league",
    "seriea":     "soccer_italy_serie_a",
    "laliga":     "soccer_spain_la_liga",
    "bundesliga": "soccer_germany_bundesliga",
    "ligue1":     "soccer_france_ligue_one",
}


@dataclass(frozen=True)
class BookmakerOdds:
    """One bookmaker's 3-way moneyline for one event. All fields from the raw payload."""
    bookmaker: str                 # e.g. 'pinnacle', 'bet365'
    last_update: str               # ISO-8601 from the API (string; parse at boundary)
    odds_home: float | None
    odds_draw: float | None
    odds_away: float | None


@dataclass(frozen=True)
class OddsEvent:
    """One event from /odds. Team names are raw; resolution to team_ids happens downstream."""
    event_id: str
    sport_key: str
    commence_time: str             # ISO-8601
    home_team_raw: str
    away_team_raw: str
    bookmakers: list[BookmakerOdds]
    raw: dict[str, Any]            # full API payload for Supabase logging


@dataclass(frozen=True)
class EventSummary:
    """Schedule-only metadata from the free /events endpoint. No odds."""
    event_id: str
    sport_key: str
    league_slug: str
    commence_time: str
    home_team_raw: str
    away_team_raw: str


def _quota_from(resp: httpx.Response) -> dict[str, str]:
    return {
        "remaining": resp.headers.get("x-requests-remaining", ""),
        "used":      resp.headers.get("x-requests-used", ""),
        "last":      resp.headers.get("x-requests-last", ""),
    }


def fetch_events_for_league(
    client: httpx.Client,
    *,
    api_key: str,
    league_slug: str,
) -> tuple[list[EventSummary], dict[str, str]]:
    """Free schedule scan: /sports/{sport}/events. x-requests-last should be 0.

    Returns (events, quota_headers). Used for cheap discovery so the paid
    /events/{id}/odds call only fires for matches inside the trade window.
    """
    sport_key = LEAGUE_TO_SPORT_KEY.get(league_slug)
    if not sport_key:
        raise ValueError(f"Unknown league slug for Odds API: {league_slug!r}")

    resp = client.get(f"{ODDS_API_BASE}/sports/{sport_key}/events",
                      params={"apiKey": api_key}, timeout=15.0)
    resp.raise_for_status()

    events = [EventSummary(
        event_id      = raw["id"],
        sport_key     = sport_key,
        league_slug   = league_slug,
        commence_time = raw["commence_time"],
        home_team_raw = raw["home_team"],
        away_team_raw = raw["away_team"],
    ) for raw in resp.json()]
    return events, _quota_from(resp)


def fetch_odds_for_event(
    client: httpx.Client,
    *,
    api_key: str,
    sport_key: str,
    event_id: str,
    regions: str = "eu",
    markets: str = "h2h",
    odds_format: str = "decimal",
    bookmakers: str | None = None,
) -> tuple[OddsEvent, dict[str, str]]:
    """Paid single-event odds: /events/{id}/odds. Costs markets × regions.

    At h2h × eu that's 1 credit. Used per in-window match only, so spend
    scales with match count, not wall-clock.
    """
    params: dict[str, str] = {
        "apiKey":     api_key,
        "regions":    regions,
        "markets":    markets,
        "oddsFormat": odds_format,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers

    resp = client.get(f"{ODDS_API_BASE}/sports/{sport_key}/events/{event_id}/odds",
                      params=params, timeout=15.0)
    resp.raise_for_status()
    return _parse_event(resp.json(), sport_key), _quota_from(resp)


def fetch_odds_for_league(
    client: httpx.Client,
    *,
    api_key: str,
    league_slug: str,
    regions: str = "eu",           # Pinnacle + Bet365 are in EU region
    markets: str = "h2h",          # 3-way moneyline
    odds_format: str = "decimal",
    bookmakers: str | None = None, # comma-separated; None = all in region
) -> tuple[list[OddsEvent], dict[str, str]]:
    """Legacy league-wide /odds call. Costs markets × regions × 1 (per league).

    Adaptive logger uses fetch_events_for_league + fetch_odds_for_event instead,
    which only pays for in-window matches. Keep this for smoke tests / backfills.
    """
    sport_key = LEAGUE_TO_SPORT_KEY.get(league_slug)
    if not sport_key:
        raise ValueError(f"Unknown league slug for Odds API: {league_slug!r}. "
                         f"Known: {list(LEAGUE_TO_SPORT_KEY)}")

    params: dict[str, str] = {
        "apiKey":     api_key,
        "regions":    regions,
        "markets":    markets,
        "oddsFormat": odds_format,
    }
    if bookmakers:
        params["bookmakers"] = bookmakers

    resp = client.get(f"{ODDS_API_BASE}/sports/{sport_key}/odds",
                      params=params, timeout=15.0)
    resp.raise_for_status()

    events: list[OddsEvent] = []
    for raw_ev in resp.json():
        events.append(_parse_event(raw_ev, sport_key))
    return events, _quota_from(resp)


def _parse_event(raw: dict[str, Any], sport_key: str) -> OddsEvent:
    books: list[BookmakerOdds] = []
    for bm in raw.get("bookmakers", []):
        h = d = a = None
        for mkt in bm.get("markets", []):
            if mkt.get("key") != "h2h":
                continue
            for outcome in mkt.get("outcomes", []):
                name = outcome.get("name")
                price = outcome.get("price")
                # Odds API uses team names for home/away outcomes and literal "Draw".
                if name == raw.get("home_team"):
                    h = price
                elif name == raw.get("away_team"):
                    a = price
                elif name == "Draw":
                    d = price
        books.append(BookmakerOdds(
            bookmaker   = bm.get("key", ""),
            last_update = bm.get("last_update", ""),
            odds_home   = h,
            odds_draw   = d,
            odds_away   = a,
        ))
    return OddsEvent(
        event_id      = raw["id"],
        sport_key     = sport_key,
        commence_time = raw["commence_time"],
        home_team_raw = raw["home_team"],
        away_team_raw = raw["away_team"],
        bookmakers    = books,
        raw           = raw,
    )


def pinnacle_from_event(event: OddsEvent) -> BookmakerOdds | None:
    """Pick the Pinnacle line from an event, if present and complete."""
    for b in event.bookmakers:
        if b.bookmaker == "pinnacle" and b.odds_home and b.odds_draw and b.odds_away:
            return b
    return None
