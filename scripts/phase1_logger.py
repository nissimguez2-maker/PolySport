"""Phase 1: 48h standalone divergence logger.

Per STRATEGY.md Phase 1 goal: log Pinnacle + Polymarket snapshots for every
upcoming EPL/UCL match for ~48h. No trading, no decisions — the whole point is
to measure whether ≥30% of monitored matches touch |div| ≥ 2¢ in the
T-120→T+0min window. If they do, the strategy is viable.

Design:
  - Poll each target league every POLL_INTERVAL seconds.
  - For each poll cycle, for each league:
      1. Fetch Odds API /odds → write one row per bookmaker per event to
         odds_api_snapshots.
      2. Fetch Polymarket Gamma events → for each event that parses as "X vs Y",
         extract the 3 moneyline markets, fetch CLOB /book per Yes-token, and
         write one row per outcome to polymarket_snapshots.
  - Team IDs are resolved at write time via TeamMatcher. Unresolved names hit
    unresolved_entities (schema unique constraint collapses duplicates).
  - Never crash the loop: catch per-league errors, log, continue.
  - Never burn quota uselessly: skip events kicking off > TIME_HORIZON_HOURS ahead.

Quota math (for Phase 1 default leagues = EPL + UCL):
  2 leagues × (60s / POLL_INTERVAL) calls/min × 60 min × 48 h = 11,520 Odds API
  calls at 30s cadence. Well inside the 20,000/mo Starter quota.
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.feeds.matcher import TeamMatcher  # noqa: E402
from polysport.feeds.odds_api import (  # noqa: E402
    LEAGUE_TO_SPORT_KEY,
    fetch_odds_for_league,
)
from polysport.feeds.polymarket import (  # noqa: E402
    extract_moneyline_markets,
    fetch_book,
    list_league_events,
)

DEFAULT_LEAGUES = ["epl", "ucl", "ligue1"]
POLL_INTERVAL_SEC = 30
TIME_HORIZON_HOURS = 72          # only log events kicking off inside this window
QUOTA_WARN_THRESHOLD = 500       # warn once Odds API remaining drops below this

# Strip trailing secondary-market tokens from Polymarket titles so the home/away
# parse only sees moneyline events.
TRAILING_NOISE = re.compile(
    r"\s*[-—]\s*(More Markets|Match Winner|Moneyline|Pre-?match|3-?Way"
    r"|Halftime\s+Result|Exact\s+Score|Total\s+Corners|Player\s+Props"
    r"|Total\s+Goals|Both\s+Teams\s+to\s+Score|BTTS|Over\/Under)"
    r".*$",
    flags=re.IGNORECASE,
)
LEAGUE_PREFIX = re.compile(r"^[^:]+:\s+")
TITLE_PATTERN = re.compile(
    r"^(?P<home>.+?)\s+(?:vs\.?|v)\s+(?P<away>.+?)$",
    flags=re.IGNORECASE,
)

_stop = False


def _inactive_sleep_seconds(tz: ZoneInfo, start_hour: int, end_hour: int) -> float:
    """0 if currently inside active window; else seconds to the next start_hour.

    Window semantics: [start_hour, end_hour) in local time, wrapping midnight if
    start > end. start == end means always active.
    """
    if start_hour == end_hour:
        return 0.0
    now = datetime.now(tz)
    h = now.hour
    active = (start_hour <= h < end_hour) if start_hour < end_hour else (
        h >= start_hour or h < end_hour)
    if active:
        return 0.0
    # Sleep until next occurrence of start_hour:00 local.
    target = now.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


def _install_sigterm_handler() -> None:
    def handler(signum, frame):
        global _stop
        _stop = True
        print(f"\n[signal {signum}] draining current cycle then exiting…", flush=True)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def _clean_title(title: str) -> str:
    return LEAGUE_PREFIX.sub("", TRAILING_NOISE.sub("", title.strip())).strip()


def _parse_iso8601(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _within_horizon(commence: datetime | None, horizon_hours: int) -> bool:
    if commence is None:
        return False
    now = datetime.now(timezone.utc)
    return (commence - now) <= timedelta(hours=horizon_hours) and commence >= (now - timedelta(hours=3))


def poll_odds_api(sb, http: httpx.Client, matcher: TeamMatcher,
                  api_key: str, league: str) -> tuple[int, dict[str, str]]:
    """Fetch one league's odds, write snapshots, return (rows_written, quota)."""
    events, quota = fetch_odds_for_league(http, api_key=api_key, league_slug=league)
    rows = 0
    for ev in events:
        commence = _parse_iso8601(ev.commence_time)
        if not _within_horizon(commence, TIME_HORIZON_HOURS):
            continue
        home_id = _resolve(matcher, ev.home_team_raw, source="odds_api",
                           league=league, extra={"event_id": ev.event_id})
        away_id = _resolve(matcher, ev.away_team_raw, source="odds_api",
                           league=league, extra={"event_id": ev.event_id})

        for bm in ev.bookmakers:
            sb.table("odds_api_snapshots").insert({
                "event_id":      ev.event_id,
                "league_key":    ev.sport_key,
                "home_team_raw": ev.home_team_raw,
                "away_team_raw": ev.away_team_raw,
                "home_team_id":  home_id,
                "away_team_id":  away_id,
                "commence_time": ev.commence_time,
                "bookmaker":     bm.bookmaker,
                "odds_home":     bm.odds_home,
                "odds_draw":     bm.odds_draw,
                "odds_away":     bm.odds_away,
                "raw":           ev.raw,
            }).execute()
            rows += 1
    return rows, quota


def poll_polymarket(sb, http: httpx.Client, matcher: TeamMatcher,
                    league: str) -> int:
    """Fetch one league's Polymarket events + books, write snapshots."""
    events = list_league_events(http, league)
    rows = 0
    for e in events:
        raw_title = e.get("title") or ""
        if " vs" not in raw_title.lower():
            continue
        title = _clean_title(raw_title)
        m = TITLE_PATTERN.match(title)
        if not m:
            continue
        home_raw = m.group("home").strip()
        away_raw = m.group("away").strip()

        commence = _parse_iso8601(e.get("startDate") or "")
        # Polymarket's startDate is event creation, not kickoff. We can't
        # horizon-filter reliably on Polymarket — only on Odds API where we
        # trust commence_time. Log everything we can parse.

        markets = extract_moneyline_markets(e, home_raw, away_raw)
        if not markets:
            continue

        home_id = _resolve(matcher, home_raw, source="polymarket",
                           league=league, extra={"event_id": e.get("id"), "title": raw_title})
        away_id = _resolve(matcher, away_raw, source="polymarket",
                           league=league, extra={"event_id": e.get("id"), "title": raw_title})

        for mkt in markets:
            try:
                book = fetch_book(http, mkt.yes_token_id)
            except httpx.HTTPError as exc:
                print(f"  [pm book {mkt.outcome_side}] {exc}", flush=True)
                continue
            bid_depth = (book.best_bid * book.bid_size_shares
                         if book.best_bid and book.bid_size_shares else None)
            ask_depth = (book.best_ask * book.ask_size_shares
                         if book.best_ask and book.ask_size_shares else None)

            sb.table("polymarket_snapshots").insert({
                "event_id":           str(e.get("id")),
                "market_id":          mkt.condition_id,
                "outcome_raw":        mkt.question,
                "outcome_side":       mkt.outcome_side,
                "home_team_id":       home_id,
                "away_team_id":       away_id,
                "commence_time":      commence.isoformat() if commence else None,
                "best_bid":           book.best_bid,
                "best_ask":           book.best_ask,
                "best_bid_depth_usd": bid_depth,
                "best_ask_depth_usd": ask_depth,
                "raw":                {
                    "event_id":        e.get("id"),
                    "event_slug":      e.get("slug"),
                    "market_slug":     mkt.slug,
                    "yes_token_id":    mkt.yes_token_id,
                    "book":            book.raw,
                },
            }).execute()
            rows += 1
    return rows


def _resolve(matcher: TeamMatcher, raw: str, *, source: str, league: str,
             extra: dict) -> str | None:
    r = matcher.resolve(raw, source=source, league_hint=league,
                        context={"league": league, **extra})
    return r.team_id if r else None


def one_cycle(sb, http: httpx.Client, matcher: TeamMatcher,
              api_key: str, leagues: list[str]) -> None:
    started = time.monotonic()
    for lg in leagues:
        if lg not in LEAGUE_TO_SPORT_KEY:
            print(f"  [odds_api {lg}] SKIPPED — no Odds API sport_key mapped", flush=True)
        else:
            try:
                n, quota = poll_odds_api(sb, http, matcher, api_key, lg)
                rem = quota.get("remaining", "?")
                warn = "  ⚠ LOW QUOTA" if (rem.isdigit() and int(rem) < QUOTA_WARN_THRESHOLD) else ""
                print(f"  [odds_api {lg:<10}] wrote {n:3d} rows  quota_remaining={rem}{warn}", flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  [odds_api {lg}] ERROR: {exc!r}", flush=True)
                traceback.print_exc()
        try:
            n = poll_polymarket(sb, http, matcher, lg)
            print(f"  [polymarket {lg:<8}] wrote {n:3d} rows", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [polymarket {lg}] ERROR: {exc!r}", flush=True)
            traceback.print_exc()
    dur = time.monotonic() - started
    print(f"cycle done in {dur:5.1f}s", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", nargs="+", default=DEFAULT_LEAGUES,
                        help=f"League slugs to poll. Default: {DEFAULT_LEAGUES}. "
                             f"Valid: {list(LEAGUE_TO_SPORT_KEY)}")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL_SEC,
                        help=f"Seconds between polls. Default: {POLL_INTERVAL_SEC}")
    parser.add_argument("--once", action="store_true",
                        help="Run a single cycle and exit (for smoke tests).")
    parser.add_argument("--duration-hours", type=float, default=48.0,
                        help="Total run time in hours. Script exits 0 when exceeded. "
                             "Default 48 matches STRATEGY.md Phase 1 sanity-check scope. "
                             "Set to 0 for unlimited (ops only — will burn quota).")
    parser.add_argument("--active-hours-start", type=int, default=12,
                        help="Hour (0–23) in --timezone at which polling resumes. "
                             "Default 12 (noon) — covers EU match windows.")
    parser.add_argument("--active-hours-end", type=int, default=24,
                        help="Hour (0–24) in --timezone at which polling pauses. "
                             "Default 24 (midnight). If start==end, always active.")
    parser.add_argument("--timezone", default="Asia/Jerusalem",
                        help="IANA timezone for active-hours gate. Default Asia/Jerusalem.")
    args = parser.parse_args()
    tz = ZoneInfo(args.timezone)

    for lg in args.leagues:
        if lg not in LEAGUE_TO_SPORT_KEY:
            print(f"WARN: league {lg!r} has no Odds API sport_key — Polymarket-only.",
                  flush=True)

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    api_key = os.environ["ODDS_API_KEY"]

    matcher = TeamMatcher(sb)
    _install_sigterm_handler()

    print(f"Phase 1 logger starting. leagues={args.leagues} interval={args.interval}s "
          f"horizon={TIME_HORIZON_HOURS}h duration={args.duration_hours}h "
          f"active={args.active_hours_start:02d}:00–{args.active_hours_end:02d}:00 "
          f"{args.timezone} once={args.once}", flush=True)
    run_deadline = (time.monotonic() + args.duration_hours * 3600.0
                    if args.duration_hours > 0 else None)

    # One connection pool across all cycles — avoids TCP/TLS handshake per cycle.
    # Connection limits scale with the number of concurrent book fetches we do.
    with httpx.Client(
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=20),
        timeout=httpx.Timeout(15.0, connect=5.0),
    ) as http:
        if args.once:
            one_cycle(sb, http, matcher, api_key, args.leagues)
            return 0

        while not _stop:
            if run_deadline is not None and time.monotonic() >= run_deadline:
                print(f"\nDuration budget of {args.duration_hours}h reached — exiting cleanly.",
                      flush=True)
                return 0

            sleep_to_next_active = _inactive_sleep_seconds(
                tz, args.active_hours_start, args.active_hours_end)
            if sleep_to_next_active > 0:
                wake_local = datetime.now(tz).replace(
                    hour=args.active_hours_start, minute=0, second=0, microsecond=0)
                if wake_local <= datetime.now(tz):
                    wake_local = wake_local + timedelta(days=1)
                print(f"\n[idle] outside active window "
                      f"{args.active_hours_start:02d}:00–{args.active_hours_end:02d}:00 "
                      f"{args.timezone}. Sleeping {sleep_to_next_active/60:.0f} min "
                      f"until {wake_local.strftime('%Y-%m-%d %H:%M %Z')}.",
                      flush=True)
                # Sleep in 60-second slices so SIGTERM preempts fast.
                deadline = time.monotonic() + sleep_to_next_active
                while not _stop and time.monotonic() < deadline:
                    if run_deadline is not None and time.monotonic() >= run_deadline:
                        return 0
                    time.sleep(min(60.0, deadline - time.monotonic()))
                continue

            cycle_start = time.monotonic()
            print(f"\n--- cycle @ {datetime.now(timezone.utc).isoformat()} ---",
                  flush=True)
            one_cycle(sb, http, matcher, api_key, args.leagues)
            elapsed = time.monotonic() - cycle_start
            if elapsed > args.interval:
                print(f"WARN: cycle took {elapsed:.1f}s > interval {args.interval}s "
                      f"— running hot, consider raising --interval", flush=True)
            sleep_for = max(0.0, args.interval - elapsed)
            if _stop:
                break
            # Sleep in small slices so SIGTERM preempts promptly.
            deadline = time.monotonic() + sleep_for
            while not _stop and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))
    print("stopped cleanly.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
