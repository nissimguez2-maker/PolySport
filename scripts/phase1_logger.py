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
    EventSummary,
    LEAGUE_TO_SPORT_KEY,
    fetch_events_for_league,
    fetch_odds_for_event,
)
from polysport.feeds.polymarket import (  # noqa: E402
    extract_moneyline_markets,
    fetch_book,
    list_league_events,
)

DEFAULT_LEAGUES = ["epl", "ucl", "ligue1", "seriea", "laliga", "bundesliga"]
POLL_INTERVAL_SEC = 60
TRADE_WINDOW_MIN = 120           # poll /events/{id}/odds only for matches kicking off within this many min
SCHEDULE_SCAN_EVERY_SEC = 600    # free /events refresh cadence (10 min)
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


def _inactive_sleep_seconds(tz: ZoneInfo, start_hour: float, end_hour: float) -> float:
    """0 if currently inside active window; else seconds to the next start.

    Hours are floats so half-hour precision works (e.g. 22.5 = 22:30). Window
    semantics: [start, end) in local time, wrapping midnight if start > end.
    start == end means always active.
    """
    if start_hour == end_hour:
        return 0.0
    now = datetime.now(tz)
    h = now.hour + now.minute / 60.0 + now.second / 3600.0
    active = (start_hour <= h < end_hour) if start_hour < end_hour else (
        h >= start_hour or h < end_hour)
    if active:
        return 0.0
    start_h = int(start_hour)
    start_m = int(round((start_hour - start_h) * 60))
    target = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
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


def _minutes_to_kick(commence_iso: str) -> float | None:
    ct = _parse_iso8601(commence_iso)
    if ct is None:
        return None
    return (ct - datetime.now(timezone.utc)).total_seconds() / 60.0


def _in_trade_window(commence_iso: str, window_min: int = TRADE_WINDOW_MIN) -> bool:
    """True iff kickoff is within [0, window_min] minutes ahead."""
    m = _minutes_to_kick(commence_iso)
    return m is not None and 0.0 <= m <= window_min


def poll_schedule(http: httpx.Client, api_key: str,
                  leagues: list[str]) -> dict[str, list[EventSummary]]:
    """Free schedule scan per league. Returns {league_slug: [EventSummary]}."""
    out: dict[str, list[EventSummary]] = {}
    for lg in leagues:
        if lg not in LEAGUE_TO_SPORT_KEY:
            continue
        try:
            evs, quota = fetch_events_for_league(http, api_key=api_key, league_slug=lg)
            last = quota.get("last", "?")
            print(f"  [schedule   {lg:<10}] {len(evs):3d} fixtures  cost={last}", flush=True)
            out[lg] = evs
        except Exception as exc:  # noqa: BLE001
            print(f"  [schedule {lg}] ERROR: {exc!r}", flush=True)
            out[lg] = []
    return out


def poll_event_odds(sb, http: httpx.Client, matcher: TeamMatcher,
                    api_key: str, ev: EventSummary) -> tuple[int, dict[str, str]]:
    """Paid per-event odds poll for one in-window match."""
    odds_ev, quota = fetch_odds_for_event(http, api_key=api_key,
                                          sport_key=ev.sport_key, event_id=ev.event_id)
    home_id = _resolve(matcher, odds_ev.home_team_raw, source="odds_api",
                       league=ev.league_slug, extra={"event_id": ev.event_id})
    away_id = _resolve(matcher, odds_ev.away_team_raw, source="odds_api",
                       league=ev.league_slug, extra={"event_id": ev.event_id})
    rows = 0
    for bm in odds_ev.bookmakers:
        sb.table("odds_api_snapshots").insert({
            "event_id":      odds_ev.event_id,
            "league_key":    odds_ev.sport_key,
            "home_team_raw": odds_ev.home_team_raw,
            "away_team_raw": odds_ev.away_team_raw,
            "home_team_id":  home_id,
            "away_team_id":  away_id,
            "commence_time": odds_ev.commence_time,
            "bookmaker":     bm.bookmaker,
            "odds_home":     bm.odds_home,
            "odds_draw":     bm.odds_draw,
            "odds_away":     bm.odds_away,
            "raw":           odds_ev.raw,
        }).execute()
        rows += 1
    return rows, quota


def poll_polymarket(sb, http: httpx.Client, matcher: TeamMatcher,
                    league: str,
                    *, target_pairs: set[tuple[str, str]] | None = None
                    ) -> tuple[int, int, int]:
    """Fetch one league's Polymarket events + books, write snapshots.

    target_pairs: optional set of (home_team_id, away_team_id). When provided,
    only write rows for PM events whose resolved team pair appears here. Lets
    us scope /book spend to matches that have an Odds API schedule entry —
    everything else (season markets, matches we don't track) is skipped.

    Returns (rows_written, events_parsed, events_matched).
    """
    events = list_league_events(http, league)
    rows = 0
    parsed = 0
    matched = 0
    for e in events:
        raw_title = e.get("title") or ""
        if " vs" not in raw_title.lower():
            continue
        title = _clean_title(raw_title)
        m = TITLE_PATTERN.match(title)
        if not m:
            continue
        parsed += 1
        home_raw = m.group("home").strip()
        away_raw = m.group("away").strip()

        # Resolve first so we can filter against the Odds API schedule before
        # spending /book calls on events we don't track.
        home_id = _resolve(matcher, home_raw, source="polymarket",
                           league=league, extra={"event_id": e.get("id"), "title": raw_title})
        away_id = _resolve(matcher, away_raw, source="polymarket",
                           league=league, extra={"event_id": e.get("id"), "title": raw_title})
        if target_pairs is not None:
            if home_id is None or away_id is None:
                continue
            if (home_id, away_id) not in target_pairs:
                continue
        matched += 1

        markets = extract_moneyline_markets(e, home_raw, away_raw)
        if not markets:
            continue

        # Polymarket's startDate is event creation, not kickoff — do not trust
        # it for analysis. Write what we have; analyze_divergence.py anchors on
        # Odds API commence_time.
        commence = _parse_iso8601(e.get("startDate") or "")

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
    return rows, parsed, matched


def _resolve(matcher: TeamMatcher, raw: str, *, source: str, league: str,
             extra: dict) -> str | None:
    r = matcher.resolve(raw, source=source, league_hint=league,
                        context={"league": league, **extra})
    return r.team_id if r else None


def one_cycle(sb, http: httpx.Client, matcher: TeamMatcher,
              api_key: str, leagues: list[str],
              schedule: dict[str, list[EventSummary]]) -> None:
    """Per-cycle work:
      1. Pinnacle (paid): /events/{id}/odds for every match in the T-120m
         window.
      2. Polymarket (free): one /events scan per league every cycle, scoped
         to team pairs in the Odds API schedule. Runs regardless of in-window
         status so we capture pre-window mid-curve drift — analysis joins by
         team_id anyway.
    """
    started = time.monotonic()
    in_window: dict[str, list[EventSummary]] = {
        lg: [ev for ev in schedule.get(lg, []) if _in_trade_window(ev.commence_time)]
        for lg in leagues
    }
    total_in_window = sum(len(v) for v in in_window.values())
    if total_in_window == 0:
        print(f"  [idle] no matches in T-{TRADE_WINDOW_MIN}m window across "
              f"{len(leagues)} leagues — skipping paid Odds API calls.", flush=True)

    last_quota_remaining = "?"
    for lg, evs in in_window.items():
        for ev in evs:
            mins = _minutes_to_kick(ev.commence_time) or 0.0
            try:
                n, quota = poll_event_odds(sb, http, matcher, api_key, ev)
                last_quota_remaining = quota.get("remaining", "?")
                print(f"  [odds_api {lg:<10}] {ev.home_team_raw} vs "
                      f"{ev.away_team_raw}  T-{mins:4.0f}m  "
                      f"wrote {n} rows  quota_remaining={last_quota_remaining}",
                      flush=True)
            except Exception as exc:  # noqa: BLE001
                print(f"  [odds_api {lg}] ERROR on {ev.event_id}: {exc!r}", flush=True)
                traceback.print_exc()

    # Polymarket poll: every league every cycle, scoped to schedule team pairs.
    for lg in leagues:
        sched_evs = schedule.get(lg, [])
        target_pairs: set[tuple[str, str]] = set()
        for ev in sched_evs:
            h = _resolve(matcher, ev.home_team_raw, source="odds_api",
                         league=lg, extra={"event_id": ev.event_id})
            a = _resolve(matcher, ev.away_team_raw, source="odds_api",
                         league=lg, extra={"event_id": ev.event_id})
            if h and a:
                target_pairs.add((h, a))

        if not target_pairs:
            print(f"  [polymarket {lg:<8}] no schedule pairs resolved — skipping.",
                  flush=True)
            continue

        try:
            rows, parsed, matched = poll_polymarket(
                sb, http, matcher, lg, target_pairs=target_pairs)
            flag = ""
            if parsed == 0:
                flag = "  ⚠ no match-level events returned by Gamma"
            elif matched == 0:
                flag = "  ⚠ parsed events but none matched schedule pairs"
            print(f"  [polymarket {lg:<8}] parsed={parsed:3d} matched={matched:2d} "
                  f"wrote={rows:3d} rows{flag}", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [polymarket {lg}] ERROR: {exc!r}", flush=True)
            traceback.print_exc()

    if last_quota_remaining.isdigit() and int(last_quota_remaining) < QUOTA_WARN_THRESHOLD:
        print(f"  ⚠ LOW QUOTA: {last_quota_remaining} remaining", flush=True)
    dur = time.monotonic() - started
    print(f"cycle done in {dur:5.1f}s  "
          f"(polled {total_in_window} Pinnacle events)", flush=True)


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
    parser.add_argument("--active-hours-start", type=float, default=14.0,
                        help="Hour (0–23.99, may be fractional) in --timezone at which "
                             "polling resumes. Default 14.0 (14:00) — EU evening kickoffs.")
    parser.add_argument("--active-hours-end", type=float, default=22.5,
                        help="Hour (0–24, may be fractional) in --timezone at which "
                             "polling pauses. Default 22.5 (22:30). "
                             "If start==end, always active.")
    parser.add_argument("--schedule-scan-every", type=int,
                        default=SCHEDULE_SCAN_EVERY_SEC,
                        help=f"Seconds between free /events refreshes. "
                             f"Default {SCHEDULE_SCAN_EVERY_SEC}.")
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

    def _fmt_hm(h: float) -> str:
        hh = int(h)
        mm = int(round((h - hh) * 60))
        return f"{hh:02d}:{mm:02d}"

    print(f"Phase 1 logger starting. leagues={args.leagues} "
          f"interval={args.interval}s schedule-scan-every={args.schedule_scan_every}s "
          f"trade-window={TRADE_WINDOW_MIN}m duration={args.duration_hours}h "
          f"active={_fmt_hm(args.active_hours_start)}–{_fmt_hm(args.active_hours_end)} "
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
            schedule = poll_schedule(http, api_key, args.leagues)
            one_cycle(sb, http, matcher, api_key, args.leagues, schedule)
            return 0

        schedule: dict[str, list[EventSummary]] = {}
        last_scan_at = 0.0

        while not _stop:
            if run_deadline is not None and time.monotonic() >= run_deadline:
                print(f"\nDuration budget of {args.duration_hours}h reached — exiting cleanly.",
                      flush=True)
                return 0

            sleep_to_next_active = _inactive_sleep_seconds(
                tz, args.active_hours_start, args.active_hours_end)
            if sleep_to_next_active > 0:
                start_h = int(args.active_hours_start)
                start_m = int(round((args.active_hours_start - start_h) * 60))
                wake_local = datetime.now(tz).replace(
                    hour=start_h, minute=start_m, second=0, microsecond=0)
                if wake_local <= datetime.now(tz):
                    wake_local = wake_local + timedelta(days=1)
                print(f"\n[idle] outside active window "
                      f"{_fmt_hm(args.active_hours_start)}–"
                      f"{_fmt_hm(args.active_hours_end)} "
                      f"{args.timezone}. Sleeping {sleep_to_next_active/60:.0f} min "
                      f"until {wake_local.strftime('%Y-%m-%d %H:%M %Z')}.",
                      flush=True)
                deadline = time.monotonic() + sleep_to_next_active
                while not _stop and time.monotonic() < deadline:
                    if run_deadline is not None and time.monotonic() >= run_deadline:
                        return 0
                    time.sleep(min(60.0, deadline - time.monotonic()))
                continue

            now_mono = time.monotonic()
            if not schedule or (now_mono - last_scan_at) >= args.schedule_scan_every:
                print(f"\n--- schedule scan @ {datetime.now(timezone.utc).isoformat()} ---",
                      flush=True)
                schedule = poll_schedule(http, api_key, args.leagues)
                last_scan_at = now_mono

            cycle_start = time.monotonic()
            print(f"\n--- cycle @ {datetime.now(timezone.utc).isoformat()} ---",
                  flush=True)
            one_cycle(sb, http, matcher, api_key, args.leagues, schedule)
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
