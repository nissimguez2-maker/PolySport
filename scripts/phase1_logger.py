"""Phase 1: 48h standalone divergence logger.

Per STRATEGY.md Phase 1 goal: log Pinnacle + Polymarket snapshots for every
upcoming EPL/UCL match for ~48h. No trading, no decisions — the whole point is
to measure whether ≥30% of monitored matches touch |div| ≥ 2¢ in the
T-120→T+0min window. If they do, the strategy is viable.

Design:
  - Poll each target league every POLL_INTERVAL seconds. Cadence is tiered:
    coarse (60s default) for T-180 → T-60, fine (30s default) for T-60 →
    kickoff. Rationale: credits are scarce and divergence moves slowly
    early in the window, but the final hour is where edges actually
    appear/vanish as Pinnacle re-vigs into the close.
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

Cadence config (env-var overridable at the top of this file):
  POLL_INTERVAL_COARSE_SEC=75   poll interval for T-180 → T-60 matches
  POLL_INTERVAL_FINE_SEC=75     poll interval once any match enters T-60 → kickoff
                                (same as coarse — see comment at the constant)
  POLL_FINE_THRESHOLD_MIN=60    minutes-to-kick boundary between the two bands

Active-hours default (2026-04-26): always-on. The previous 14:00–22:30 IL
window missed every MLS kickoff (02:00–05:30 IL). Always-on adds ~5–10k
credits/month for MLS coverage, which the upgraded 100k tier accommodates.

Quota math (illustrative, 9 default leagues, always-on, 75s cadence):
  Coarse band: 1 call / match / 75s × 120 min coarse window ≈  96 calls.
  Fine band:   1 call / match / 75s × 60 min fine window    =  48 calls.
  Halftime:    1 call / match / 75s × 15 min halftime band  =  12 calls.
  Per fully-tracked match: 96 + 48 + 12 ≈ 156 credits.
  At ~270 matches/month across 9 leagues: ≈ 42k credits/mo — well under
  the 100k Odds API tier with ~58k buffer. To tighten cadence we'd need
  cross-league parallelism (sequential per-league processing caps the
  cycle at ~65–70s in prod). See STRATEGY.md "Timing".
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.data.paper_trades import record_signal as record_paper_trade
from polysport.data.resolver import resolve_batch
from polysport.feeds.matcher import TeamMatcher
from polysport.feeds.odds_api import (
    LEAGUE_TO_SPORT_KEY,
    EventSummary,
    fetch_events_for_league,
    fetch_odds_for_event,
)
from polysport.feeds.polymarket import (
    MoneylineMarket,
    extract_moneyline_markets,
    fetch_book,
    list_league_events,
)
from polysport.math.devig import devig_3way
from polysport.sim.honest_fill import EntrySignal as SimEntrySignal
from polysport.sim.honest_fill import ExitPlan, simulate_round_trip
from polysport.strategy.moneyline import EntrySignal, Outcome, evaluate_entry

DEFAULT_LEAGUES = [
    "epl",
    "ucl",
    "ligue1",
    "seriea",
    "laliga",
    "bundesliga",
    "mls",
    "eredivisie",
    "primeira",
]

# Cadence config. Env vars win over the defaults so Railway can tune without
# redeploying code. Both bands set to 75s on 2026-04-26 after empirical
# cycle-time measurement: even with parallel PM book fetches, sequential
# per-league processing of 9 leagues pegs each cycle at ~65–70s in prod.
# 75s gives a small headroom buffer above measured worst-case so the
# "running hot" warning stays silent. Cross-league parallelism is the
# next lever if Phase 1 data shows the strategy is missing fast price
# moves; not worth the complexity until then.
POLL_INTERVAL_COARSE_SEC = int(os.getenv("POLL_INTERVAL_COARSE_SEC", "75"))
POLL_INTERVAL_FINE_SEC = int(os.getenv("POLL_INTERVAL_FINE_SEC", "75"))
POLL_FINE_THRESHOLD_MIN = int(os.getenv("POLL_FINE_THRESHOLD_MIN", "60"))

TRADE_WINDOW_MIN = 120  # poll /events/{id}/odds only for matches kicking off within this many min
SCHEDULE_SCAN_EVERY_SEC = 600  # free /events refresh cadence (10 min)
QUOTA_WARN_THRESHOLD = 500  # warn once Odds API remaining drops below this
QUOTA_HARD_FLOOR = 100  # skip paid Pinnacle calls below this (free Polymarket continues)

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
    active = (
        (start_hour <= h < end_hour) if start_hour < end_hour else (h >= start_hour or h < end_hour)
    )
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
    return (ct - datetime.now(UTC)).total_seconds() / 60.0


def _in_pre_match_window(commence_iso: str, window_min: int = TRADE_WINDOW_MIN) -> bool:
    """Pre-match band: kickoff in [0, window_min] minutes ahead."""
    m = _minutes_to_kick(commence_iso)
    return m is not None and 0.0 <= m <= window_min


def _in_halftime_window(commence_iso: str) -> bool:
    """Halftime band: T+45 → T+60 (the break between halves).

    STRATEGY.md treats halftime as a separate trade window — pricing
    can shift during the 15-min break before the second half starts.
    NOT live in-play (the game is paused, latency-tolerant), so it's
    consistent with the "no live in-play" rule.

    A coverage audit on 2026-04-26 found 0/17 matches had any halftime
    poll because the previous _in_trade_window only matched non-negative
    minutes_to_kick. Halftime predicate added separately so the two
    bands stay distinct in callers that need to know which one fired.
    """
    m = _minutes_to_kick(commence_iso)
    return m is not None and -60.0 <= m <= -45.0


def _in_trade_window(commence_iso: str, window_min: int = TRADE_WINDOW_MIN) -> bool:
    """True iff the match is currently in either active trade band:
    pre-match [T-window_min, T-0] or halftime [T+45, T+60]."""
    return _in_pre_match_window(commence_iso, window_min) or _in_halftime_window(commence_iso)


def _cadence_for_cycle(
    schedule: dict[str, list[EventSummary]],
    *,
    coarse_sec: int,
    fine_sec: int,
    fine_threshold_min: int,
) -> int:
    """Pick the poll interval for the next cycle.

    Fine cadence applies when any match is either:
      - within fine_threshold_min of kickoff (final-hour edge volatility), or
      - currently in halftime (15-min window, every snapshot is precious).
    """
    for evs in schedule.values():
        for ev in evs:
            m = _minutes_to_kick(ev.commence_time)
            if m is None:
                continue
            if 0.0 <= m <= fine_threshold_min:
                return fine_sec
            if -60.0 <= m <= -45.0:
                return fine_sec
    return coarse_sec


def poll_schedule(
    http: httpx.Client, api_key: str, leagues: list[str]
) -> dict[str, list[EventSummary]]:
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
        except Exception as exc:
            print(f"  [schedule {lg}] ERROR: {exc!r}", flush=True)
            out[lg] = []
    return out


def poll_event_odds(
    sb, http: httpx.Client, matcher: TeamMatcher, api_key: str, ev: EventSummary
) -> tuple[int, dict[str, str]]:
    """Paid per-event odds poll for one in-window match."""
    odds_ev, quota = fetch_odds_for_event(
        http, api_key=api_key, sport_key=ev.sport_key, event_id=ev.event_id
    )
    home_id = _resolve(
        matcher,
        odds_ev.home_team_raw,
        source="odds_api",
        league=ev.league_slug,
        extra={"event_id": ev.event_id},
    )
    away_id = _resolve(
        matcher,
        odds_ev.away_team_raw,
        source="odds_api",
        league=ev.league_slug,
        extra={"event_id": ev.event_id},
    )
    rows = 0
    for bm in odds_ev.bookmakers:
        sb.table("odds_api_snapshots").insert(
            {
                "event_id": odds_ev.event_id,
                "league_key": odds_ev.sport_key,
                "home_team_raw": odds_ev.home_team_raw,
                "away_team_raw": odds_ev.away_team_raw,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "commence_time": odds_ev.commence_time,
                "bookmaker": bm.bookmaker,
                "odds_home": bm.odds_home,
                "odds_draw": bm.odds_draw,
                "odds_away": bm.odds_away,
                "raw": odds_ev.raw,
            }
        ).execute()
        rows += 1
    return rows, quota


# Concurrent book fetches per cycle. The HTTP client's connection pool is
# 20, so we cap the worker count at the same number — no point asking for
# more concurrency than the pool can serve.
PM_BOOK_FETCH_WORKERS = 20


def poll_polymarket(
    sb,
    http: httpx.Client,
    matcher: TeamMatcher,
    league: str,
    *,
    target_pairs: set[tuple[str, str]] | None = None,
) -> tuple[int, int, int]:
    """Fetch one league's Polymarket events + books, write snapshots.

    target_pairs: optional set of (home_team_id, away_team_id). When provided,
    only write rows for PM events whose resolved team pair appears here. Lets
    us scope /book spend to matches that have an Odds API schedule entry —
    everything else (season markets, matches we don't track) is skipped.

    Pipeline:
      1. Parse + match events sequentially (matcher cache + Supabase
         look-ups dominated by single-thread DB latency, parallelism
         doesn't help).
      2. Fetch all eligible books in parallel — this is the cycle's
         biggest latency sink (one HTTP round-trip per market).
      3. Write snapshots sequentially. Supabase client is requests-based
         and fine to share, but serialising writes keeps log output
         deterministic and dodges any per-table quotas.

    Returns (rows_written, events_parsed, events_matched).
    """
    events = list_league_events(http, league)

    # Step 1: walk events, build a flat list of (context, market) pairs
    # that need a book fetch. `context` carries everything the writer
    # needs that isn't on the market itself.
    fetch_jobs: list[tuple[dict, MoneylineMarket]] = []
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
        home_id = _resolve(
            matcher,
            home_raw,
            source="polymarket",
            league=league,
            extra={"event_id": e.get("id"), "title": raw_title},
        )
        away_id = _resolve(
            matcher,
            away_raw,
            source="polymarket",
            league=league,
            extra={"event_id": e.get("id"), "title": raw_title},
        )
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
        ctx = {
            "event_id": str(e.get("id")),
            "event_slug": e.get("slug"),
            "home_id": home_id,
            "away_id": away_id,
            "commence_iso": commence.isoformat() if commence else None,
        }
        for mkt in markets:
            fetch_jobs.append((ctx, mkt))

    if not fetch_jobs:
        return 0, parsed, matched

    # Step 2: fetch all books concurrently. Per-job exceptions are
    # captured into the result and logged in step 3 — one bad token
    # shouldn't kill the whole league.
    def _fetch(job: tuple[dict, MoneylineMarket]) -> tuple[dict, MoneylineMarket, object, object]:
        ctx, mkt = job
        try:
            book = fetch_book(http, mkt.yes_token_id)
            return ctx, mkt, book, None
        except httpx.HTTPError as exc:
            return ctx, mkt, None, exc

    results: list[tuple[dict, MoneylineMarket, object, object]] = []
    with ThreadPoolExecutor(max_workers=PM_BOOK_FETCH_WORKERS) as pool:
        for fut in as_completed(pool.submit(_fetch, j) for j in fetch_jobs):
            results.append(fut.result())

    # Step 3: serial writes.
    rows = 0
    for ctx, mkt, book, exc in results:
        if exc is not None:
            print(f"  [pm book {mkt.outcome_side}] {exc}", flush=True)
            continue
        bid_depth = (
            book.best_bid * book.bid_size_shares if book.best_bid and book.bid_size_shares else None
        )
        ask_depth = (
            book.best_ask * book.ask_size_shares if book.best_ask and book.ask_size_shares else None
        )
        sb.table("polymarket_snapshots").insert(
            {
                "event_id": ctx["event_id"],
                "market_id": mkt.condition_id,
                "outcome_raw": mkt.question,
                "outcome_side": mkt.outcome_side,
                "home_team_id": ctx["home_id"],
                "away_team_id": ctx["away_id"],
                "commence_time": ctx["commence_iso"],
                "best_bid": book.best_bid,
                "best_ask": book.best_ask,
                "best_bid_depth_usd": bid_depth,
                "best_ask_depth_usd": ask_depth,
                "raw": {
                    "event_id": ctx["event_id"],
                    "event_slug": ctx["event_slug"],
                    "market_slug": mkt.slug,
                    "yes_token_id": mkt.yes_token_id,
                    "book": book.raw,
                },
            }
        ).execute()
        rows += 1
    return rows, parsed, matched


def _resolve(
    matcher: TeamMatcher, raw: str, *, source: str, league: str, extra: dict
) -> str | None:
    r = matcher.resolve(raw, source=source, league_hint=league, context={"league": league, **extra})
    return r.team_id if r else None


def _read_quota_remaining(sb) -> int | None:
    """Latest known Odds API remaining-quota reading from the singleton row.

    Returns None on first run / migration not applied / read error so the
    caller can default to "proceed" rather than wedging the logger.
    """
    try:
        rows = (sb.table("odds_api_quota").select("remaining").eq("id", 1).limit(1).execute()).data
        if rows and rows[0].get("remaining") is not None:
            return int(rows[0]["remaining"])
    except Exception as exc:
        print(f"  [quota read] WARN: {exc!r}", flush=True)
    return None


def _persist_quota(sb, quota: dict[str, str]) -> None:
    """Upsert latest Odds API quota headers into the singleton odds_api_quota row.

    Non-fatal — the logger must not crash if the table is absent (e.g. migration
    not yet applied) or if a header is missing.
    """
    try:
        payload = {
            "id": 1,
            "remaining": int(quota["remaining"]) if quota.get("remaining", "").isdigit() else None,
            "used": int(quota["used"]) if quota.get("used", "").isdigit() else None,
            "last_cost": int(quota["last"]) if quota.get("last", "").isdigit() else None,
            "updated_at": "now()",
        }
        sb.table("odds_api_quota").upsert(payload).execute()
    except Exception as exc:
        print(f"  [quota persist] WARN: {exc!r}", flush=True)


def one_cycle(
    sb,
    http: httpx.Client,
    matcher: TeamMatcher,
    api_key: str,
    leagues: list[str],
    schedule: dict[str, list[EventSummary]],
) -> None:
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
        print(
            f"  [idle] no matches in T-{TRADE_WINDOW_MIN}m window across "
            f"{len(leagues)} leagues — skipping paid Odds API calls.",
            flush=True,
        )

    # Hard quota floor: stop spending paid Odds API calls below this. Polymarket
    # is free so it keeps polling — partial data beats running into 429s.
    quota_remaining = _read_quota_remaining(sb)
    quota_blocked = quota_remaining is not None and quota_remaining < QUOTA_HARD_FLOOR
    if quota_blocked:
        print(
            f"  ⚠ QUOTA FLOOR: {quota_remaining} remaining < {QUOTA_HARD_FLOOR} — "
            f"skipping all paid Odds API calls this cycle.",
            flush=True,
        )

    last_quota_remaining = "?" if quota_remaining is None else str(quota_remaining)
    paid_pinnacle_iter = [] if quota_blocked else list(in_window.items())
    for lg, evs in paid_pinnacle_iter:
        for ev in evs:
            mins = _minutes_to_kick(ev.commence_time) or 0.0
            try:
                n, quota = poll_event_odds(sb, http, matcher, api_key, ev)
                last_quota_remaining = quota.get("remaining", "?")
                _persist_quota(sb, quota)
                # Halftime polls show as "T+50m" etc.; pre-match keeps the
                # familiar "T-30m" countdown. Sign comes from minutes_to_kick.
                when = f"T-{mins:4.0f}m" if mins >= 0 else f"T+{-mins:3.0f}m"
                tag = "HT " if _in_halftime_window(ev.commence_time) else "   "
                print(
                    f"  [odds_api {lg:<10}] {tag}{ev.home_team_raw} vs "
                    f"{ev.away_team_raw}  {when}  "
                    f"wrote {n} rows  quota_remaining={last_quota_remaining}",
                    flush=True,
                )
            except Exception as exc:
                print(f"  [odds_api {lg}] ERROR on {ev.event_id}: {exc!r}", flush=True)
                traceback.print_exc()

    # Polymarket poll: every league every cycle, scoped to schedule team pairs.
    for lg in leagues:
        sched_evs = schedule.get(lg, [])
        target_pairs: set[tuple[str, str]] = set()
        for ev in sched_evs:
            h = _resolve(
                matcher,
                ev.home_team_raw,
                source="odds_api",
                league=lg,
                extra={"event_id": ev.event_id},
            )
            a = _resolve(
                matcher,
                ev.away_team_raw,
                source="odds_api",
                league=lg,
                extra={"event_id": ev.event_id},
            )
            if h and a:
                target_pairs.add((h, a))

        if not target_pairs:
            print(f"  [polymarket {lg:<8}] no schedule pairs resolved — skipping.", flush=True)
            continue

        try:
            rows, parsed, matched = poll_polymarket(
                sb, http, matcher, lg, target_pairs=target_pairs
            )
            flag = ""
            if parsed == 0:
                flag = "  ⚠ no match-level events returned by Gamma"
            elif matched == 0:
                flag = "  ⚠ parsed events but none matched schedule pairs"
            print(
                f"  [polymarket {lg:<8}] parsed={parsed:3d} matched={matched:2d} "
                f"wrote={rows:3d} rows{flag}",
                flush=True,
            )
        except Exception as exc:
            print(f"  [polymarket {lg}] ERROR: {exc!r}", flush=True)
            traceback.print_exc()

    if last_quota_remaining.isdigit() and int(last_quota_remaining) < QUOTA_WARN_THRESHOLD:
        print(f"  ⚠ LOW QUOTA: {last_quota_remaining} remaining", flush=True)

    # Paper-trade tape: evaluate every in-window match against the full
    # strategy gates and INSERT one row per first-fire. Defensive — any
    # failure here logs and continues; the cycle's primary job (raw feed
    # logging) is already complete by this point.
    try:
        n_new = _evaluate_and_record_paper_trades(sb)
        if n_new:
            print(f"  [paper] +{n_new} new paper-trade entries.", flush=True)
    except Exception as exc:
        print(f"  [paper] WARN: {exc!r}", flush=True)
        traceback.print_exc()

    dur = time.monotonic() - started
    print(f"cycle done in {dur:5.1f}s  (polled {total_in_window} Pinnacle events)", flush=True)


def _evaluate_and_record_paper_trades(sb) -> int:
    """For every match currently in a trade window with fresh (≤120s) data
    on both feeds, run moneyline.evaluate_entry. Insert one paper_trades
    row per first-fire (unique constraint dedupes subsequent same-match
    fires automatically).

    The 120s freshness window is the staleness limit; evaluate_entry
    itself enforces the 60s pinnacle staleness gate from STRATEGY.md
    so the larger pull window just gives us margin to find the latest
    valid Pinnacle for each Polymarket moment.
    """
    now = datetime.now(UTC)
    recent_cutoff = (now - timedelta(minutes=10)).isoformat()

    pin_rows = (
        sb.table("odds_api_snapshots")
        .select(
            "home_team_id, away_team_id, commence_time, odds_home, odds_draw, odds_away, polled_at"
        )
        .eq("bookmaker", "pinnacle")
        .gte("polled_at", recent_cutoff)
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
            "home_team_id, away_team_id, outcome_side, market_id, raw, "
            "best_bid, best_ask, best_bid_depth_usd, best_ask_depth_usd, polled_at"
        )
        .gte("polled_at", recent_cutoff)
        .not_.is_("home_team_id", "null")
        .not_.is_("outcome_side", "null")
        .order("polled_at", desc=True)
        .execute()
        .data
    )

    # Latest Pinnacle per (home, away). Dedup on team-pair only — same
    # rationale as dashboard.data: kickoff jitter creates phantom matches
    # at minute precision.
    pin_by_match: dict[tuple[str, str], dict] = {}
    for r in pin_rows:
        if not r.get("commence_time"):
            continue
        pin_key: tuple[str, str] = (r["home_team_id"], r["away_team_id"])
        if pin_key not in pin_by_match:
            pin_by_match[pin_key] = r

    # Latest PM per (home, away, outcome).
    pm_by_outcome: dict[tuple[str, str, str], dict] = {}
    for r in pm_rows:
        pm_key: tuple[str, str, str] = (
            r["home_team_id"],
            r["away_team_id"],
            r["outcome_side"],
        )
        if pm_key not in pm_by_outcome:
            pm_by_outcome[pm_key] = r

    inserted = 0
    for (home_id, away_id), pin in pin_by_match.items():
        commence_iso = pin["commence_time"]
        if not (_in_pre_match_window(commence_iso) or _in_halftime_window(commence_iso)):
            continue

        kickoff = _parse_iso8601(commence_iso)
        if kickoff is None:
            continue

        pin_polled = _parse_iso8601(pin["polled_at"])
        if pin_polled is None:
            continue
        pin_staleness_sec = (now - pin_polled).total_seconds()

        try:
            fair = devig_3way(
                float(pin["odds_home"]), float(pin["odds_draw"]), float(pin["odds_away"])
            )
        except (ValueError, TypeError):
            continue

        outcomes: dict[str, Outcome] = {}
        complete = True
        for side, fair_p in (("home", fair.home), ("draw", fair.draw), ("away", fair.away)):
            pm = pm_by_outcome.get((home_id, away_id, side))
            if not pm:
                complete = False
                break
            bid, ask = pm.get("best_bid"), pm.get("best_ask")
            if bid is None or ask is None:
                complete = False
                break
            d_bid, d_ask = pm.get("best_bid_depth_usd"), pm.get("best_ask_depth_usd")
            if d_bid is None or d_ask is None:
                complete = False
                break
            outcomes[side] = Outcome(
                side=side,  # type: ignore[arg-type]
                fair=fair_p,
                best_bid=float(bid),
                best_ask=float(ask),
                depth_usd=min(float(d_bid), float(d_ask)),
            )
        if not complete:
            continue

        decision = evaluate_entry(
            outcomes,  # type: ignore[arg-type]
            pinnacle_staleness_sec=pin_staleness_sec,
            has_position=False,  # DB unique constraint enforces single-leg
        )
        if not isinstance(decision, EntrySignal):
            continue

        target = outcomes[decision.target_outcome]
        # Capture Polymarket keys from the matched outcome's snapshot so the
        # resolver (scripts/resolve_paper_trades.py) can settle this row
        # post-match without having to re-derive the market via the matcher.
        target_pm = pm_by_outcome.get((home_id, away_id, decision.target_outcome)) or {}
        target_condition_id = target_pm.get("market_id")
        target_raw = target_pm.get("raw") or {}
        target_yes_token_id = (
            target_raw.get("yes_token_id") if isinstance(target_raw, dict) else None
        )
        sim_entry = SimEntrySignal(
            match_id=f"{home_id}_{away_id}_{int(kickoff.timestamp())}",
            side="buy",
            outcome_side=decision.target_outcome,
            polymarket_mid=target.mid,
            polymarket_best_ask=target.best_ask,
            polymarket_best_bid=target.best_bid,
            pinnacle_fair=target.fair,
            notional_usd=5.0,  # STRATEGY.md Stage 1
            t_minutes_to_kick=(kickoff - now).total_seconds() / 60.0,
        )
        sim_result = simulate_round_trip(
            entry=sim_entry, exit_plan=ExitPlan(kind="hold-to-settlement")
        )

        was_new = record_paper_trade(
            sb,
            home_team_id=home_id,
            away_team_id=away_id,
            kickoff=kickoff,
            minutes_to_kick=(kickoff - now).total_seconds() / 60.0,
            target_outcome=decision.target_outcome,
            side="buy",
            limit_price=decision.limit_price,
            expected_edge=decision.expected_edge,
            fair=target.fair,
            mid=target.mid,
            pinnacle_staleness_sec=pin_staleness_sec,
            notional_usd=5.0,
            sim_entry_price=sim_result.entry_price,
            sim_net_pnl_ev=sim_result.net_pnl,
            polymarket_condition_id=target_condition_id,
            polymarket_yes_token_id=target_yes_token_id,
        )
        if was_new:
            inserted += 1

    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--leagues",
        nargs="+",
        default=DEFAULT_LEAGUES,
        help=f"League slugs to poll. Default: {DEFAULT_LEAGUES}. "
        f"Valid: {list(LEAGUE_TO_SPORT_KEY)}",
    )
    parser.add_argument(
        "--interval-coarse",
        type=int,
        default=POLL_INTERVAL_COARSE_SEC,
        help=f"Seconds between polls for matches further than "
        f"--fine-threshold minutes from kickoff. "
        f"Default: {POLL_INTERVAL_COARSE_SEC} "
        f"(env POLL_INTERVAL_COARSE_SEC).",
    )
    parser.add_argument(
        "--interval-fine",
        type=int,
        default=POLL_INTERVAL_FINE_SEC,
        help=f"Seconds between polls once any match is inside "
        f"--fine-threshold minutes of kickoff. "
        f"Default: {POLL_INTERVAL_FINE_SEC} "
        f"(env POLL_INTERVAL_FINE_SEC).",
    )
    parser.add_argument(
        "--fine-threshold",
        type=int,
        default=POLL_FINE_THRESHOLD_MIN,
        help=f"Minutes-to-kick boundary: at or below this, "
        f"cadence switches from coarse to fine. "
        f"Default: {POLL_FINE_THRESHOLD_MIN} "
        f"(env POLL_FINE_THRESHOLD_MIN).",
    )
    parser.add_argument(
        "--once", action="store_true", help="Run a single cycle and exit (for smoke tests)."
    )
    parser.add_argument(
        "--duration-hours",
        type=float,
        default=0.0,
        help="Total run time in hours. Script exits 0 when exceeded. "
        "Default 0 = unlimited; the original 48h cap (STRATEGY.md Phase 1) "
        "produced n=12 matches which was too small to evaluate the touch-rate "
        "gate. Continuous run lets the touch-rate sample size grow until the "
        "gate verdict is statistically meaningful. Override per-run if you "
        "want the bounded-window behaviour.",
    )
    parser.add_argument(
        "--active-hours-start",
        type=float,
        default=0.0,
        help="Hour (0–23.99, may be fractional) in --timezone at which "
        "polling resumes. Default 0.0 — combined with --active-hours-end=0.0 "
        "this is always-active (start==end ⇒ no idle window). The "
        "always-active default exists so US-evening MLS games (~02:00–05:30 IL) "
        "actually get polled; the previous 14:00–22:30 default missed them.",
    )
    parser.add_argument(
        "--active-hours-end",
        type=float,
        default=0.0,
        help="Hour (0–24, may be fractional) in --timezone at which "
        "polling pauses. Default 0.0 — combined with start==0.0 this is "
        "always-active. Set start≠end to enforce a quiet window.",
    )
    parser.add_argument(
        "--schedule-scan-every",
        type=int,
        default=SCHEDULE_SCAN_EVERY_SEC,
        help=f"Seconds between free /events refreshes. Default {SCHEDULE_SCAN_EVERY_SEC}.",
    )
    parser.add_argument(
        "--timezone",
        default="Asia/Jerusalem",
        help="IANA timezone for active-hours gate. Default Asia/Jerusalem.",
    )
    args = parser.parse_args()
    tz = ZoneInfo(args.timezone)

    for lg in args.leagues:
        if lg not in LEAGUE_TO_SPORT_KEY:
            print(f"WARN: league {lg!r} has no Odds API sport_key — Polymarket-only.", flush=True)

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])
    api_key = os.environ["ODDS_API_KEY"]

    matcher = TeamMatcher(sb)
    _install_sigterm_handler()

    def _fmt_hm(h: float) -> str:
        hh = int(h)
        mm = int(round((h - hh) * 60))
        return f"{hh:02d}:{mm:02d}"

    print(
        f"Phase 1 logger starting. leagues={args.leagues} "
        f"cadence=coarse:{args.interval_coarse}s/fine:{args.interval_fine}s@T-{args.fine_threshold}m "
        f"schedule-scan-every={args.schedule_scan_every}s "
        f"trade-window={TRADE_WINDOW_MIN}m duration={args.duration_hours}h "
        f"active={_fmt_hm(args.active_hours_start)}–{_fmt_hm(args.active_hours_end)} "
        f"{args.timezone} once={args.once}",
        flush=True,
    )
    run_deadline = (
        time.monotonic() + args.duration_hours * 3600.0 if args.duration_hours > 0 else None
    )

    # One connection pool across all cycles — avoids TCP/TLS handshake per cycle.
    # Connection limits scale with the number of concurrent book fetches we do.
    with httpx.Client(
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=20),
        timeout=httpx.Timeout(15.0, connect=5.0),
    ) as http:
        schedule: dict[str, list[EventSummary]] = {}

        if args.once:
            schedule = poll_schedule(http, api_key, args.leagues)
            one_cycle(sb, http, matcher, api_key, args.leagues, schedule)
            return 0

        last_scan_at = 0.0
        # Hourly paper-trade settlement runs in-process (Option A): one
        # less Railway service to manage. resolve_batch is idempotent and
        # exception-isolated, so a flaky Gamma response can't hurt the
        # logger's primary job. First run fires ~3600s after boot.
        last_resolve_at = time.monotonic()
        resolve_interval_sec = 3600.0

        while not _stop:
            if run_deadline is not None and time.monotonic() >= run_deadline:
                print(
                    f"\nDuration budget of {args.duration_hours}h reached — exiting cleanly.",
                    flush=True,
                )
                return 0

            sleep_to_next_active = _inactive_sleep_seconds(
                tz, args.active_hours_start, args.active_hours_end
            )
            if sleep_to_next_active > 0:
                start_h = int(args.active_hours_start)
                start_m = int(round((args.active_hours_start - start_h) * 60))
                wake_local = datetime.now(tz).replace(
                    hour=start_h, minute=start_m, second=0, microsecond=0
                )
                if wake_local <= datetime.now(tz):
                    wake_local = wake_local + timedelta(days=1)
                print(
                    f"\n[idle] outside active window "
                    f"{_fmt_hm(args.active_hours_start)}–"
                    f"{_fmt_hm(args.active_hours_end)} "
                    f"{args.timezone}. Sleeping {sleep_to_next_active / 60:.0f} min "
                    f"until {wake_local.strftime('%Y-%m-%d %H:%M %Z')}.",
                    flush=True,
                )
                deadline = time.monotonic() + sleep_to_next_active
                while not _stop and time.monotonic() < deadline:
                    if run_deadline is not None and time.monotonic() >= run_deadline:
                        return 0
                    time.sleep(min(60.0, deadline - time.monotonic()))
                continue

            now_mono = time.monotonic()
            if not schedule or (now_mono - last_scan_at) >= args.schedule_scan_every:
                print(f"\n--- schedule scan @ {datetime.now(UTC).isoformat()} ---", flush=True)
                schedule = poll_schedule(http, api_key, args.leagues)
                last_scan_at = now_mono

            cadence_sec = _cadence_for_cycle(
                schedule,
                coarse_sec=args.interval_coarse,
                fine_sec=args.interval_fine,
                fine_threshold_min=args.fine_threshold,
            )
            cycle_start = time.monotonic()
            print(
                f"\n--- cycle @ {datetime.now(UTC).isoformat()} cadence={cadence_sec}s ---",
                flush=True,
            )
            one_cycle(sb, http, matcher, api_key, args.leagues, schedule)

            # Hourly resolver tick. Wrapped so any failure stays out of
            # the cycle's hot path — the logger's job is to keep logging
            # snapshots even if Polymarket Gamma is down.
            if (time.monotonic() - last_resolve_at) >= resolve_interval_sec:
                last_resolve_at = time.monotonic()
                try:
                    counts = resolve_batch(sb, http, max_rows=50)
                    if counts.settled or counts.errors:
                        print(
                            f"[resolver] settled={counts.settled} "
                            f"unresolved_skip={counts.skipped_unresolved} "
                            f"missing={counts.skipped_missing} "
                            f"errors={counts.errors}",
                            flush=True,
                        )
                except Exception as exc:
                    print(f"[resolver] failed (continuing): {exc}", flush=True)

            elapsed = time.monotonic() - cycle_start
            if elapsed > cadence_sec:
                print(
                    f"WARN: cycle took {elapsed:.1f}s > cadence {cadence_sec}s "
                    f"— running hot, consider raising cadence or trimming leagues",
                    flush=True,
                )
            sleep_for = max(0.0, cadence_sec - elapsed)
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
