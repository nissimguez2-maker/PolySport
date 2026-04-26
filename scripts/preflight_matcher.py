"""Preflight: calibrate the teams-alias seed against real Polymarket data.

Queries Polymarket per target league (EPL, UCL, UEL, Serie A, La Liga,
Bundesliga, Ligue 1) and reports which team names would resolve against our
teams table and which wouldn't. The output tells us exactly what aliases to
add before the logger goes live.

No Odds API quota used. ~7 free Gamma API calls.
"""

from __future__ import annotations

import os
import re
import sys
from collections import Counter
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.feeds.polymarket import list_league_events

# Preflight scope: club leagues only. WC 2026 handled separately (national teams).
TARGET_LEAGUES = ["epl", "ucl", "uel", "seriea", "laliga", "bundesliga", "ligue1"]

# Strip these trailing/leading noise fragments from titles before "X vs Y" parsing.
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


def normalize(name: str) -> str:
    return name.strip().lower()


def clean_title(title: str) -> str:
    t = title.strip()
    t = TRAILING_NOISE.sub("", t)
    t = LEAGUE_PREFIX.sub("", t)
    return t.strip()


def load_alias_map(sb) -> dict[str, tuple[str, str]]:
    """Map lowercased alias/canonical -> (canonical_name, league)."""
    teams = sb.table("teams").select("canonical_name, aliases, league").execute().data
    out: dict[str, tuple[str, str]] = {}
    for t in teams:
        out[normalize(t["canonical_name"])] = (t["canonical_name"], t["league"])
        for a in t["aliases"] or []:
            out[normalize(a)] = (t["canonical_name"], t["league"])
    return out


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    alias_to_team = load_alias_map(sb)
    print(f"Loaded {len(alias_to_team)} alias keys from teams table.\n")

    per_league_stats: dict[str, dict] = {}
    missing_names: Counter[str] = Counter()
    unparseable_titles: list[str] = []
    resolved_samples: list[str] = []

    with httpx.Client() as http:
        for league in TARGET_LEAGUES:
            events = list_league_events(http, league)
            match_events = [e for e in events if " vs" in (e.get("title") or "").lower()]

            parseable = 0
            resolved = 0
            partial = 0
            for e in match_events:
                raw_title = e.get("title") or ""
                title = clean_title(raw_title)
                m = TITLE_PATTERN.match(title)
                if not m:
                    unparseable_titles.append(raw_title)
                    continue
                parseable += 1

                home = m.group("home").strip()
                away = m.group("away").strip()
                home_hit = alias_to_team.get(normalize(home))
                away_hit = alias_to_team.get(normalize(away))

                if home_hit and away_hit:
                    resolved += 1
                    if len(resolved_samples) < 10:
                        resolved_samples.append(f"  [{league:<10}]  {raw_title}")
                else:
                    partial += 1
                    if not home_hit:
                        missing_names[home] += 1
                    if not away_hit:
                        missing_names[away] += 1

            per_league_stats[league] = {
                "events_total": len(events),
                "per_match_events": len(match_events),
                "parseable": parseable,
                "fully_resolved": resolved,
                "partial_or_missing": partial,
            }

    print("=" * 78)
    print(
        f"{'LEAGUE':<12} {'events':>7} {'per-match':>10} {'parseable':>10} {'resolved':>9} {'missing':>8}"
    )
    print("-" * 78)
    for league, s in per_league_stats.items():
        print(
            f"{league:<12} {s['events_total']:>7} {s['per_match_events']:>10} "
            f"{s['parseable']:>10} {s['fully_resolved']:>9} {s['partial_or_missing']:>8}"
        )
    total_parseable = sum(s["parseable"] for s in per_league_stats.values())
    total_resolved = sum(s["fully_resolved"] for s in per_league_stats.values())
    rate = (total_resolved / total_parseable * 100) if total_parseable else 0.0
    print("-" * 78)
    print(f"{'TOTAL':<12} {'':<7} {'':<10} {total_parseable:>10} {total_resolved:>9} ({rate:.1f}%)")

    print("\n" + "=" * 78)
    print(f"TOP {min(60, len(missing_names))} MISSING NAMES — add these as aliases")
    print("=" * 78)
    for name, cnt in missing_names.most_common(60):
        print(f"  {cnt:3d}×  {name}")

    if unparseable_titles:
        print("\n" + "=" * 78)
        print(f"UNPARSEABLE TITLES (sample of {min(15, len(unparseable_titles))})")
        print("=" * 78)
        for t in unparseable_titles[:15]:
            print(f"  {t}")

    if resolved_samples:
        print("\n" + "=" * 78)
        print("SAMPLE FULLY-RESOLVED MATCHES (sanity check)")
        print("=" * 78)
        for s in resolved_samples:
            print(s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
