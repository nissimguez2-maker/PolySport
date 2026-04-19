"""Phase 1 connectivity check: verify The Odds API key and inspect EPL/UCL coverage."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv

SOCCER_LEAGUES = ["soccer_epl", "soccer_uefa_champs_league"]


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    key = os.environ.get("ODDS_API_KEY")
    if not key:
        print("ERROR: ODDS_API_KEY not set in .env")
        return 1

    resp = httpx.get(
        "https://api.the-odds-api.com/v4/sports",
        params={"apiKey": key},
        timeout=10.0,
    )
    if resp.status_code != 200:
        print(f"ERROR: /sports returned {resp.status_code}: {resp.text}")
        return 1

    sports = resp.json()
    active_soccer = [s for s in sports if s["key"].startswith("soccer_") and s["active"]]
    wanted = [s for s in active_soccer if s["key"] in SOCCER_LEAGUES]

    remaining = resp.headers.get("x-requests-remaining", "?")
    used = resp.headers.get("x-requests-used", "?")

    print(f"Connection OK. Quota: {used} used, {remaining} remaining this month.")
    print(f"Active soccer leagues: {len(active_soccer)}")
    print(f"Target leagues present (EPL + UCL): {len(wanted)}/{len(SOCCER_LEAGUES)}")
    for s in wanted:
        print(f"  - {s['key']}: {s['title']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
