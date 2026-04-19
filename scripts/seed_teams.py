"""Seed the teams table from TheSportsDB.

Ground truth for the matcher. Pulls canonical team names + alternate names for
the five top European leagues. Idempotent — re-running only upserts new aliases.

TheSportsDB test API key "3" is sufficient for one-time seeding.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client

# league slug in our schema -> TheSportsDB league name
# Top-5 + extra European leagues whose clubs appear in UCL / Europa League
# (needed so UEL/UCL-only names like SC Braga, Ajax, Galatasaray resolve).
LEAGUES = {
    "epl":        "English Premier League",
    "seriea":     "Italian Serie A",
    "laliga":     "Spanish La Liga",
    "bundesliga": "German Bundesliga",
    "ligue1":     "French Ligue 1",
    "primeira":   "Portuguese Primeira Liga",
    "eredivisie": "Dutch Eredivisie",
    "sueper":     "Turkish Super Lig",
    "belgian":    "Belgian First Division A",
    "scottish":   "Scottish Premiership",
    "swiss":      "Swiss Super League",
    "austrian":   "Austrian Bundesliga",
    "greek":      "Greek Super League",
}

SPORTSDB_BASE = "https://www.thesportsdb.com/api/v1/json/3"


def fetch_league_teams(client: httpx.Client, league_name: str) -> list[dict]:
    resp = client.get(
        f"{SPORTSDB_BASE}/search_all_teams.php",
        params={"l": league_name},
        timeout=30.0,
    )
    resp.raise_for_status()
    teams = resp.json().get("teams") or []
    return teams


def build_aliases(team: dict) -> list[str]:
    """Collect canonical + alternate names into an aliases list."""
    aliases = set()
    for field in ("strTeam", "strTeamShort", "strAlternate"):
        val = team.get(field)
        if not val:
            continue
        for piece in val.split(","):
            piece = piece.strip()
            if piece:
                aliases.add(piece)
    return sorted(aliases)


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    supabase = create_client(url, key)

    total_inserted = 0
    total_updated = 0

    with httpx.Client() as http:
        for league_slug, league_name in LEAGUES.items():
            print(f"Fetching {league_name}...")
            teams = fetch_league_teams(http, league_name)
            print(f"  {len(teams)} teams returned")

            for team in teams:
                canonical = team["strTeam"]
                country = team.get("strCountry")
                aliases = build_aliases(team)

                existing = (
                    supabase.table("teams")
                    .select("id, aliases")
                    .eq("canonical_name", canonical)
                    .eq("league", league_slug)
                    .execute()
                )

                if existing.data:
                    current = set(existing.data[0]["aliases"] or [])
                    merged = sorted(current | set(aliases))
                    if merged != sorted(current):
                        supabase.table("teams").update(
                            {"aliases": merged, "updated_at": "now()"}
                        ).eq("id", existing.data[0]["id"]).execute()
                        total_updated += 1
                else:
                    supabase.table("teams").insert({
                        "canonical_name": canonical,
                        "country":        country,
                        "league":         league_slug,
                        "aliases":        aliases,
                    }).execute()
                    total_inserted += 1

    print(f"\nDone. Inserted {total_inserted} new teams, updated {total_updated} existing.")

    count = supabase.table("teams").select("id", count="exact").execute()
    print(f"teams table now has {count.count} rows across {len(LEAGUES)} leagues.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
