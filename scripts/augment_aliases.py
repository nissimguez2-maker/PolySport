"""Augment teams.aliases by matching Polymarket event names to canonical teams.

Two-pass, fully deterministic:
  1. For each missing Polymarket team name, attempt a HIGH-confidence match by
     normalising both sides (strip diacritics, collapse club markers like FC/CF,
     map known city variants like München->munich). If exactly one team matches,
     it's auto-proposed.
  2. Names that don't auto-match are reported as NEEDS_REVIEW — we do NOT guess.

No changes are written until the user passes --apply.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.feeds.polymarket import list_league_events
from polysport.utils.text import normalise_name

TARGET_LEAGUES = ["epl", "ucl", "uel", "seriea", "laliga", "bundesliga", "ligue1"]

# Trailing noise on Polymarket titles — secondary-market subtypes that aren't moneyline.
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


def clean_title(title: str) -> str:
    t = TRAILING_NOISE.sub("", title.strip())
    t = LEAGUE_PREFIX.sub("", t)
    return t.strip()


def build_lookup(teams: list[dict]) -> tuple[dict, dict]:
    """Return (exact_alias_map, normalised_alias_map)."""
    exact: dict[str, tuple[str, str, str]] = {}
    norm: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for t in teams:
        tid = t["id"]
        cname = t["canonical_name"]
        lg = t["league"]
        for name in [cname] + (t["aliases"] or []):
            exact[name.strip().lower()] = (tid, cname, lg)
            n = normalise_name(name)
            if n:
                norm[n].append((tid, cname, lg))
    return exact, norm


def collect_missing_names(http: httpx.Client, exact_lookup: dict) -> dict[str, Counter]:
    """Fetch events per league, return league_slug -> Counter(missing_name -> frequency)."""
    missing: dict[str, Counter] = {lg: Counter() for lg in TARGET_LEAGUES}
    for league in TARGET_LEAGUES:
        events = list_league_events(http, league)
        for e in events:
            raw = e.get("title") or ""
            if " vs" not in raw.lower():
                continue
            title = clean_title(raw)
            m = TITLE_PATTERN.match(title)
            if not m:
                continue
            for side in (m.group("home"), m.group("away")):
                name = side.strip()
                if name.lower() not in exact_lookup:
                    missing[league][name] += 1
    return missing


def propose_aliases(
    missing_per_league: dict[str, Counter],
    norm_lookup: dict,
) -> tuple[dict, list]:
    """Return (proposals, unmatched).

    proposals: team_id -> set of new aliases to add
    unmatched: list of (league_hint, name, frequency) that didn't auto-resolve
    """
    proposals: dict[str, set[str]] = defaultdict(set)
    unmatched: list[tuple[str, str, int]] = []

    for league_hint, counter in missing_per_league.items():
        for name, freq in counter.items():
            n = normalise_name(name)
            if not n:
                unmatched.append((league_hint, name, freq))
                continue
            candidates = norm_lookup.get(n, [])
            # Dedupe by team_id — the same team may contribute multiple aliases
            # that all normalise to the same key.
            unique_team_ids = {team_id for team_id, _cname, _lg in candidates}
            # Only auto-accept if EXACTLY one distinct team matches.
            if len(unique_team_ids) == 1:
                team_id = next(iter(unique_team_ids))
                proposals[team_id].add(name)
            else:
                unmatched.append((league_hint, name, freq))
    return proposals, unmatched


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write approved aliases to Supabase. Without this flag, prints a dry run only.",
    )
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    teams = sb.table("teams").select("id, canonical_name, league, aliases").execute().data
    exact_lookup, norm_lookup = build_lookup(teams)

    with httpx.Client() as http:
        missing_per_league = collect_missing_names(http, exact_lookup)

    proposals, unmatched = propose_aliases(missing_per_league, norm_lookup)

    id_to_team = {t["id"]: t for t in teams}

    print("=" * 78)
    print(
        f"AUTO-MATCH PROPOSALS  ({sum(len(v) for v in proposals.values())} new aliases across {len(proposals)} teams)"
    )
    print("=" * 78)
    for team_id, new_aliases in sorted(
        proposals.items(), key=lambda x: id_to_team[x[0]]["canonical_name"]
    ):
        t = id_to_team[team_id]
        for a in sorted(new_aliases):
            print(f"  [{t['league']:<11}] {t['canonical_name']:<26}  <-  {a}")

    print()
    print("=" * 78)
    print(f"UNMATCHED  ({len(unmatched)} names) — need manual alias or team entry")
    print("=" * 78)
    for lg_hint, name, freq in sorted(unmatched, key=lambda x: -x[2]):
        print(f"  [{lg_hint:<11}] {freq:3d}×  {name}")

    if not args.apply:
        print(
            "\n(dry run — no changes written. Re-run with --apply to persist proposed auto-matches.)"
        )
        return 0

    # Persist proposed auto-matches only. Unmatched stays for later manual action.
    print("\nApplying auto-matches...")
    updated = 0
    for team_id, new_aliases in proposals.items():
        current = set(id_to_team[team_id]["aliases"] or [])
        merged = sorted(current | new_aliases)
        if merged != sorted(current):
            sb.table("teams").update({"aliases": merged, "updated_at": "now()"}).eq(
                "id", team_id
            ).execute()
            updated += 1
    print(f"Updated {updated} team rows with new aliases.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
