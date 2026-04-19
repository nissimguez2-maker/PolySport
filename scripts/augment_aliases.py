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
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.feeds.polymarket import list_league_events  # noqa: E402

TARGET_LEAGUES = ["epl", "ucl", "uel", "seriea", "laliga", "bundesliga", "ligue1"]

# City / locality / honorific translations observed in Polymarket naming.
# Expand only when a concrete mismatch is observed — never speculatively.
CITY_MAP = {
    "münchen":        "munich",
    "köln":           "cologne",
    "nürnberg":       "nuremberg",
    "hamburger":      "hamburg",          # "Hamburger SV" -> Hamburg
    "lyonnais":       "lyon",
    "rennais":        "rennes",
    "brestois":       "brest",
    "saint-germain":  "sg",                # "Paris Saint-Germain" -> "Paris SG"
    "saint germain":  "sg",
    "balompié":       "",
    "balompie":       "",
    " de fútbol":     "",
    " de futbol":     "",
    " de madrid":     " madrid",
    " de vigo":       " vigo",
    " de barcelona":  "",
    " de lens":       " lens",
    " de marseille":  " marseille",
    "&":              " and ",
}

# Direct alternate-name hints: Polymarket name -> canonical name (both normalised
# downstream by normalise_name). Use sparingly for genuine alias relationships
# that no regex can derive (e.g. Athletic Club = Athletic Bilbao).
CANONICAL_HINTS = {
    "athletic club":         "athletic bilbao",
    # "Rayo Vallecano de Madrid" normalises to "rayo vallecano madrid" because the
    # generic " de madrid" -> " madrid" rule (which Atlético needs) over-reaches here.
    "rayo vallecano madrid": "rayo vallecano",
}

# Strip these club markers from start or end of a name. Patterns run iteratively.
# NOTE: punctuation is stripped BEFORE these run, so never use \. in a pattern.
STRIP_PATTERNS = [
    # prefixes — longest first, and support the "1." form post-punct-strip as "1 "
    r"^1\s+fc\s+", r"^1\s+fsv\s+", r"^1\s+",
    r"^racing\s+club\s+", r"^club\s+",
    r"^stade\s+(de\s+)?", r"^olympique\s+(de\s+)?",
    r"^fc\s+", r"^cf\s+", r"^sc\s+",
    r"^ac\s+", r"^as\s+", r"^rc\s+", r"^ad\s+", r"^afc\s+", r"^aj\s+",
    r"^bv\s+", r"^sv\s+", r"^tsg\s+", r"^vfb\s+", r"^vfl\s+",
    r"^rcd\s+", r"^ud\s+", r"^ca\s+", r"^cd\s+", r"^ogc\s+",
    # leading year (e.g. "1899 Hoffenheim" after TSG stripped)
    r"^\d{2,4}\s+",
    # suffixes
    r"\s+fc$", r"\s+cf$", r"\s+sc$", r"\s+ac$", r"\s+afc$", r"\s+ud$",
    r"\s+sv$", r"\s+sco$", r"\s+osc$", r"\s+ogc$", r"\s+alsace$",
    # trailing year (e.g. "FC Heidenheim 1846")
    r"\s+\d{2,4}$",
]

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


def strip_diacritics(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def normalise_name(s: str) -> str:
    """Deterministic canonicalisation used for matching ONLY."""
    s = s.lower().strip()
    # City/locality normalisation BEFORE diacritic stripping so map keys match.
    for k, v in CITY_MAP.items():
        s = s.replace(k, v)
    s = strip_diacritics(s)
    # Remove punctuation, keep alphanumerics + spaces.
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Strip mid-string year tokens ("Bayer 04 Leverkusen" -> "Bayer Leverkusen").
    # Replace with a space, not empty, so words stay separated.
    s = re.sub(r"(?<=\s)\d{2,4}(?=\s)", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Iteratively strip club markers.
    changed = True
    while changed:
        changed = False
        for pat in STRIP_PATTERNS:
            new_s = re.sub(pat, "", s, flags=re.IGNORECASE).strip()
            if new_s != s and new_s:
                s = new_s
                changed = True
    s = re.sub(r"\s+", " ", s).strip()
    # Apply direct canonical hints last, once the name is stripped.
    return CANONICAL_HINTS.get(s, s)


def clean_title(title: str) -> str:
    t = TRAILING_NOISE.sub("", title.strip())
    t = LEAGUE_PREFIX.sub("", t)
    return t.strip()


def build_lookup(teams: list[dict]) -> tuple[dict, dict]:
    """Return (exact_alias_map, normalised_alias_map)."""
    exact: dict[str, tuple[str, str, str]] = {}
    norm:  dict[str, list[tuple[str, str, str]]] = defaultdict(list)
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
    parser.add_argument("--apply", action="store_true",
                        help="Write approved aliases to Supabase. Without this flag, prints a dry run only.")
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
    print(f"AUTO-MATCH PROPOSALS  ({sum(len(v) for v in proposals.values())} new aliases across {len(proposals)} teams)")
    print("=" * 78)
    for team_id, new_aliases in sorted(proposals.items(), key=lambda x: id_to_team[x[0]]["canonical_name"]):
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
        print("\n(dry run — no changes written. Re-run with --apply to persist proposed auto-matches.)")
        return 0

    # Persist proposed auto-matches only. Unmatched stays for later manual action.
    print("\nApplying auto-matches...")
    updated = 0
    for team_id, new_aliases in proposals.items():
        current = set(id_to_team[team_id]["aliases"] or [])
        merged = sorted(current | new_aliases)
        if merged != sorted(current):
            sb.table("teams").update({"aliases": merged, "updated_at": "now()"}).eq("id", team_id).execute()
            updated += 1
    print(f"Updated {updated} team rows with new aliases.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
