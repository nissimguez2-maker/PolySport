"""Deterministic text normalisation shared by the matcher and the alias augmenter.

Both sides must agree on equivalence. If `normalise_name("Bayern München")` differs
between the two, aliases written by the augmenter won't resolve at logger time.
"""

from __future__ import annotations

import re
import unicodedata

# City / locality / honorific translations observed in Polymarket naming.
# Expand only when a concrete mismatch is observed — never speculatively.
CITY_MAP: dict[str, str] = {
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
CANONICAL_HINTS: dict[str, str] = {
    "athletic club":         "athletic bilbao",
    # "Rayo Vallecano de Madrid" normalises to "rayo vallecano madrid" because the
    # generic " de madrid" -> " madrid" rule (which Atlético needs) over-reaches here.
    "rayo vallecano madrid": "rayo vallecano",
}

# Strip these club markers from start or end of a name. Patterns run iteratively.
# NOTE: punctuation is stripped BEFORE these run, so never use \. in a pattern.
STRIP_PATTERNS: list[str] = [
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
