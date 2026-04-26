"""Team-name resolution: raw feed string → teams.id (UUID).

Design rules (from STRATEGY.md "never guess silently"):
  1. Match only against the teams table (canonical_name + aliases). No fuzzy.
  2. If no match: return None AND log the raw name into unresolved_entities so
     the user can add it to aliases manually.
  3. Matching uses the exact-lowercase form first, then the deterministic
     normalised form (same normaliser as the augmenter, so any alias we added
     there resolves here too).
  4. This module is I/O-light: it loads the alias map ONCE at construction and
     holds it in memory. The poll loop re-creates it on restart. Teams table
     changes take effect on next process restart — acceptable for Phase 1.

The matcher is intentionally separate from the writer. The caller's job:
  resolved = matcher.resolve(raw_name, source="polymarket", league_hint="epl")
  if resolved is None:
      # write row with team_id = NULL; unresolved_entities already updated.
  else:
      # write row with team_id = resolved.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from polysport.utils.text import normalise_name


@dataclass(frozen=True)
class Resolved:
    team_id: str  # UUID as string
    canonical_name: str
    league: str


class SupabaseLike(Protocol):
    """Narrow interface so we can unit-test without hitting Supabase."""

    def table(self, name: str) -> Any: ...


class TeamMatcher:
    """Resolves raw feed names to team UUIDs. Logs misses to unresolved_entities."""

    def __init__(self, sb: SupabaseLike):
        self._sb = sb
        self._exact: dict[str, Resolved] = {}
        self._norm: dict[str, list[Resolved]] = {}
        self._reload()

    def _reload(self) -> None:
        teams = self._sb.table("teams").select("id, canonical_name, league, aliases").execute().data
        exact: dict[str, Resolved] = {}
        norm: dict[str, list[Resolved]] = {}
        for t in teams:
            r = Resolved(team_id=t["id"], canonical_name=t["canonical_name"], league=t["league"])
            for name in [t["canonical_name"]] + (t["aliases"] or []):
                exact[name.strip().lower()] = r
                n = normalise_name(name)
                if n:
                    norm.setdefault(n, []).append(r)
        self._exact, self._norm = exact, norm

    def resolve(
        self,
        raw_name: str,
        *,
        source: str,
        league_hint: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Resolved | None:
        """Return a Resolved or None. On miss, upsert into unresolved_entities."""
        if not raw_name or not raw_name.strip():
            return None

        key = raw_name.strip().lower()
        hit = self._exact.get(key)
        if hit:
            # Optional: enforce league consistency when the hint is supplied.
            # For Phase 1, we log but don't filter — cup competitions cross leagues.
            return hit

        n = normalise_name(raw_name)
        if n:
            candidates = self._norm.get(n, [])
            unique_ids = {r.team_id for r in candidates}
            if len(unique_ids) == 1:
                return candidates[0]
            # Ambiguous normalised match (e.g. two "United" teams) — treat as miss.

        self._log_unresolved(raw_name, source=source, context=context)
        return None

    def _log_unresolved(
        self, raw_name: str, *, source: str, context: dict[str, Any] | None
    ) -> None:
        """Upsert into unresolved_entities, bumping seen_count and last_seen."""
        # Try to update an existing row first; insert if none. The unique constraint
        # is (source, raw_name), so this is atomic enough for our cadence.
        existing = (
            self._sb.table("unresolved_entities")
            .select("id, seen_count")
            .eq("source", source)
            .eq("raw_name", raw_name)
            .is_("resolved_at", "null")
            .execute()
        ).data
        if existing:
            row = existing[0]
            self._sb.table("unresolved_entities").update(
                {
                    "seen_count": (row["seen_count"] or 0) + 1,
                    "last_seen": "now()",
                    "context": context,
                }
            ).eq("id", row["id"]).execute()
        else:
            self._sb.table("unresolved_entities").insert(
                {
                    "source": source,
                    "raw_name": raw_name,
                    "context": context,
                    "seen_count": 1,
                }
            ).execute()
