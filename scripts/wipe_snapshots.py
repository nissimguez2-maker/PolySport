"""Wipe Phase 1 snapshot data for a clean re-run.

Deletes rows from:
  - odds_api_snapshots
  - polymarket_snapshots
  - unresolved_entities

Preserves:
  - teams            (184-row canonical lookup, expensive to rebuild)
  - match_links      (empty anyway in Phase 1)

Refuses to run without --confirm. Prints counts before and after.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

WIPE_TABLES = ("odds_api_snapshots", "polymarket_snapshots", "unresolved_entities")
PRESERVE_TABLES = ("teams", "match_links")


def _count(sb, table: str) -> int:
    return sb.table(table).select("id", count="exact").limit(1).execute().count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required. Without this flag, the script only reports counts.",
    )
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    print("=" * 70)
    print("BEFORE")
    print("=" * 70)
    before = {}
    for t in WIPE_TABLES + PRESERVE_TABLES:
        n = _count(sb, t)
        before[t] = n
        tag = "WIPE   " if t in WIPE_TABLES else "PRESERVE"
        print(f"  {tag}  {t:<24}  rows={n}")

    if not args.confirm:
        print("\n(dry-run) re-run with --confirm to actually delete.")
        return 0

    print("\n" + "=" * 70)
    print("WIPING (batched to dodge statement timeout)")
    print("=" * 70)
    # 200 uuids ≈ 7k chars in the query string — inside most URL limits.
    BATCH = 200
    for t in WIPE_TABLES:
        total_deleted = 0
        while True:
            # Grab a batch of ids, then delete by `in (...)`. Faster than a
            # big `id is not null` scan because the WHERE clause is an indexed
            # pk lookup.
            ids = [r["id"] for r in sb.table(t).select("id").limit(BATCH).execute().data]
            if not ids:
                break
            sb.table(t).delete().in_("id", ids).execute()
            total_deleted += len(ids)
            print(f"    {t}: deleted {total_deleted} so far")
        print(f"  done {t}: {total_deleted} rows")

    print("\n" + "=" * 70)
    print("AFTER")
    print("=" * 70)
    for t in WIPE_TABLES + PRESERVE_TABLES:
        n = _count(sb, t)
        delta = n - before[t]
        print(f"  {t:<24}  rows={n}  delta={delta:+d}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
