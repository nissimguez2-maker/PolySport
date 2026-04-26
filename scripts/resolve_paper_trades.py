"""CLI wrapper for the paper-trade resolver.

The actual settlement logic lives in polysport/data/resolver.py so the
in-process hourly hook in scripts/phase1_logger.py can share the exact
same code path. This script is the standalone / cron entrypoint.

Run modes:
  python scripts/resolve_paper_trades.py                 # settle up to 50 rows
  python scripts/resolve_paper_trades.py --max-rows 200
  python scripts/resolve_paper_trades.py --dry-run       # compute but don't write
  python scripts/resolve_paper_trades.py --paper-trade-id <uuid>  # one row only
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.data.resolver import resolve_batch


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rows", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--paper-trade-id", type=str, default=None)
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    print(
        f"Resolving paper trades ({'DRY RUN' if args.dry_run else 'LIVE'}, max={args.max_rows})...",
        flush=True,
    )
    with httpx.Client() as http:
        counts = resolve_batch(
            sb,
            http,
            max_rows=args.max_rows,
            dry_run=args.dry_run,
            paper_trade_id=args.paper_trade_id,
        )
    print(
        f"\nDone. settled={counts.settled} unresolved_skip={counts.skipped_unresolved} "
        f"missing={counts.skipped_missing} errors={counts.errors}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
