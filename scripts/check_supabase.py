"""Phase 0 connectivity check: verify Supabase credentials by reading health_check table."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")

    if not url or not key:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env")
        return 1

    client = create_client(url, key)
    response = client.table("health_check").select("*").execute()
    print(f"Connection OK: {response.data}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
