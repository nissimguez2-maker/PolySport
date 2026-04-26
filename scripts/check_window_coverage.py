"""Active-window coverage check.

The logger's active window is 14:00–22:30 Asia/Jerusalem (= 11:00–19:30 UTC
during IDT, 12:00–20:30 UTC during IST). For a match kicking off at time K,
the T–180 → kickoff window we care about is [K-180min, K]. The logger only
catches a poll inside that window if it was running at that wall-clock
moment.

Concrete risk: Champions League Tue/Wed knockout fixtures kick at 21:00 UTC
≈ 00:00 IDT — the logger has been inactive for ~90 minutes by kickoff.
Pre-match polls back to T-180 land at 18:00 UTC ≈ 21:00 IDT, which IS
inside the window, so pre-match coverage exists. But halftime (T+45 →
T+60 ≈ 22:30–23:30 IDT) sits entirely outside the window. STRATEGY.md's
halftime track is unreachable for late-evening UCL matches.

This script prints two distributions to verify the assumption against
live data:
  1. commence_time grouped by UTC hour-of-day → which kickoff slots are
     in the dataset?
  2. polled_at grouped by UTC hour-of-day → when is the logger actually
     running?
The intersection is what we cover. The mismatch is what we miss.
"""

from __future__ import annotations

import os
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from polysport.data.snapshots import load_pinnacle_pm_rows


def _parse_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def _bar(n: int, peak: int, width: int = 30) -> str:
    if peak == 0:
        return ""
    return "█" * int(round(width * n / peak))


def main() -> int:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])

    pin_rows, pm_rows, _ = load_pinnacle_pm_rows(sb, days_back=30)
    print(f"Loaded {len(pin_rows)} pinnacle rows, {len(pm_rows)} polymarket rows.\n")

    # 1) Distinct kickoffs by UTC hour.
    seen_kickoffs: set[tuple] = set()
    kickoff_hours: Counter = Counter()
    for r in pin_rows:
        kt = _parse_ts(r["commence_time"])
        if not kt:
            continue
        key = (r["home_team_id"], r["away_team_id"], kt.replace(second=0, microsecond=0))
        if key in seen_kickoffs:
            continue
        seen_kickoffs.add(key)
        kickoff_hours[kt.hour] += 1

    print("=" * 78)
    print("KICKOFF DISTRIBUTION (UTC hour)")
    print(f"  {len(seen_kickoffs)} distinct matches in dataset")
    print("=" * 78)
    if kickoff_hours:
        peak = max(kickoff_hours.values())
        for h in range(24):
            n = kickoff_hours.get(h, 0)
            print(f"  {h:02d}:00 UTC  {n:4d}  {_bar(n, peak)}")

    # 2) Logger uptime by UTC hour (using polled_at on the pinnacle table —
    # paid Pinnacle calls only happen inside the active window).
    poll_hours: Counter = Counter()
    for r in pin_rows:
        pt = _parse_ts(r["polled_at"])
        if not pt:
            continue
        poll_hours[pt.hour] += 1

    print("\n" + "=" * 78)
    print("LOGGER UPTIME (Pinnacle polls per UTC hour, all 30d)")
    print("=" * 78)
    if poll_hours:
        peak = max(poll_hours.values())
        for h in range(24):
            n = poll_hours.get(h, 0)
            print(f"  {h:02d}:00 UTC  {n:5d}  {_bar(n, peak)}")

    # 3) Coverage table — for each kickoff, is there at least one Pinnacle
    # poll inside [K-180, K]? Inside [K, K+60]? (halftime band)
    pin_by_match: dict[tuple, list[datetime]] = {}
    for r in pin_rows:
        kt = _parse_ts(r["commence_time"])
        pt = _parse_ts(r["polled_at"])
        if not (kt and pt):
            continue
        key = (r["home_team_id"], r["away_team_id"], kt.replace(second=0, microsecond=0))
        pin_by_match.setdefault(key, []).append(pt)

    pre_window_covered = 0
    halftime_covered = 0
    pre_window_total = 0
    halftime_total = 0
    for key, polls in pin_by_match.items():
        kickoff = key[2]
        # Pre-match band [K-180, K]
        pre_window_total += 1
        if any(kickoff - timedelta(minutes=180) <= p <= kickoff for p in polls):
            pre_window_covered += 1
        # Halftime band [K+45, K+60]
        halftime_total += 1
        if any(
            kickoff + timedelta(minutes=45) <= p <= kickoff + timedelta(minutes=60) for p in polls
        ):
            halftime_covered += 1

    print("\n" + "=" * 78)
    print("PER-MATCH COVERAGE")
    print("=" * 78)
    print(
        f"  Pre-match (T-180 → T-0):  {pre_window_covered:4d} / {pre_window_total:4d}  "
        f"({100 * pre_window_covered / max(pre_window_total, 1):.1f}%)"
    )
    print(
        f"  Halftime  (T+45 → T+60):  {halftime_covered:4d} / {halftime_total:4d}  "
        f"({100 * halftime_covered / max(halftime_total, 1):.1f}%)"
    )

    # 4) Late-kickoff specific: matches where kickoff_utc >= 19:00 UTC
    late_matches = [k for k in pin_by_match if k[2].hour >= 19]
    if late_matches:
        late_pre = 0
        late_half = 0
        for k in late_matches:
            kickoff = k[2]
            polls = pin_by_match[k]
            if any(kickoff - timedelta(minutes=180) <= p <= kickoff for p in polls):
                late_pre += 1
            if any(
                kickoff + timedelta(minutes=45) <= p <= kickoff + timedelta(minutes=60)
                for p in polls
            ):
                late_half += 1
        print(
            f"\n  Late kickoffs (≥19:00 UTC, n={len(late_matches)}):  "
            f"pre-match {late_pre} ({100 * late_pre / len(late_matches):.0f}%)  "
            f"halftime {late_half} ({100 * late_half / len(late_matches):.0f}%)"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
