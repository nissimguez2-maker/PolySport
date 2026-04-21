# PolySport — Strategy Spec

> Founding document. This file is the source of truth for what PolySport does and why.
> Written 2026-04-19, locked after spec-design conversation. Do not loosen any threshold
> without calibrated shadow-mode data justifying the change.

---

## Thesis

Polymarket soccer moneylines drift 1–3¢ off Pinnacle's de-vigged fair line. Harvest the
gap with maker-first limit orders in the pre-match and halftime windows. Edge source is
fundamental (Pinnacle as sharp benchmark), not microstructure — latency-tolerant from
Israel.

## Why this beats PolyGuez

| PolyGuez (failed) | PolySport |
|---|---|
| Crypto 5-min binaries — highest fee category (1.80%) | Soccer — 0.75% taker, maker-rebate pilot on Serie A |
| Millisecond signal, 80ms+ Israel latency disadvantage | Minute-to-hour signal, latency-tolerant |
| No fundamental benchmark, just oracle vs strike | Pinnacle as industry-standard sharp benchmark |
| Taker-heavy execution, crossing spread | Maker-first hybrid, spread-preserving |
| Shadow mid-price fills → +$11k sim vs –$218 live | Honest-fill simulator required before live |

## Scope

- **Leagues:** EPL, UCL, Serie A, La Liga, Bundesliga, Ligue 1, Europa League, World Cup
  2026 (Jun 11 – Jul 19). Thin matchups auto-filter via depth gate.
- **Markets Phase 1:** 3-way moneyline only (Home / Draw / Away). Max one leg per match —
  the outcome with the largest absolute divergence.
- **Markets Phase 2:** add Over/Under 2.5 goals after Phase 1 graduates.
- **Skip:** BTTS, correct score, player props, live in-play (Phase 3 reassessment only).

## Timing

- **Pre-match:** T–180min → kickoff. Poll cadence is tiered:
  - **T–180 → T–60min:** 60s interval (coarse; divergence rarely moves fast
    this far out, and we need the Odds API credit headroom).
  - **T–60 → kickoff:** 30s interval (fine; this is the window where
    Pinnacle re-vigs into the close and edges appear / vanish quickly).
- **Halftime:** T+45 → T+60min. Poll every 30s.
- **No live in-play.** Fatal latency disadvantage from Israel.

## Benchmark feed

- **The Odds API Starter**, $30/mo, 20,000 requests/month.
- Pull Pinnacle + Bet365 fair lines per match.
- Staleness threshold: 60s. Skip trade decision if Pinnacle snapshot older than 60s.

## De-vigging

Power method on 3-way moneyline. Given raw implied probabilities `p_i = 1/odds_i` from
Pinnacle, solve for `k` such that `p_home^k + p_draw^k + p_away^k = 1.0` via bisection.
Yields `{fair_home, fair_draw, fair_away}`.

## Entry rule

At each poll cycle, for each active match:

1. Pull Pinnacle odds → compute `fair_i` for i ∈ {home, draw, away}.
2. Pull Polymarket best bid/ask per outcome token → `mid_i = (bid + ask) / 2`.
3. For each outcome, compute `divergence_i = fair_i − mid_i`.
4. Pick outcome with max `|divergence|`.
5. **Fire if ALL of:**
   - `|divergence| ≥ 0.02`
   - `spread_pm ≤ 0.03`
   - `book_depth ≥ $500` at best price
   - `pinnacle_staleness < 60s`
   - `favorite_probability < 0.80` (skip extreme favorites — numerically unstable)
   - no existing position on this match
6. Post GTC `post_only` maker limit, priced 0.5¢ inside the current best, on the underpriced side. Expire at T–5min.

## Exit rule (hold track, primary)

- **Default:** hold to settlement.
- **Hybrid taker fallback:** at T–10min, if unfilled AND divergence still ≥ 2¢ AND Pinnacle
  has not moved toward Polymarket (i.e., the edge wasn't just the feed catching up) → cancel
  maker, FOK taker at best. Otherwise cancel and skip.
- **Early exit:** if Polymarket mid moves ≥5¢ against position AND Pinnacle confirms ≥3¢
  same direction → FOK taker exit.

## Strategy variant: pre-match flip (Phase 2 parallel track)

Runs alongside the hold track on the same entry signal. Whichever track the match qualifies
for first fires; matches never hold both a flip and a hold position simultaneously.

- **Entry:** identical to hold (|div| ≥ 2¢, spread ≤ 3¢, depth ≥ $500, Pinnacle fresh,
  fav prob < 0.80, no existing position).
- **Flip exit target:** immediately after the buy fills, post GTC `post_only` sell at
  `entry_mid + 0.015`. Fill → +1.5¢/share realised, position closed, no settlement risk.
- **Flip time stop:** at T–30min, if sell unfilled, cancel it and inherit hold-track exit rules.
- **Flip stop-loss:** if Polymarket mid moves ≥ 2¢ *away* from fair AND Pinnacle confirms
  ≥ 1¢ same direction → FOK taker exit (tighter than hold: flip is short-horizon, can't wait).

Flip profitability depends on Polymarket mean-reverting toward Pinnacle before kickoff.
`scripts/analyze_reversion.py` measures this directly from Phase 1 data; flip is only
built into Phase 2 shadow if reversion data supports it.

### Flip graduation gate (separate from hold)

| Metric | Threshold |
|---|---|
| Flip shadow trade count | ≥ 250 |
| Flip win rate (Wilson 95% CL lower bound) | ≥ 55% (higher bar — smaller per-trade edge) |
| Flip fill-sim divergence | < 10% (tighter — exit fills dominate PnL) |
| Flip median holding period | ≤ 90 min (sanity check — longer = not a flip) |

## Sizing

| Bankroll | Stake per trade | Cap concurrent positions |
|---|---|---|
| $100 – $499 | $5 flat | 3 |
| $500 – $999 | $10 flat | 3 |
| $1,000 – $4,999 | 2% of bankroll (½-Kelly) | 4 |
| $5,000+ | 2% of bankroll, capped $100 | 5 |

Max $15 at risk at $100 bankroll.

## Kill switches

| Trigger | Action |
|---|---|
| Cumulative drawdown ≥ $50 | Retire — full strategy stop |
| Rolling 50-trade WR < 48% | Pause 7 days, reassess |
| 8 consecutive losses | Pause 7 days |
| Rolling 50-trade realized edge < 1.0% for 3 consecutive windows | Retire — edge decay |
| Maker toxicity rate > 55% over 50 fills | Abandon maker mode, go taker-only |
| Maker toxicity rate 40–55% | Tighten divergence threshold to ≥ 2.5¢ |
| Pinnacle feed staleness > 60s at decision | Skip trade (per-decision, not session-killing) |
| Polymarket geoblock adds Israel | Full halt |

Maker toxicity rate = fraction of maker fills where Pinnacle fair moves > 0.5¢ against
position within 60s post-fill. Random noise baseline ≈ 50%; sustained >55% indicates
informed counterparties picking off stale limits.

## Shadow → live graduation gate

All eight metrics must pass simultaneously before a single USDC touches the live account.

| Metric | Threshold |
|---|---|
| Shadow trade count | ≥ 500 |
| Shadow win rate (Wilson 95% CL lower bound) | ≥ 51% |
| Shadow daily Sharpe | ≥ 0.8 |
| Max consecutive losses in shadow | ≤ 7 |
| Feed uptime last 72h (Odds API + Polymarket CLOB) | ≥ 99.5% |
| Median order-to-decision latency from Israel | ≤ 500ms |
| Shadow-vs-honest-fill-sim PnL divergence | < 15% |
| Settlement prediction match rate | 100% |

**Why n=500 / LB≥51% and not n=250 / LB≥52%** (the earlier draft): at n=250 the
Wilson 95% CL lower bound requires an observed WR of ~58.2% to hit 52%, which
means that at a realistic true WR of 53% the gate passes only ~5% of the time.
That's a "wait for a hot streak" gate, not a "demonstrate edge" gate.
Doubling n and loosening the LB by 1pp preserves the same statistical rigor
while making the gate actually reachable at a realistic edge. At n=500 and
true p=0.53, pass probability rises to a usable band (~25–30%); at true
p=0.54 it's a strong pass (~50%+).

After gate passes: live at $5 flat stakes, first 50 trades. Any cumulative >2σ divergence
from shadow expectation during first 50 live trades → demote back to shadow.

## Stage plan

The earlier draft of this table carried an arithmetic inconsistency: Stage 1
E[PnL] was built from a conservative fill-frequency assumption (~1.3/day), but
the σ column was built from a more aggressive one (~4/day), and the "weeks to
next" column assumed a realistic EV that neither of those produced. Reconciled
around a single, defensible fill-rate assumption below: **~1.5 qualifying
fills/day at Stage 1**, derived from Phase 1 logger match counts × the
probability of any outcome passing the entry gates during its trade window.

At $5 stake, p=0.50, p_win=0.53:
- Per-trade σ = 0.5 × $10 ≈ $4.99
- Daily σ = $4.99 × √1.5 ≈ **$6.10**
- Daily E[PnL] = 1.5 × (0.53 × $5 + 0.47 × −$5) × effective_edge_multiplier ≈ **$0.45**

| Stage | Bankroll | Stake | Max concurrent | Daily E / σ | Weeks to next |
|---|---|---|---|---|---|
| 1 | $100 | $5 flat | 3 | **$0.45 / $6.10** | **52–65 (12–15 months)** |
| 2 | $250 | $5 flat | 3 | ~$0.70 / $6.10 | 26–34 (6–8 months) |
| 3 | $500 | $10 flat | 3 | ~$1.80 / $12.20 | 22–30 |
| 4 | $1,000 | $20 (2%) | 4 | ~$6–10 / $24 | 18–26 |
| 5 | $2,500 | $50 (2%) | 5 | ~$18–28 / $60 | 30–50 |
| 6 | $5,000 | $100 (2%, cap) | 5 | ~$40–60 / $120 — $50/day target band | — |

Stage 1 → 6 honest timeline: **18–28 months**, ~25–35% probability of getting
there at all (revised down from the earlier 40–50% after more rigorous
stage-by-stage ruin accounting; see audit notes). Median outcome is stall at
Stage 3–4. Primary decay risk: Pinnacle CLV has been weakening 2+ years as arb
volume compresses edges.

**Note on Stage 1→2 specifically.** Stage 1 bankroll is $100, Stage 2 is $250,
so you need +$150 from trading. At $0.45/day E that's ~334 trading days of pure
EV. Even with favorable variance the realistic calendar window is 12–15 months,
not the 10–14 weeks the earlier draft claimed. Any estimate shorter than that
is quietly assuming either a higher fill rate (which the Phase 1 data hasn't
justified yet) or a fatter edge per fill (same). Stay honest here — it's the
most common place for retail arb strategies to kid themselves.

## Phase plan

| Phase | What | Gate to next |
|---|---|---|
| 0 — Infra | Create repo, wipe Supabase, open new Claude project | Done when all three exist |
| 1 — 48h sanity check | Standalone script logs Pinnacle + Polymarket divergences across all upcoming EPL/UCL matches for 48h. No trading. | ≥30% of monitored matches touch \|div\| ≥ 2¢ in T–120→0min window |
| 2 — Shadow strategy | Full entry/exit/sizing in shadow for both hold and flip tracks. Honest-fill sim. Maker toxicity tracking. Accumulate 250 trades per track. | Hold: all 8 graduation metrics green. Flip: its own gate (see flip section). |
| 3 — Live $5 stakes | First 50 live trades under strict 1.5σ monitoring | 50 live trades within 1.5σ of shadow expectation |
| 4+ | Stage progression per sizing table | Bankroll thresholds |

## Codebase

- **Fresh repo:** `github.com/nissimguez2-maker/PolySport`. Public.
- **No migration from PolyGuez.** Old repo stays as historical reference only.
- **Reusable patterns to reimplement cleanly:** Supabase logger, Railway deployment,
  py-clob-client wrapper, FastAPI dashboard shell.
- **New modules required:**
  - `polysport/feeds/odds_api.py` — The Odds API client
  - `polysport/feeds/polymarket.py` — Polymarket CLOB + Gamma client
  - `polysport/math/devig.py` — power method 3-way de-vigging
  - `polysport/strategy/moneyline.py` — entry/exit logic (hold track)
  - `polysport/strategy/flip.py` — flip entry/exit logic (parallel track)
  - `polysport/execution/hybrid_maker.py` — post-only + FOK fallback
  - `polysport/sim/honest_fill.py` — honest-fill simulator for shadow mode
  - `polysport/monitoring/toxicity.py` — maker adverse-selection tracker
  - `polysport/monitoring/edge_decay.py` — rolling 50-trade edge monitor
  - `polysport/dashboard/` — FastAPI shell
  - `polysport/cli.py` — runtime entrypoint

## Non-negotiable rules

- Dry-run / shadow mode until graduation gate passes. No exceptions.
- Never loosen divergence threshold, depth gate, spread gate, staleness gate,
  or kill-switch thresholds without calibrated data justifying the change.
- Honest-fill simulator is mandatory before any live deployment — the single most
  likely cause of PolyGuez's shadow-vs-live gap was spread-crossing bias in the
  fill model.
- No live in-play trading. Reassess at Phase 3 only.
- If Polymarket adds Israel to geoblock list: full halt, no VPN workarounds
  (ToS violation, funds freeze risk).
