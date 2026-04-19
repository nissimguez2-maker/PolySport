# Polymarket production gotchas

Operational constraints that fail silently if missed. Verified 2026-04-19 against
`MrFadiAi/Polymarket-bot` production code.

## 1. USDC.e only — native USDC silently fails

Polymarket's CTF (Conditional Token Framework) contract **only accepts bridged
USDC.e**. Native USDC looks fine in the wallet balance but all contract calls
revert with "insufficient USDC balance".

| Token | Polygon address | CTF compatible |
|---|---|---|
| **USDC.e** (bridged) | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` | ✅ yes |
| Native USDC | `0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359` | ❌ no |

CTF contract (Polygon mainnet): `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`

**Action at funding time:** verify the funded token is USDC.e. If it's native
USDC, swap to USDC.e on-chain before attempting any trade.

## 2. Order minimums — enforce before send, not after reject

Every CLOB order must satisfy **both**:

- `price × size ≥ $1` USDC (notional)
- `size ≥ 5` shares

Sub-threshold orders are rejected with errors like:
- `invalid amount for a marketable BUY order ($X), min size: $1`
- `Size (X) lower than the minimum: 5`

**Impact on our sizing table:**

| Stage bankroll | Stake | Safe at outcome price ≥ | Failure mode below |
|---|---|---|---|
| $100 ($5 stake) | $5 | $0.20 | share-count floor (5 shares × p < $1) |
| $250 ($5 stake) | $5 | $0.20 | same |
| $500 ($10 stake) | $10 | $0.10 | same |
| $1,000+ (2%) | $20+ | $0.05 | rarely relevant |

Moneyline outcomes on the leagues we trade are almost always above $0.20,
so this only bites on extreme underdogs. Still — pre-flight validator is
cheap and must exist before any real order goes out.

## 3. Endpoints (reference)

| Service | URL |
|---|---|
| CLOB (orders + books) | `https://clob.polymarket.com` |
| Gamma (event metadata) | `https://gamma-api.polymarket.com` |

## When to revisit

- Before first live order: re-verify addresses against Polymarket docs
  (contracts migrate occasionally).
- When onboarding a new wallet: confirm USDC.e balance specifically.
- If CLOB returns unexpected rejections: check min-order thresholds first.
