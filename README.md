# Binance Futures Profit Monitor (Railway)

Wundertrading keeps opening your trades exactly as it does now — TradingView
alert → Wundertrading → Binance. This app does NOT open anything. It only
watches your open positions and closes them:

- **Per position:** as soon as that position's profit reaches **20% of the
  margin used** on it, that one position is closed.
- **Portfolio-wide:** if the **combined unrealized profit across all open
  positions** reaches **$15**, everything is closed at once, regardless of
  where each individual trade stands.

It polls Binance every 15 seconds by default (configurable).

## 1. Deploy to Railway

**GitHub route**
1. Push this folder to a new repo.
2. railway.com → New Project → Deploy from GitHub repo → select it.

**CLI route**
```
npm i -g @railway/cli
railway login
cd railway-profit-monitor
railway init
railway up
```

## 2. Set environment variables

Service → **Variables**:

| Key | Value |
|---|---|
| `BINANCE_API_KEY` | Binance Futures API key |
| `BINANCE_API_SECRET` | Binance Futures API secret |
| `BINANCE_TESTNET` | `true` while testing, `false` when live |
| `PER_POSITION_PROFIT_PCT` | `20` |
| `PORTFOLIO_PROFIT_USD` | `15` |
| `POLL_INTERVAL_SECONDS` | `15` (or tighter, e.g. `5`, if you want faster reaction) |

**API key permissions:** enable **Futures** only. No withdrawal permission.
This key only needs read + close (reduce-only order) rights — it never opens
a new position, so it can't be used to increase your exposure even if the
key leaked.

## 3. Watch it work

- `https://your-app.up.railway.app/` → health check.
- `https://your-app.up.railway.app/status` → live view of every open
  position, its current ROI %, total portfolio unrealized profit, and the
  thresholds it's watching for. Good for sanity-checking before you trust it
  unattended.
- Railway → Deployments → **Logs** shows every check and every close action
  it takes, with the reason (which threshold triggered it).

## How the 20% ROI is calculated

For each position: `unrealized profit ÷ margin used on that position × 100`.
Margin used = isolated margin if the position is isolated, or
`notional ÷ leverage` if cross margin. This matches the "profit %" you'd see
on that position in the Binance app, not raw price movement.

## Notes

- Runs as a single worker with a background polling thread — don't scale
  this service to multiple instances, or you'll get duplicate close attempts
  racing each other (harmless since the second one just finds no position
  left, but noisy in the logs).
- Start with `BINANCE_TESTNET=true` and watch `/status` for a while before
  flipping to live funds.
