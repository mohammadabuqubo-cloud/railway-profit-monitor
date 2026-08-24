import os
import time
import logging
import threading
from flask import Flask, jsonify
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("profit-monitor")

app = Flask(__name__)

# ---- Config (set as Railway environment variables) ----
BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")
USE_TESTNET = os.environ.get("BINANCE_TESTNET", "false").lower() == "true"

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "15"))
PER_POSITION_PROFIT_PCT = float(os.environ.get("PER_POSITION_PROFIT_PCT", "20"))   # close that trade at +20% margin ROI
PORTFOLIO_PROFIT_USD = float(os.environ.get("PORTFOLIO_PROFIT_USD", "15"))          # close everything at +$15 combined

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    log.warning("BINANCE_API_KEY / BINANCE_API_SECRET not set — monitor will fail until configured.")

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)

# in-memory snapshot for the /status endpoint
latest_snapshot = {"positions": [], "total_unrealized_profit": 0.0, "last_run": None}


def close_position(symbol, position_amt, reason):
    close_side = SIDE_SELL if position_amt > 0 else SIDE_BUY
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=close_side,
            type=ORDER_TYPE_MARKET,
            quantity=abs(position_amt),
            reduceOnly=True,
        )
        log.info("Closed %s (%s): %s", symbol, reason, order)
        return order
    except Exception:
        log.exception("Failed to close %s", symbol)
        return None


def position_margin(p):
    """Margin used for this position — isolated margin if isolated, else notional/leverage for cross."""
    isolated = p.get("isolated", False) if "isolated" in p else (p.get("marginType") == "isolated")
    leverage = float(p.get("leverage", 1) or 1)
    notional = abs(float(p.get("notional", 0)) or (float(p["positionAmt"]) * float(p["markPrice"])))
    if isolated and float(p.get("isolatedMargin", 0)) > 0:
        return float(p["isolatedMargin"])
    if leverage <= 0:
        leverage = 1
    return abs(notional) / leverage


def check_positions():
    try:
        positions = client.futures_position_information()
    except Exception:
        log.exception("Failed to fetch positions")
        return

    open_positions = [p for p in positions if float(p["positionAmt"]) != 0]
    total_unrealized = 0.0
    snapshot = []

    for p in open_positions:
        symbol = p["symbol"]
        position_amt = float(p["positionAmt"])
        unrealized = float(p["unRealizedProfit"])
        total_unrealized += unrealized

        margin = position_margin(p)
        roi_pct = (unrealized / margin * 100.0) if margin > 0 else 0.0

        snapshot.append({
            "symbol": symbol,
            "positionAmt": position_amt,
            "unrealizedProfit": round(unrealized, 4),
            "roiPct": round(roi_pct, 2),
        })

        # --- Rule 1: per-position profit margin threshold ---
        if roi_pct >= PER_POSITION_PROFIT_PCT:
            log.info("%s hit %.2f%% ROI (threshold %.2f%%) — closing", symbol, roi_pct, PER_POSITION_PROFIT_PCT)
            close_position(symbol, position_amt, f"+{roi_pct:.2f}% margin ROI")

    latest_snapshot["positions"] = snapshot
    latest_snapshot["total_unrealized_profit"] = round(total_unrealized, 4)
    latest_snapshot["last_run"] = time.time()

    # --- Rule 2: portfolio-wide combined profit threshold ---
    if total_unrealized >= PORTFOLIO_PROFIT_USD and open_positions:
        log.info("Portfolio combined profit $%.2f >= $%.2f — closing all positions",
                  total_unrealized, PORTFOLIO_PROFIT_USD)
        for p in open_positions:
            position_amt = float(p["positionAmt"])
            if position_amt != 0:
                close_position(p["symbol"], position_amt, f"portfolio +${total_unrealized:.2f}")


def monitor_loop():
    log.info("Starting monitor loop: per-position %.1f%%, portfolio $%.2f, poll every %ss",
              PER_POSITION_PROFIT_PCT, PORTFOLIO_PROFIT_USD, POLL_INTERVAL_SECONDS)
    while True:
        check_positions()
        time.sleep(POLL_INTERVAL_SECONDS)


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "binance-profit-monitor"}), 200


@app.route("/status", methods=["GET"])
def status():
    return jsonify({
        "config": {
            "per_position_profit_pct": PER_POSITION_PROFIT_PCT,
            "portfolio_profit_usd": PORTFOLIO_PROFIT_USD,
            "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        },
        "snapshot": latest_snapshot,
    }), 200


# start the background polling thread once, on import
_thread = threading.Thread(target=monitor_loop, daemon=True)
_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
