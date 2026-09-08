import os
import time
import logging
import threading
from decimal import Decimal, ROUND_DOWN, ROUND_UP

from flask import Flask, jsonify
from binance.client import Client
from binance.enums import (
    SIDE_BUY,
    SIDE_SELL,
    ORDER_TYPE_MARKET,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("profit-monitor")

app = Flask(__name__)

# =========================================================
# CONFIG - Railway Environment Variables
# =========================================================

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET")

USE_TESTNET = (
    os.environ.get("BINANCE_TESTNET", "false").lower() == "true"
)

POLL_INTERVAL_SECONDS = int(
    os.environ.get("POLL_INTERVAL_SECONDS", "15")
)

# Close individual position when ROI reaches this level
PER_POSITION_PROFIT_PCT = float(
    os.environ.get("PER_POSITION_PROFIT_PCT", "20")
)

# Close all positions when combined unrealized profit reaches this amount
PORTFOLIO_PROFIT_USD = float(
    os.environ.get("PORTFOLIO_PROFIT_USD", "15")
)

# NEW:
# When position ROI reaches this %, move SL to entry price
BREAKEVEN_TRIGGER_ROI_PCT = float(
    os.environ.get("BREAKEVEN_TRIGGER_ROI_PCT", "10")
)

if not BINANCE_API_KEY or not BINANCE_API_SECRET:
    log.warning(
        "BINANCE_API_KEY / BINANCE_API_SECRET not set — "
        "monitor will fail until configured."
    )

client = Client(
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    testnet=USE_TESTNET,
)

# =========================================================
# STATE
# =========================================================

latest_snapshot = {
    "positions": [],
    "total_unrealized_profit": 0.0,
    "last_run": None,
}

# Keeps track of which currently-open positions already
# had their stop moved to breakeven.
breakeven_armed = {}


# =========================================================
# HELPERS
# =========================================================

def position_key(p):
    """
    Unique key for position.

    Supports normal one-way mode and hedge mode.
    """
    symbol = p["symbol"]
    position_side = p.get("positionSide", "BOTH")

    return f"{symbol}:{position_side}"


def get_tick_size(symbol):
    """
    Get Binance tick size for price rounding.
    """
    try:
        info = client.futures_exchange_info()

        for s in info["symbols"]:
            if s["symbol"] == symbol:
                for f in s["filters"]:
                    if f["filterType"] == "PRICE_FILTER":
                        return float(f["tickSize"])

    except Exception:
        log.exception(
            "Failed to get tick size for %s",
            symbol
        )

    return None


def round_price_to_tick(price, tick_size, side):
    """
    Round stop price to Binance tick size.

    For SELL stop:
    round down.

    For BUY stop:
    round up.
    """
    if not tick_size:
        return price

    price_decimal = Decimal(str(price))
    tick_decimal = Decimal(str(tick_size))

    ticks = price_decimal / tick_decimal

    if side == SIDE_SELL:
        rounded_ticks = ticks.quantize(
            Decimal("1"),
            rounding=ROUND_DOWN
        )
    else:
        rounded_ticks = ticks.quantize(
            Decimal("1"),
            rounding=ROUND_UP
        )

    rounded_price = rounded_ticks * tick_decimal

    return float(rounded_price)


# =========================================================
# CLOSE POSITION
# =========================================================

def close_position(symbol, position_amt, reason, position_side="BOTH"):

    close_side = (
        SIDE_SELL
        if position_amt > 0
        else SIDE_BUY
    )

    try:

        params = {
            "symbol": symbol,
            "side": close_side,
            "type": ORDER_TYPE_MARKET,
            "quantity": abs(position_amt),
        }

        # Normal one-way mode
        if position_side == "BOTH":
            params["reduceOnly"] = True

        # Hedge mode
        else:
            params["positionSide"] = position_side

        order = client.futures_create_order(
            **params
        )

        log.info(
            "Closed %s (%s): %s",
            symbol,
            reason,
            order,
        )

        return order

    except Exception:

        log.exception(
            "Failed to close %s",
            symbol
        )

        return None


# =========================================================
# POSITION MARGIN
# =========================================================

def position_margin(p):
    """
    Margin used for this position.

    Isolated:
    use isolated margin if available.

    Cross:
    approximate margin as notional / leverage.
    """

    isolated = (
        p.get("isolated", False)
        if "isolated" in p
        else p.get("marginType") == "isolated"
    )

    leverage = float(
        p.get("leverage", 1) or 1
    )

    notional = abs(
        float(
            p.get(
                "notional",
                0
            )
        )
        or (
            float(p["positionAmt"])
            * float(p["markPrice"])
        )
    )

    if (
        isolated
        and float(
            p.get(
                "isolatedMargin",
                0
            )
        ) > 0
    ):
        return float(
            p["isolatedMargin"]
        )

    if leverage <= 0:
        leverage = 1

    return (
        abs(notional)
        / leverage
    )


# =========================================================
# CANCEL OLD STOP LOSS
# =========================================================

def cancel_existing_stop_losses(
    symbol,
    close_side,
    position_side="BOTH"
):
    """
    Cancel existing stop-loss orders for this position.

    Does NOT cancel take-profit orders.
    """

    try:

        orders = client.futures_get_open_orders(
            symbol=symbol
        )

        for order in orders:

            order_type = str(
                order.get(
                    "type",
                    ""
                )
            ).upper()

            order_side = order.get("side")

            order_position_side = order.get(
                "positionSide",
                "BOTH"
            )

            reduce_only = bool(
                order.get(
                    "reduceOnly",
                    False
                )
            )

            close_position_flag = bool(
                order.get(
                    "closePosition",
                    False
                )
            )

            is_stop = order_type in [
                "STOP",
                "STOP_MARKET"
            ]

            correct_side = (
                order_side == close_side
            )

            correct_position = (
                position_side == "BOTH"
                or
                order_position_side == position_side
            )

            is_protective = (
                reduce_only
                or close_position_flag
            )

            if (
                is_stop
                and correct_side
                and correct_position
                and is_protective
            ):

                order_id = order["orderId"]

                client.futures_cancel_order(
                    symbol=symbol,
                    orderId=order_id
                )

                log.info(
                    "Cancelled previous stop-loss "
                    "%s orderId=%s",
                    symbol,
                    order_id,
                )

    except Exception:

        log.exception(
            "Failed checking/cancelling "
            "existing stop loss for %s",
            symbol
        )


# =========================================================
# MOVE STOP LOSS TO BREAKEVEN
# =========================================================

def move_stop_to_breakeven(p):

    symbol = p["symbol"]

    position_amt = float(
        p["positionAmt"]
    )

    entry_price = float(
        p["entryPrice"]
    )

    mark_price = float(
        p["markPrice"]
    )

    position_side = p.get(
        "positionSide",
        "BOTH"
    )

    if position_amt == 0:
        return False

    if entry_price <= 0:
        log.warning(
            "%s has invalid entry price: %s",
            symbol,
            entry_price,
        )
        return False

    # LONG position -> SL is SELL
    if position_amt > 0:

        close_side = SIDE_SELL

        # Stop must be below current market
        if mark_price <= entry_price:

            log.warning(
                "%s already returned to/below "
                "breakeven before SL could be armed. "
                "Mark %.8f / Entry %.8f",
                symbol,
                mark_price,
                entry_price,
            )

            return False

    # SHORT position -> SL is BUY
    else:

        close_side = SIDE_BUY

        # Stop must be above current market
        if mark_price >= entry_price:

            log.warning(
                "%s already returned to/above "
                "breakeven before SL could be armed. "
                "Mark %.8f / Entry %.8f",
                symbol,
                mark_price,
                entry_price,
            )

            return False

    tick_size = get_tick_size(
        symbol
    )

    stop_price = round_price_to_tick(
        entry_price,
        tick_size,
        close_side,
    )

    try:

        # Remove previous protective SL
        cancel_existing_stop_losses(
            symbol,
            close_side,
            position_side,
        )

        params = {
            "symbol": symbol,
            "side": close_side,
            "type": "STOP_MARKET",
            "stopPrice": stop_price,
            "quantity": abs(position_amt),
            "workingType": "MARK_PRICE",
        }

        # One-way mode
        if position_side == "BOTH":
            params["reduceOnly"] = True

        # Hedge mode
        else:
            params["positionSide"] = position_side

        order = client.futures_create_order(
            **params
        )

        log.info(
            "BREAKEVEN ARMED %s | "
            "Entry %.8f | "
            "Stop %.8f | "
            "Mark %.8f",
            symbol,
            entry_price,
            stop_price,
            mark_price,
        )

        return True

    except Exception:

        log.exception(
            "Failed moving %s stop "
            "to breakeven",
            symbol
        )

        return False


# =========================================================
# CHECK POSITIONS
# =========================================================

def check_positions():

    try:

        positions = (
            client
            .futures_position_information()
        )

    except Exception:

        log.exception(
            "Failed to fetch positions"
        )

        return

    open_positions = [
        p
        for p in positions
        if float(
            p["positionAmt"]
        ) != 0
    ]

    # Track currently-open position keys
    active_keys = {
        position_key(p)
        for p in open_positions
    }

    # Remove old entries for positions
    # that are no longer open
    for key in list(
        breakeven_armed.keys()
    ):

        if key not in active_keys:

            breakeven_armed.pop(
                key,
                None
            )

    total_unrealized = 0.0

    snapshot = []

    for p in open_positions:

        symbol = p["symbol"]

        position_amt = float(
            p["positionAmt"]
        )

        unrealized = float(
            p["unRealizedProfit"]
        )

        total_unrealized += unrealized

        margin = position_margin(p)

        roi_pct = (
            unrealized
            / margin
            * 100.0
        ) if margin > 0 else 0.0

        key = position_key(p)

        # =================================================
        # RULE 1:
        # BREAKEVEN
        # =================================================

        if (
            roi_pct
            >= BREAKEVEN_TRIGGER_ROI_PCT
            and not breakeven_armed.get(
                key,
                False
            )
        ):

            log.info(
                "%s reached %.2f%% ROI. "
                "Breakeven trigger is %.2f%%.",
                symbol,
                roi_pct,
                BREAKEVEN_TRIGGER_ROI_PCT,
            )

            success = (
                move_stop_to_breakeven(
                    p
                )
            )

            if success:

                breakeven_armed[key] = True

        # =================================================
        # SNAPSHOT
        # =================================================

        snapshot.append({
            "symbol": symbol,
            "positionAmt": position_amt,
            "entryPrice": float(
                p["entryPrice"]
            ),
            "markPrice": float(
                p["markPrice"]
            ),
            "unrealizedProfit": round(
                unrealized,
                4
            ),
            "roiPct": round(
                roi_pct,
                2
            ),
            "breakevenArmed":
                breakeven_armed.get(
                    key,
                    False
                ),
        })

        # =================================================
        # RULE 2:
        # PER POSITION PROFIT
        # =================================================

        if (
            roi_pct
            >= PER_POSITION_PROFIT_PCT
        ):

            log.info(
                "%s hit %.2f%% ROI "
                "(threshold %.2f%%) — closing",
                symbol,
                roi_pct,
                PER_POSITION_PROFIT_PCT,
            )

            close_position(
                symbol,
                position_amt,
                f"+{roi_pct:.2f}% margin ROI",
                p.get(
                    "positionSide",
                    "BOTH"
                ),
            )

    latest_snapshot[
        "positions"
    ] = snapshot

    latest_snapshot[
        "total_unrealized_profit"
    ] = round(
        total_unrealized,
        4
    )

    latest_snapshot[
        "last_run"
    ] = time.time()

    # =====================================================
    # RULE 3:
    # PORTFOLIO COMBINED PROFIT
    # =====================================================

    if (
        total_unrealized
        >= PORTFOLIO_PROFIT_USD
        and open_positions
    ):

        log.info(
            "Portfolio combined profit "
            "$%.2f >= $%.2f — "
            "closing all positions",
            total_unrealized,
            PORTFOLIO_PROFIT_USD,
        )

        for p in open_positions:

            position_amt = float(
                p["positionAmt"]
            )

            if position_amt != 0:

                close_position(
                    p["symbol"],
                    position_amt,
                    (
                        f"portfolio "
                        f"+${total_unrealized:.2f}"
                    ),
                    p.get(
                        "positionSide",
                        "BOTH"
                    ),
                )


# =========================================================
# MONITOR LOOP
# =========================================================

def monitor_loop():

    log.info(
        "Starting monitor | "
        "Breakeven %.1f%% ROI | "
        "Position TP %.1f%% ROI | "
        "Portfolio TP $%.2f | "
        "Poll every %ss",
        BREAKEVEN_TRIGGER_ROI_PCT,
        PER_POSITION_PROFIT_PCT,
        PORTFOLIO_PROFIT_USD,
        POLL_INTERVAL_SECONDS,
    )

    while True:

        try:

            check_positions()

        except Exception:

            log.exception(
                "Unexpected monitor-loop error"
            )

        time.sleep(
            POLL_INTERVAL_SECONDS
        )


# =========================================================
# WEB ENDPOINTS
# =========================================================

@app.route(
    "/",
    methods=["GET"]
)
def health():

    return jsonify({
        "status": "ok",
        "service":
            "binance-profit-monitor",
    }), 200


@app.route(
    "/status",
    methods=["GET"]
)
def status():

    return jsonify({
        "config": {
            "breakeven_trigger_roi_pct":
                BREAKEVEN_TRIGGER_ROI_PCT,
            "per_position_profit_pct":
                PER_POSITION_PROFIT_PCT,
            "portfolio_profit_usd":
                PORTFOLIO_PROFIT_USD,
            "poll_interval_seconds":
                POLL_INTERVAL_SECONDS,
        },
        "snapshot":
            latest_snapshot,
    }), 200


# =========================================================
# START BACKGROUND THREAD
# =========================================================

_thread = threading.Thread(
    target=monitor_loop,
    daemon=True,
)

_thread.start()


# =========================================================
# START FLASK
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            8080
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        )
