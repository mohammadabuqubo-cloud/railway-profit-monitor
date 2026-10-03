import os
import time
import logging
import threading
import hmac
import hashlib
import json
import urllib.parse
import urllib.request
import urllib.error

from decimal import Decimal, ROUND_DOWN

from flask import Flask, jsonify, request
from binance.client import Client
from binance.enums import SIDE_BUY, SIDE_SELL, ORDER_TYPE_MARKET


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger("MAQ-BINANCE-MANAGER")


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# ENVIRONMENT HELPERS
# ============================================================

def env_float(name, default):
    raw = os.environ.get(name)

    if raw is None or str(raw).strip() == "":
        return float(default)

    return float(raw)


def env_int(name, default):
    raw = os.environ.get(name)

    if raw is None or str(raw).strip() == "":
        return int(default)

    return int(raw)


def env_str(name, default=""):
    raw = os.environ.get(name)

    if raw is None:
        return default

    return str(raw).strip()


# ============================================================
# CONFIGURATION
# ============================================================

BINANCE_API_KEY = env_str("BINANCE_API_KEY")
BINANCE_API_SECRET = env_str("BINANCE_API_SECRET")

USE_TESTNET = (
    env_str(
        "BINANCE_TESTNET",
        "false"
    ).lower() == "true"
)


# ------------------------------------------------------------
# ENTRY
# ------------------------------------------------------------

ORDER_MARGIN_USDT = env_float(
    "ORDER_MARGIN_USDT",
    50
)

ENTRY_LEVERAGE = env_int(
    "ENTRY_LEVERAGE",
    16
)


# ------------------------------------------------------------
# RISK MANAGEMENT
# ------------------------------------------------------------

INITIAL_STOP_LOSS_ROI_PCT = env_float(
    "INITIAL_STOP_LOSS_ROI_PCT",
    20
)

BREAKEVEN_TRIGGER_ROI_PCT = env_float(
    "BREAKEVEN_TRIGGER_ROI_PCT",
    10
)

PARTIAL_TP_ROI_PCT = env_float(
    "PARTIAL_TP_ROI_PCT",
    20
)

PARTIAL_CLOSE_PCT = env_float(
    "PARTIAL_CLOSE_PCT",
    50
)

FINAL_TP_ROI_PCT = env_float(
    "FINAL_TP_ROI_PCT",
    60
)

PORTFOLIO_PROFIT_USD = env_float(
    "PORTFOLIO_PROFIT_USD",
    0
)


# ------------------------------------------------------------
# MONITORING
# ------------------------------------------------------------

POLL_INTERVAL_SECONDS = max(
    30,
    env_int(
        "POLL_INTERVAL_SECONDS",
        30
    )
)


# Newly opened positions will not be SOFTWARE-CLOSED during
# this short grace period.
#
# IMPORTANT:
# Binance protective stop is still armed immediately.
# ------------------------------------------------------------

ENTRY_GRACE_SECONDS = max(
    5,
    env_int(
        "ENTRY_GRACE_SECONDS",
        10
    )
)


EXCHANGE_INFO_CACHE_SECONDS = max(
    3600,
    env_int(
        "EXCHANGE_INFO_CACHE_SECONDS",
        21600
    )
)


# ============================================================
# BINANCE CLIENT
# ============================================================

if not BINANCE_API_KEY or not BINANCE_API_SECRET:

    log.warning(
        "BINANCE_API_KEY or BINANCE_API_SECRET is missing"
    )


client = Client(
    BINANCE_API_KEY,
    BINANCE_API_SECRET,
    testnet=USE_TESTNET
)


BINANCE_FUTURES_BASE = (
    "https://testnet.binancefuture.com"
    if USE_TESTNET
    else "https://fapi.binance.com"
)


# ============================================================
# RUNTIME STATE
# ============================================================

position_states = {}

last_errors = {}

last_snapshot = {
    "last_run": None,
    "positions": [],
    "total_unrealized_profit": 0.0
}


BOT_ALGO_PREFIX = "MAQSL"

entry_lock = threading.Lock()

monitor_start_lock = threading.Lock()

exchange_info_lock = threading.Lock()

monitor_started = False


exchange_info_cache = {
    "loaded_at": 0.0,
    "symbols": {}
}


# ============================================================
# RATE LIMIT HELPER
# ============================================================

def is_binance_rate_limit_error(exc):

    message = str(exc).lower()

    return (
        "-1003" in message
        or "too many requests" in message
        or "request limit" in message
        or "rate limit" in message
    )


# ============================================================
# SIGNED BINANCE ALGO REQUEST
# ============================================================

def binance_algo_request(
    method,
    path,
    params=None
):

    params = dict(
        params or {}
    )

    params["timestamp"] = int(
        time.time() * 1000
    )

    params["recvWindow"] = 5000


    query = urllib.parse.urlencode(
        params
    )


    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


    signed_query = (
        query
        + "&signature="
        + signature
    )


    headers = {
        "X-MBX-APIKEY":
            BINANCE_API_KEY,

        "Content-Type":
            "application/x-www-form-urlencoded"
    }


    method = method.upper()


    if method == "POST":

        url = (
            BINANCE_FUTURES_BASE
            + path
        )

        req = urllib.request.Request(
            url,
            data=signed_query.encode("utf-8"),
            headers=headers,
            method="POST"
        )


    else:

        url = (
            BINANCE_FUTURES_BASE
            + path
            + "?"
            + signed_query
        )

        req = urllib.request.Request(
            url,
            headers=headers,
            method=method
        )


    try:

        with urllib.request.urlopen(
            req,
            timeout=15
        ) as response:

            text = response.read().decode(
                "utf-8"
            )

            if not text:
                return {}

            return json.loads(
                text
            )


    except urllib.error.HTTPError as exc:

        body = exc.read().decode(
            "utf-8",
            errors="replace"
        )

        raise RuntimeError(
            f"Binance Algo HTTP "
            f"{exc.code}: {body}"
        ) from exc


    except Exception as exc:

        raise RuntimeError(
            f"Binance Algo request failed: "
            f"{exc}"
        ) from exc


# ============================================================
# POSITION HELPERS
# ============================================================

def position_key(p):

    return (
        f"{p['symbol']}:"
        f"{p.get('positionSide', 'BOTH')}"
    )


def get_open_positions():

    positions = (
        client.futures_position_information()
    )

    return [
        p
        for p in positions
        if abs(
            float(
                p["positionAmt"]
            )
        ) > 0
    ]


def get_symbol_positions(symbol):

    return (
        client.futures_position_information(
            symbol=symbol
        )
    )


def get_current_position(
    symbol,
    position_side="BOTH"
):

    positions = get_symbol_positions(
        symbol
    )

    for p in positions:

        if (
            p["symbol"] == symbol
            and p.get(
                "positionSide",
                "BOTH"
            ) == position_side
            and abs(
                float(
                    p["positionAmt"]
                )
            ) > 0
        ):

            return p

    return None


def get_any_symbol_position(symbol):

    positions = get_symbol_positions(
        symbol
    )

    for p in positions:

        if abs(
            float(
                p["positionAmt"]
            )
        ) > 0:

            return p

    return None


# ============================================================
# POSITION MARGIN
# ============================================================

def position_margin(p):

    try:

        value = float(
            p.get(
                "positionInitialMargin",
                0
            ) or 0
        )

        if value > 0:
            return value

    except Exception:
        pass


    try:

        isolated = float(
            p.get(
                "isolatedMargin",
                0
            ) or 0
        )

        if isolated > 0:
            return isolated

    except Exception:
        pass


    try:

        notional = abs(
            float(
                p.get(
                    "notional",
                    0
                ) or 0
            )
        )

        leverage = float(
            p.get(
                "leverage",
                1
            ) or 1
        )

        if leverage > 0:

            return (
                notional
                / leverage
            )

    except Exception:
        pass


    return 0.0


# ============================================================
# ROI
# ============================================================

def calculate_roi_pct(p):

    margin = position_margin(
        p
    )

    if margin <= 0:
        return 0.0


    unrealized = float(
        p.get(
            "unRealizedProfit",
            0
        )
        or p.get(
            "unrealizedProfit",
            0
        )
        or 0
    )


    return (
        unrealized
        / margin
    ) * 100.0


# ============================================================
# EXCHANGE INFORMATION
# ============================================================

def refresh_exchange_info_cache(
    force=False
):

    now = time.time()

    age = (
        now
        - exchange_info_cache[
            "loaded_at"
        ]
    )


    if (
        not force
        and exchange_info_cache[
            "symbols"
        ]
        and age
        < EXCHANGE_INFO_CACHE_SECONDS
    ):

        return


    with exchange_info_lock:

        now = time.time()

        age = (
            now
            - exchange_info_cache[
                "loaded_at"
            ]
        )


        if (
            not force
            and exchange_info_cache[
                "symbols"
            ]
            and age
            < EXCHANGE_INFO_CACHE_SECONDS
        ):

            return


        exchange_info = (
            client.futures_exchange_info()
        )


        exchange_info_cache[
            "symbols"
        ] = {

            item["symbol"]:
                item

            for item in exchange_info.get(
                "symbols",
                []
            )
        }


        exchange_info_cache[
            "loaded_at"
        ] = now


        log.info(
            "Exchange info cached: %s symbols",
            len(
                exchange_info_cache[
                    "symbols"
                ]
            )
        )


def get_symbol_info(symbol):

    refresh_exchange_info_cache()

    return exchange_info_cache[
        "symbols"
    ].get(
        symbol
    )


# ============================================================
# QUANTITY / PRICE PRECISION
# ============================================================

def get_quantity_step_size(symbol):

    info = get_symbol_info(
        symbol
    )

    if info:

        for item_filter in info[
            "filters"
        ]:

            if (
                item_filter[
                    "filterType"
                ]
                == "LOT_SIZE"
            ):

                return Decimal(
                    str(
                        item_filter[
                            "stepSize"
                        ]
                    )
                )

    return Decimal(
        "0.001"
    )


def get_tick_size(symbol):

    info = get_symbol_info(
        symbol
    )

    if info:

        for item_filter in info[
            "filters"
        ]:

            if (
                item_filter[
                    "filterType"
                ]
                == "PRICE_FILTER"
            ):

                return Decimal(
                    str(
                        item_filter[
                            "tickSize"
                        ]
                    )
                )

    return Decimal(
        "0.00000001"
    )


def round_quantity_down(
    symbol,
    qty
):

    step = get_quantity_step_size(
        symbol
    )

    value = Decimal(
        str(
            abs(qty)
        )
    )


    rounded = (
        value
        / step
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * step


    return float(
        rounded
    )


def round_price(
    symbol,
    price
):

    tick = get_tick_size(
        symbol
    )

    value = Decimal(
        str(price)
    )


    rounded = (
        value
        / tick
    ).to_integral_value(
        rounding=ROUND_DOWN
    ) * tick


    return format(
        rounded,
        "f"
    )


# ============================================================
# CLOSE POSITION
# ============================================================

def close_position_quantity(
    p,
    quantity,
    reason="UNSPECIFIED"
):

    symbol = p[
        "symbol"
    ]

    position_amt = float(
        p["positionAmt"]
    )


    if position_amt == 0:
        return False


    quantity = round_quantity_down(
        symbol,
        quantity
    )


    if quantity <= 0:
        return False


    close_side = (
        SIDE_SELL
        if position_amt > 0
        else SIDE_BUY
    )


    position_side = p.get(
        "positionSide",
        "BOTH"
    )


    params = {
        "symbol":
            symbol,

        "side":
            close_side,

        "type":
            ORDER_TYPE_MARKET,

        "quantity":
            quantity
    }


    if position_side == "BOTH":

        params[
            "reduceOnly"
        ] = True

    else:

        params[
            "positionSide"
        ] = position_side


    try:

        log.warning(
            "CLOSE REASON=%s | %s | qty=%s | side=%s",
            reason,
            symbol,
            quantity,
            close_side
        )


        result = (
            client.futures_create_order(
                **params
            )
        )


        log.warning(
            "POSITION CLOSED | %s | reason=%s",
            symbol,
            reason
        )


        return result


    except Exception as exc:

        last_errors[
            position_key(p)
        ] = str(exc)


        log.exception(
            "FAILED CLOSE | %s | reason=%s",
            symbol,
            reason
        )


        return False


def close_position(
    p,
    reason="UNSPECIFIED"
):

    return close_position_quantity(
        p,
        abs(
            float(
                p["positionAmt"]
            )
        ),
        reason=reason
    )


# ============================================================
# ALGO STOP MANAGEMENT
# ============================================================

def get_open_algo_orders(symbol):

    result = binance_algo_request(
        "GET",
        "/fapi/v1/openAlgoOrders",
        {
            "symbol":
                symbol,

            "algoType":
                "CONDITIONAL"
        }
    )


    if isinstance(
        result,
        list
    ):

        return result


    return []


def cancel_algo_order(
    symbol,
    algo_id
):

    return binance_algo_request(
        "DELETE",
        "/fapi/v1/algoOrder",
        {
            "symbol":
                symbol,

            "algoId":
                algo_id
        }
    )


def cancel_bot_stops(
    symbol,
    position_side="BOTH"
):

    try:

        orders = get_open_algo_orders(
            symbol
        )


        for order in orders:

            client_id = str(
                order.get(
                    "clientAlgoId",
                    ""
                )
            )


            if not client_id.startswith(
                BOT_ALGO_PREFIX
            ):

                continue


            algo_position_side = (
                order.get(
                    "positionSide",
                    "BOTH"
                )
            )


            if (
                position_side != "BOTH"
                and algo_position_side
                != position_side
            ):

                continue


            algo_id = order.get(
                "algoId"
            )


            if algo_id:

                cancel_algo_order(
                    symbol,
                    algo_id
                )


                log.info(
                    "Cancelled bot stop %s algoId=%s",
                    symbol,
                    algo_id
                )


        return True


    except Exception as exc:

        last_errors[
            f"{symbol}:{position_side}"
        ] = str(exc)


        log.exception(
            "Failed cancelling stop %s",
            symbol
        )


        return False


# ============================================================
# CREATE PROTECTIVE STOP
# ============================================================

def create_protective_stop(
    p,
    stop_price,
    reason
):

    symbol = p[
        "symbol"
    ]

    position_amt = float(
        p["positionAmt"]
    )


    if position_amt == 0:
        return False


    position_side = p.get(
        "positionSide",
        "BOTH"
    )


    close_side = (
        "SELL"
        if position_amt > 0
        else "BUY"
    )


    stop_price = round_price(
        symbol,
        stop_price
    )


    mark_price = float(
        p.get(
            "markPrice",
            0
        ) or 0
    )


    entry_price = float(
        p.get(
            "entryPrice",
            0
        ) or 0
    )


    log.warning(
        "ARM STOP | %s | reason=%s | "
        "entry=%s | mark=%s | trigger=%s | "
        "positionAmt=%s",
        symbol,
        reason,
        entry_price,
        mark_price,
        stop_price,
        position_amt
    )


    # --------------------------------------------------------
    # SAFETY VALIDATION
    #
    # LONG stop must be below current mark price.
    # SHORT stop must be above current mark price.
    # --------------------------------------------------------

    numeric_stop = float(
        stop_price
    )


    if mark_price > 0:

        if (
            position_amt > 0
            and numeric_stop >= mark_price
        ):

            log.error(
                "STOP REJECTED SAFETY | %s | "
                "LONG stop=%s >= mark=%s",
                symbol,
                numeric_stop,
                mark_price
            )

            return False


        if (
            position_amt < 0
            and numeric_stop <= mark_price
        ):

            log.error(
                "STOP REJECTED SAFETY | %s | "
                "SHORT stop=%s <= mark=%s",
                symbol,
                numeric_stop,
                mark_price
            )

            return False


    # Cancel our old stop only after the new stop
    # has passed basic directional validation.
    # --------------------------------------------------------

    cancel_bot_stops(
        symbol,
        position_side
    )


    client_algo_id = (
        f"{BOT_ALGO_PREFIX}_"
        f"{reason[:4]}_"
        f"{int(time.time())}"
    )


    params = {
        "algoType":
            "CONDITIONAL",

        "symbol":
            symbol,

        "side":
            close_side,

        "type":
            "STOP_MARKET",

        "triggerPrice":
            stop_price,

        "workingType":
            "MARK_PRICE",

        "closePosition":
            "true",

        "clientAlgoId":
            client_algo_id
    }


    if position_side != "BOTH":

        params[
            "positionSide"
        ] = position_side


    try:

        response = (
            binance_algo_request(
                "POST",
                "/fapi/v1/algoOrder",
                params
            )
        )


        last_errors.pop(
            position_key(p),
            None
        )


        log.warning(
            "STOP ARMED | %s | %s | trigger=%s",
            symbol,
            reason,
            stop_price
        )


        return response


    except Exception as exc:

        last_errors[
            position_key(p)
        ] = str(exc)


        log.exception(
            "Failed creating protective stop %s",
            symbol
        )


        return False


# ============================================================
# INITIAL STOP PRICE
# ============================================================

def calculate_initial_stop_price(p):

    entry = float(
        p["entryPrice"]
    )


    leverage = float(
        p.get(
            "leverage",
            1
        ) or 1
    )


    position_amt = float(
        p["positionAmt"]
    )


    if leverage <= 0:
        leverage = 1


    adverse_fraction = (
        INITIAL_STOP_LOSS_ROI_PCT
        / 100.0
        / leverage
    )


    if position_amt > 0:

        stop = entry * (
            1.0
            - adverse_fraction
        )

    else:

        stop = entry * (
            1.0
            + adverse_fraction
        )


    log.info(
        "INITIAL STOP CALC | %s | "
        "entry=%s | leverage=%s | "
        "ROI stop=-%s%% | price stop=%s",
        p["symbol"],
        entry,
        leverage,
        INITIAL_STOP_LOSS_ROI_PCT,
        stop
    )


    return stop


def arm_initial_stop_loss(p):

    return create_protective_stop(
        p,
        calculate_initial_stop_price(
            p
        ),
        "INITIAL"
    )


# ============================================================
# BREAKEVEN
# ============================================================

def get_breakeven_price(p):

    # IMPORTANT:
    # Use the actual Binance position ENTRY PRICE.
    # Do not use Binance's separate breakEvenPrice field.
    # --------------------------------------------------------

    entry_price = float(
        p.get(
            "entryPrice",
            0
        ) or 0
    )


    if entry_price <= 0:

        raise ValueError(
            f"Invalid entryPrice for "
            f"{p.get('symbol', 'UNKNOWN')}"
        )


    return entry_price


def move_stop_to_breakeven(p):

    return create_protective_stop(
        p,
        get_breakeven_price(
            p
        ),
        "BREAKEVEN"
    )


# ============================================================
# PARTIAL TAKE PROFIT
# ============================================================

def execute_partial_take_profit(p):

    current_qty = abs(
        float(
            p["positionAmt"]
        )
    )


    qty_to_close = (
        current_qty
        * PARTIAL_CLOSE_PCT
        / 100.0
    )


    result = close_position_quantity(
        p,
        qty_to_close,
        reason="PARTIAL_TP"
    )


    if result:

        log.warning(
            "PARTIAL TP | %s | %.2f%% closed",
            p["symbol"],
            PARTIAL_CLOSE_PCT
        )

        return True


    return False


# ============================================================
# POSITION STATE
# ============================================================

def get_or_create_state(p):

    key = position_key(
        p
    )


    state = position_states.get(
        key
    )


    current_entry = float(
        p["entryPrice"]
    )


    if state is None:

        state = {
            "entry_price":
                current_entry,

            "opened_at":
                time.time(),

            "initial_stop_armed":
                False,

            "breakeven_armed":
                False,

            "partial_tp_done":
                False
        }


        position_states[
            key
        ] = state


        log.info(
            "NEW POSITION STATE | %s | entry=%s",
            key,
            current_entry
        )


    # --------------------------------------------------------
    # Detect a genuinely new position that reused same key.
    # --------------------------------------------------------

    elif abs(
        state.get(
            "entry_price",
            0
        )
        - current_entry
    ) > max(
        abs(current_entry) * 0.000001,
        0.0000000001
    ):

        log.warning(
            "POSITION ENTRY CHANGED | %s | old=%s new=%s",
            key,
            state.get(
                "entry_price"
            ),
            current_entry
        )


        state = {
            "entry_price":
                current_entry,

            "opened_at":
                time.time(),

            "initial_stop_armed":
                False,

            "breakeven_armed":
                False,

            "partial_tp_done":
                False
        }


        position_states[
            key
        ] = state


    return state


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def normalize_symbol(raw_symbol):

    symbol = str(
        raw_symbol
    ).upper().strip()


    if ":" in symbol:

        symbol = symbol.split(
            ":"
        )[-1]


    if symbol.endswith(
        ".P"
    ):

        symbol = symbol[
            :-2
        ]


    symbol = symbol.replace(
        "/",
        ""
    )

    symbol = symbol.replace(
        "-",
        ""
    )

    symbol = symbol.replace(
        "_",
        ""
    )


    return symbol


# ============================================================
# VALIDATE FUTURES SYMBOL
# ============================================================

def validate_futures_symbol(symbol):

    info = get_symbol_info(
        symbol
    )


    if info is None:

        raise ValueError(
            f"{symbol} is not a Binance Futures symbol"
        )


    status = str(
        info.get(
            "status",
            ""
        )
    ).upper()


    if (
        status
        and status != "TRADING"
    ):

        raise ValueError(
            f"{symbol} is not currently trading"
        )


    return info


# ============================================================
# PRICE / LEVERAGE
# ============================================================

def get_futures_price(symbol):

    ticker = (
        client.futures_symbol_ticker(
            symbol=symbol
        )
    )


    return float(
        ticker["price"]
    )


def set_symbol_leverage(symbol):

    result = (
        client.futures_change_leverage(
            symbol=symbol,
            leverage=ENTRY_LEVERAGE
        )
    )


    log.warning(
        "LEVERAGE SET | %s | %sx",
        symbol,
        ENTRY_LEVERAGE
    )


    return result


# ============================================================
# ENTRY QUANTITY
# ============================================================

def calculate_entry_quantity(symbol):

    price = get_futures_price(
        symbol
    )


    if price <= 0:

        raise ValueError(
            f"Invalid market price for {symbol}"
        )


    notional = (
        ORDER_MARGIN_USDT
        * ENTRY_LEVERAGE
    )


    raw_quantity = (
        notional
        / price
    )


    quantity = round_quantity_down(
        symbol,
        raw_quantity
    )


    if quantity <= 0:

        raise ValueError(
            f"Calculated quantity for {symbol} is zero"
        )


    return (
        quantity,
        price,
        notional
    )


# ============================================================
# OPEN MARKET POSITION
# ============================================================

def open_market_position(
    symbol,
    signal
):

    (
        quantity,
        price,
        notional
    ) = calculate_entry_quantity(
        symbol
    )


    side = (
        SIDE_BUY
        if signal == "BUY"
        else SIDE_SELL
    )


    log.warning(
        "OPENING POSITION | %s | %s | "
        "qty=%s | market≈%s | margin=$%.2f | "
        "leverage=%sx | notional≈$%.2f",
        signal,
        symbol,
        quantity,
        price,
        ORDER_MARGIN_USDT,
        ENTRY_LEVERAGE,
        notional
    )


    order = (
        client.futures_create_order(
            symbol=symbol,
            side=side,
            type=ORDER_TYPE_MARKET,
            quantity=quantity
        )
    )


    log.warning(
        "ENTRY EXECUTED | %s | %s | orderId=%s",
        signal,
        symbol,
        order.get(
            "orderId"
        )
    )


    return order


# ============================================================
# WAIT FOR POSITION TO APPEAR
# ============================================================

def wait_for_position(
    symbol,
    timeout_seconds=8
):

    started = time.time()


    while (
        time.time()
        - started
        < timeout_seconds
    ):

        current = get_any_symbol_position(
            symbol
        )


        if current is not None:
            return current


        time.sleep(
            0.25
        )


    return None


# ============================================================
# WAIT FOR POSITION TO CLOSE
# ============================================================

def wait_until_position_closed(
    symbol,
    timeout_seconds=10
):

    started = time.time()


    while (
        time.time()
        - started
        < timeout_seconds
    ):

        current = get_any_symbol_position(
            symbol
        )


        if current is None:
            return True


        time.sleep(
            0.5
        )


    return False


# ============================================================
# PROCESS TRADINGVIEW SIGNAL
#
# BUY:
#   No position       -> LONG
#   Existing LONG     -> IGNORE
#   Existing SHORT    -> CLOSE SHORT + LONG
#
# SELL:
#   No position       -> SHORT
#   Existing SHORT    -> IGNORE
#   Existing LONG     -> CLOSE LONG + SHORT
# ============================================================

def process_entry_signal(
    signal,
    raw_symbol
):

    signal = str(
        signal
    ).upper().strip()


    symbol = normalize_symbol(
        raw_symbol
    )


    if signal not in (
        "BUY",
        "SELL"
    ):

        raise ValueError(
            "Signal must be BUY or SELL"
        )


    validate_futures_symbol(
        symbol
    )


    with entry_lock:

        current = get_any_symbol_position(
            symbol
        )


        # ====================================================
        # EXISTING POSITION
        # ====================================================

        if current:

            amount = float(
                current[
                    "positionAmt"
                ]
            )


            current_direction = (
                "BUY"
                if amount > 0
                else "SELL"
            )


            # ------------------------------------------------
            # SAME DIRECTION = DO NOTHING
            # ------------------------------------------------

            if (
                current_direction
                == signal
            ):

                log.warning(
                    "SIGNAL IGNORED | %s | %s | "
                    "already in same direction",
                    signal,
                    symbol
                )


                return {
                    "status":
                        "ignored",

                    "reason":
                        "same_direction_position_exists",

                    "signal":
                        signal,

                    "symbol":
                        symbol
                }


            # ------------------------------------------------
            # OPPOSITE SIGNAL = REVERSE
            # ------------------------------------------------

            log.warning(
                "OPPOSITE SIGNAL | %s | %s | "
                "current=%s | reversing",
                signal,
                symbol,
                current_direction
            )


            cancel_bot_stops(
                symbol,
                current.get(
                    "positionSide",
                    "BOTH"
                )
            )


            close_result = close_position(
                current,
                reason=f"OPPOSITE_{signal}_SIGNAL"
            )


            if not close_result:

                raise RuntimeError(
                    f"Could not close existing "
                    f"{symbol} position"
                )


            closed = wait_until_position_closed(
                symbol
            )


            if not closed:

                raise RuntimeError(
                    f"{symbol} position did not "
                    f"close in time"
                )


            position_states.pop(
                position_key(
                    current
                ),
                None
            )


        # ====================================================
        # SET LEVERAGE
        # ====================================================

        set_symbol_leverage(
            symbol
        )


        # ====================================================
        # OPEN NEW POSITION
        # ====================================================

        order = open_market_position(
            symbol,
            signal
        )


        # ====================================================
        # WAIT UNTIL BINANCE CONFIRMS POSITION
        # ====================================================

        new_position = wait_for_position(
            symbol
        )


        stop_armed = False


        if new_position:

            state = get_or_create_state(
                new_position
            )


            # Record actual creation time explicitly.
            state[
                "opened_at"
            ] = time.time()


            stop_armed = bool(
                arm_initial_stop_loss(
                    new_position
                )
            )


            if stop_armed:

                state[
                    "initial_stop_armed"
                ] = True


        else:

            log.error(
                "POSITION NOT FOUND AFTER ENTRY | %s",
                symbol
            )


        return {
            "status":
                "executed",

            "signal":
                signal,

            "symbol":
                symbol,

            "margin_usdt":
                ORDER_MARGIN_USDT,

            "leverage":
                ENTRY_LEVERAGE,

            "approx_notional_usdt":
                ORDER_MARGIN_USDT
                * ENTRY_LEVERAGE,

            "orderId":
                order.get(
                    "orderId"
                ),

            "initial_stop_armed":
                stop_armed
        }


# ============================================================
# BACKGROUND SIGNAL PROCESSOR
# ============================================================

def process_signal_background(
    signal,
    symbol
):

    try:

        log.warning(
            "BACKGROUND PROCESS START | %s | %s",
            signal,
            symbol
        )


        result = process_entry_signal(
            signal,
            symbol
        )


        log.warning(
            "BACKGROUND PROCESS COMPLETE | %s",
            result
        )


    except Exception as exc:

        normalized = normalize_symbol(
            symbol
        )


        last_errors[
            f"ENTRY:{normalized}"
        ] = str(exc)


        log.exception(
            "BACKGROUND SIGNAL FAILED | %s | %s",
            signal,
            symbol
        )


# ============================================================
# MAIN RISK MONITOR
# ============================================================

def check_positions():

    global last_snapshot


    open_positions = get_open_positions()


    current_keys = {
        position_key(p)
        for p in open_positions
    }


    # Remove state belonging to positions that no longer exist.
    # --------------------------------------------------------

    for key in list(
        position_states.keys()
    ):

        if key not in current_keys:

            position_states.pop(
                key,
                None
            )


    snapshot_positions = []

    total_unrealized = 0.0


    for p in open_positions:

        symbol = p[
            "symbol"
        ]


        position_side = p.get(
            "positionSide",
            "BOTH"
        )


        key = position_key(
            p
        )


        state = get_or_create_state(
            p
        )


        roi = calculate_roi_pct(
            p
        )


        unrealized = float(
            p.get(
                "unRealizedProfit",
                0
            )
            or p.get(
                "unrealizedProfit",
                0
            )
            or 0
        )


        total_unrealized += unrealized


        opened_at = float(
            state.get(
                "opened_at",
                time.time()
            )
        )


        position_age = (
            time.time()
            - opened_at
        )


        log.info(
            "POSITION | %s | ROI=%.2f%% | "
            "PnL=%.4f | age=%.1fs | "
            "initialSL=%s | BE=%s | partial=%s",
            symbol,
            roi,
            unrealized,
            position_age,
            state[
                "initial_stop_armed"
            ],
            state[
                "breakeven_armed"
            ],
            state[
                "partial_tp_done"
            ]
        )


        # ====================================================
        # HARD SOFTWARE STOP
        #
        # Grace period prevents a just-created position from
        # being immediately software-closed because Binance
        # position fields are still settling.
        #
        # Hardware/Algo protective stop remains active.
        # ====================================================

        if (
            position_age
            >= ENTRY_GRACE_SECONDS
            and roi
            <= -INITIAL_STOP_LOSS_ROI_PCT
        ):

            log.warning(
                "HARD STOP TRIGGER | %s | "
                "ROI %.2f%% <= -%.2f%%",
                symbol,
                roi,
                INITIAL_STOP_LOSS_ROI_PCT
            )


            if close_position(
                p,
                reason="HARD_SOFTWARE_STOP"
            ):

                cancel_bot_stops(
                    symbol,
                    position_side
                )


                position_states.pop(
                    key,
                    None
                )


            continue


        # ====================================================
        # FINAL TAKE PROFIT
        # ====================================================

        if (
            roi
            >= FINAL_TP_ROI_PCT
        ):

            log.warning(
                "FINAL TP TRIGGER | %s | "
                "ROI %.2f%% >= %.2f%%",
                symbol,
                roi,
                FINAL_TP_ROI_PCT
            )


            if close_position(
                p,
                reason="FINAL_TAKE_PROFIT"
            ):

                cancel_bot_stops(
                    symbol,
                    position_side
                )


                position_states.pop(
                    key,
                    None
                )


            continue


        # ====================================================
        # BREAKEVEN
        # ====================================================

        if (
            roi
            >= BREAKEVEN_TRIGGER_ROI_PCT
            and not state[
                "breakeven_armed"
            ]
        ):

            log.warning(
                "BREAKEVEN TRIGGER | %s | ROI %.2f%%",
                symbol,
                roi
            )


            result = move_stop_to_breakeven(
                p
            )


            if result:

                state[
                    "breakeven_armed"
                ] = True

                state[
                    "initial_stop_armed"
                ] = False


        # ====================================================
        # ENSURE INITIAL STOP EXISTS
        # ====================================================

        elif (
            not state[
                "initial_stop_armed"
            ]
            and not state[
                "breakeven_armed"
            ]
        ):

            result = arm_initial_stop_loss(
                p
            )


            if result:

                state[
                    "initial_stop_armed"
                ] = True


        # ====================================================
        # PARTIAL TAKE PROFIT
        # ====================================================

        if (
            roi
            >= PARTIAL_TP_ROI_PCT
            and not state[
                "partial_tp_done"
            ]
        ):

            result = execute_partial_take_profit(
                p
            )


            if result:

                state[
                    "partial_tp_done"
                ] = True


                time.sleep(
                    0.75
                )


                remaining = get_current_position(
                    symbol,
                    position_side
                )


                if remaining:

                    breakeven_result = (
                        move_stop_to_breakeven(
                            remaining
                        )
                    )


                    if breakeven_result:

                        state[
                            "breakeven_armed"
                        ] = True

                        state[
                            "initial_stop_armed"
                        ] = False


        # ====================================================
        # STATUS SNAPSHOT
        # ====================================================

        snapshot_positions.append(
            {
                "symbol":
                    symbol,

                "positionSide":
                    position_side,

                "positionAmt":
                    float(
                        p["positionAmt"]
                    ),

                "entryPrice":
                    float(
                        p["entryPrice"]
                    ),

                "markPrice":
                    float(
                        p.get(
                            "markPrice",
                            0
                        ) or 0
                    ),

                "leverage":
                    float(
                        p.get(
                            "leverage",
                            0
                        ) or 0
                    ),

                "unrealizedProfit":
                    unrealized,

                "roiPct":
                    round(
                        roi,
                        2
                    ),

                "positionAgeSeconds":
                    round(
                        position_age,
                        1
                    ),

                "initialStopArmed":
                    state[
                        "initial_stop_armed"
                    ],

                "breakevenArmed":
                    state[
                        "breakeven_armed"
                    ],

                "partialTpDone":
                    state[
                        "partial_tp_done"
                    ],

                "lastError":
                    last_errors.get(
                        key
                    )
            }
        )


    # ========================================================
    # OPTIONAL PORTFOLIO EXIT
    # ========================================================

    if (
        PORTFOLIO_PROFIT_USD > 0
        and total_unrealized
        >= PORTFOLIO_PROFIT_USD
    ):

        log.warning(
            "PORTFOLIO TARGET | %.2f >= %.2f",
            total_unrealized,
            PORTFOLIO_PROFIT_USD
        )


        for p in open_positions:

            close_position(
                p,
                reason="PORTFOLIO_PROFIT_TARGET"
            )


            cancel_bot_stops(
                p["symbol"],
                p.get(
                    "positionSide",
                    "BOTH"
                )
            )


        position_states.clear()


    last_snapshot = {
        "last_run":
            time.time(),

        "positions":
            snapshot_positions,

        "total_unrealized_profit":
            round(
                total_unrealized,
                8
            )
    }


# ============================================================
# MONITOR LOOP
# ============================================================

def monitor_loop():

    log.warning(
        "================================================"
    )

    log.warning(
        "MAQ BINANCE MANAGER STARTED"
    )

    log.warning(
        "TradingView entries ENABLED"
    )

    log.warning(
        "Margin=$%.2f | leverage=%sx | poll=%ss",
        ORDER_MARGIN_USDT,
        ENTRY_LEVERAGE,
        POLL_INTERVAL_SECONDS
    )

    log.warning(
        "SL=-%.2f%% ROI | BE=+%.2f%% ROI | "
        "Partial=+%.2f%% ROI close %.2f%% | "
        "Final=+%.2f%% ROI",
        INITIAL_STOP_LOSS_ROI_PCT,
        BREAKEVEN_TRIGGER_ROI_PCT,
        PARTIAL_TP_ROI_PCT,
        PARTIAL_CLOSE_PCT,
        FINAL_TP_ROI_PCT
    )

    log.warning(
        "Software entry grace=%ss",
        ENTRY_GRACE_SECONDS
    )

    log.warning(
        "================================================"
    )


    rate_limit_backoff = 60


    while True:

        try:

            check_positions()

            rate_limit_backoff = 60

            time.sleep(
                POLL_INTERVAL_SECONDS
            )


        except Exception as exc:

            log.exception(
                "Monitor iteration failed"
            )


            if is_binance_rate_limit_error(
                exc
            ):

                log.warning(
                    "Binance rate limit reached; "
                    "pausing %s seconds",
                    rate_limit_backoff
                )


                time.sleep(
                    rate_limit_backoff
                )


                rate_limit_backoff = min(
                    rate_limit_backoff * 2,
                    600
                )


            else:

                time.sleep(
                    POLL_INTERVAL_SECONDS
                )


# ============================================================
# HEALTH
# ============================================================

@app.route("/")
def health():

    return jsonify(
        {
            "status":
                "ok",

            "service":
                "MAQ-binance-risk-manager",

            "mode":
                "TRADINGVIEW_BINANCE",

            "webhook_mode":
                "ASYNC_FAST_ACK"
        }
    )


# ============================================================
# STATUS
# ============================================================

@app.route("/status")
def status():

    return jsonify(
        {
            "mode":
                "TRADINGVIEW_BINANCE",

            "webhook_mode":
                "ASYNC_FAST_ACK",

            "entry_config": {

                "order_margin_usdt":
                    ORDER_MARGIN_USDT,

                "entry_leverage":
                    ENTRY_LEVERAGE,

                "approx_notional_usdt":
                    ORDER_MARGIN_USDT
                    * ENTRY_LEVERAGE,

                "entry_grace_seconds":
                    ENTRY_GRACE_SECONDS
            },

            "risk_config": {

                "poll_interval_seconds":
                    POLL_INTERVAL_SECONDS,

                "initial_stop_loss_roi_pct":
                    INITIAL_STOP_LOSS_ROI_PCT,

                "breakeven_trigger_roi_pct":
                    BREAKEVEN_TRIGGER_ROI_PCT,

                "partial_tp_roi_pct":
                    PARTIAL_TP_ROI_PCT,

                "partial_close_pct":
                    PARTIAL_CLOSE_PCT,

                "final_tp_roi_pct":
                    FINAL_TP_ROI_PCT,

                "portfolio_profit_usd":
                    PORTFOLIO_PROFIT_USD
            },

            "position_count":
                len(
                    last_snapshot.get(
                        "positions",
                        []
                    )
                ),

            "position_states":
                position_states,

            "snapshot":
                last_snapshot,

            "errors":
                last_errors
        }
    )


# ============================================================
# TRADINGVIEW WEBHOOK
#
# ACCEPTED:
#
# BUY|BTCUSDT
# SELL|BTCUSDT
#
# BUY|BTCUSDT.P
# SELL|BTCUSDT.P
#
# JSON:
# {"signal":"BUY","symbol":"BTCUSDT"}
#
# IMPORTANT:
# This route DOES NOT wait for Binance.
#
# TradingView gets HTTP 200 immediately.
# Binance processing happens in background.
# ============================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def tradingview_webhook():

    try:

        signal = None
        symbol = None


        # ----------------------------------------------------
        # JSON PAYLOAD
        # ----------------------------------------------------

        if request.is_json:

            data = request.get_json(
                silent=True
            ) or {}


            signal = data.get(
                "signal"
            )

            symbol = data.get(
                "symbol"
            )


        # ----------------------------------------------------
        # TEXT PAYLOAD
        # ----------------------------------------------------

        if not signal or not symbol:

            raw = request.get_data(
                as_text=True
            ).strip()


            log.warning(
                "TradingView raw payload: %s",
                raw[:200]
            )


            if "|" in raw:

                signal, symbol = raw.split(
                    "|",
                    1
                )


        # ----------------------------------------------------
        # VALIDATE BASIC PAYLOAD
        # ----------------------------------------------------

        if not signal or not symbol:

            return jsonify(
                {
                    "status":
                        "error",

                    "error":
                        "Expected BUY|BTCUSDT "
                        "or SELL|BTCUSDT"
                }
            ), 400


        signal = str(
            signal
        ).upper().strip()


        symbol = str(
            symbol
        ).upper().strip()


        if signal not in (
            "BUY",
            "SELL"
        ):

            return jsonify(
                {
                    "status":
                        "error",

                    "error":
                        "Signal must be BUY or SELL"
                }
            ), 400


        normalized_symbol = normalize_symbol(
            symbol
        )


        log.warning(
            "WEBHOOK ACCEPTED | %s | %s -> %s",
            signal,
            symbol,
            normalized_symbol
        )


        # ====================================================
        # PROCESS BINANCE IN BACKGROUND
        # ====================================================

        worker = threading.Thread(
            target=process_signal_background,
            args=(
                signal,
                symbol
            ),
            daemon=True,
            name=f"TV-{signal}-{normalized_symbol}"
        )


        worker.start()


        # ====================================================
        # IMMEDIATE RESPONSE TO TRADINGVIEW
        # ====================================================

        return jsonify(
            {
                "status":
                    "accepted",

                "signal":
                    signal,

                "symbol":
                    normalized_symbol,

                "processing":
                    "background"
            }
        ), 200


    except Exception as exc:

        log.exception(
            "TradingView webhook acceptance failed"
        )


        return jsonify(
            {
                "status":
                    "error",

                "error":
                    str(exc)
            }
        ), 500


# ============================================================
# START MONITOR ONCE
# ============================================================

def start_monitor():

    global monitor_started


    with monitor_start_lock:

        if monitor_started:
            return


        monitor_started = True


        thread = threading.Thread(
            target=monitor_loop,
            daemon=True,
            name="binance-position-monitor"
        )


        thread.start()


start_monitor()


# ============================================================
# LOCAL / RAILWAY START
# ============================================================

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
        threaded=True
    )
