
# ============================================================
# TRADINGVIEW MESSAGE PARSER
# Supports:
# 1. BUY|BTCUSDT
# 2. SELL|ETHUSDT
# 3. Original DH Strategy order-fill messages
# 4. JSON TradingView messages
# ============================================================

def parse_tradingview_message(raw):
    raw = str(raw or "").strip()

    if not raw:
        raise ValueError("Empty TradingView message")

    # Existing MAQ format:
    # BUY|BTCUSDT
    # SELL|ETHUSDT
    old = re.fullmatch(
        r"\s*(BUY|SELL)\s*\|\s*([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?\s*",
        raw,
        re.IGNORECASE
    )

    if old:
        signal = old.group(1).upper()
        symbol = normalize_symbol(old.group(2))
        return signal, symbol

    # Original DH TradingView strategy message:
    #
    # DH: (Strategy) Super SmartSuper Trend (...):
    # order buy @ 1000 filled on BINANCE:BTCUSDT.P.
    # New strategy position is 1234

    dh = re.search(
        r"\border\s+(buy|sell)\s+@\s+"
        r"[^\r\n]*?\bfilled\s+on\s+"
        r"([A-Za-z0-9:._/\-]+)",
        raw,
        re.IGNORECASE
    )

    if dh:
        signal = dh.group(1).upper()
        symbol = normalize_symbol(
            dh.group(2).rstrip(".")
        )
        return signal, symbol

    raise ValueError(
        "Unsupported TradingView message format"
    )


def parse_tradingview_request(req):

    if req.is_json:
        data = req.get_json(silent=True)

        if isinstance(data, dict):

            signal = (
                data.get("signal")
                or data.get("action")
            )

            symbol = (
                data.get("symbol")
                or data.get("ticker")
            )

            if signal and symbol:
                signal = str(signal).strip().upper()

                if signal not in ("BUY", "SELL"):
                    raise ValueError(
                        "JSON signal must be BUY or SELL"
                    )

                return signal, normalize_symbol(symbol)

            # JSON containing DH alert text
            for field in ("message", "text", "alert"):
                if isinstance(data.get(field), str):
                    return parse_tradingview_message(
                        data[field]
                    )

        elif isinstance(data, str):
            return parse_tradingview_message(data)

    return parse_tradingview_message(
        req.get_data(as_text=True)
    )


@app.route("/webhook", methods=["POST"])
def tradingview_webhook():

    try:
        signal, symbol = parse_tradingview_request(
            request
        )

        log.info(
            "TradingView signal parsed: %s | %s",
            signal,
            symbol
        )

        result = process_entry_signal(
            signal,
            symbol
        )

        return jsonify(result), 200

    except ValueError as exc:

        log.warning(
            "Rejected TradingView webhook: %s",
            exc
        )

        return jsonify({
            "status": "error",
            "error": str(exc)
        }), 400

    except Exception as exc:

        log.exception(
            "TradingView webhook failed"
        )

        status_code = (
            429 if is_binance_rate_limit_error(exc)
            else 500
        )

        return jsonify({
            "status": "error",
            "error": str(exc)
        }), status_code
