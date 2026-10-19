
def parse_message(message):
    message = str(message).strip()

    # TradingView VWAP partial take profit
    partial = re.fullmatch(
        r"PARTIAL\s*\|\s*([A-Za-z0-9:._/\-]+)\s*\|\s*(20(?:\.0+)?)",
        message,
        re.IGNORECASE
    )

    if partial:
        return (
            "VWAP_PARTIAL",
            normalize_symbol(partial.group(1))
        )

    # TradingView full close
    close = re.fullmatch(
        r"CLOSE\s*\|\s*([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if close:
        return (
            "VWAP_CLOSE",
            normalize_symbol(close.group(1))
        )

    # Explicit VWAP messages
    vwap = re.fullmatch(
        r"VWAP\|(BUY|SELL|TP[1-5]|CLOSE)"
        r"\|([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if vwap:
        return (
            "VWAP_" + vwap.group(1).upper(),
            normalize_symbol(vwap.group(2))
        )

    # Existing BUY/SELL alerts
    legacy = re.fullmatch(
        r"(BUY|SELL)\s*\|\s*"
        r"([A-Za-z0-9:._/\-]+)"
        r"(?:\|[^\r\n]*)?",
        message,
        re.IGNORECASE
    )

    if legacy:
        return (
            legacy.group(1).upper(),
            normalize_symbol(legacy.group(2))
        )

    # Existing DH order-fill messages
    dh = re.search(
        r"\border\s+(buy|sell)\s+@\s+"
        r"[^\r\n]*?\bfilled\s+on\s+"
        r"([A-Za-z0-9:._/\-]+)",
        message,
        re.IGNORECASE
    )

    if dh:
        return (
            dh.group(1).upper(),
            normalize_symbol(dh.group(2))
        )

    raise ValueError(
        "Unsupported TradingView alert: " + message
    )
