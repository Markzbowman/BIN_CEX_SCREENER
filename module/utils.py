import logging
from datetime import datetime

import requests
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")


def get_kline_open_at(symbol: str, interval: str, dt_local: datetime):
    """
    Holt den Open-Preis der Kline, in die dt_local fällt.
    interval: '1m', '5m', '1h'
    """
    dt_utc = dt_local.astimezone(UTC)

    interval_ms_map = {
        "1m": 60_000,
        "5m": 300_000,
        "1h": 3_600_000,
    }

    if interval not in interval_ms_map:
        raise ValueError(f"Unsupported interval: {interval}")

    interval_ms = interval_ms_map[interval]

    ts_ms = int(dt_utc.timestamp() * 1000)
    open_time_ms = ts_ms - (ts_ms % interval_ms)

    url = (
        f"https://api.binance.com/api/v3/klines?"
        f"symbol={symbol}&interval={interval}&startTime={open_time_ms}&limit=1"
    )

    try:
        resp = requests.get(url, timeout=3)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        return float(data[0][1])
    except Exception as e:
        logging.error("Kline API error (%s, %s): %s", symbol, interval, e)
        return None


def pct_change(current, anchor):
    """
    Prozentänderung relativ zum Anchor.
    Rückgabe als Dezimalwert, z.B. 0.0123 = 1.23%
    """
    if current is None or anchor is None or anchor == 0:
        return None
    return (current - anchor) / anchor