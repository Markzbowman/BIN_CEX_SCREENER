import logging
from datetime import datetime
from threading import Lock

import requests

from module.config import TIMEZONE


class AlphaClient:
    def __init__(self, url: str, cache_ttl: int = 10):
        self.url = url
        self.cache_ttl = cache_ttl
        self.cache_prices = {}
        self.cache_time = None
        self.lock = Lock()
        self.session = requests.Session()

    def get_prices(self, alpha_tokens):
        now = datetime.now(TIMEZONE)

        if not self.url:
            logging.warning("ALPHA_URL is not set.")
            with self.lock:
                return {t: self.cache_prices.get(t) for t in alpha_tokens}

        with self.lock:
            if self.cache_time and (now - self.cache_time).total_seconds() < self.cache_ttl:
                return {t: self.cache_prices.get(t) for t in alpha_tokens}

        try:
            resp = self.session.get(self.url, timeout=3)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.error("Alpha API error: %s", e)
            with self.lock:
                return {t: self.cache_prices.get(t) for t in alpha_tokens}

        items = data.get("data") if isinstance(data, dict) else data
        if not isinstance(items, list):
            logging.error("Alpha API returned unexpected payload: %r", data)
            with self.lock:
                return {t: self.cache_prices.get(t) for t in alpha_tokens}

        requested = set(alpha_tokens)
        prices = {}

        for item in items:
            if not isinstance(item, dict):
                continue

            sym = item.get("symbol")
            if sym not in requested:
                continue

            try:
                prices[sym] = float(item.get("price"))
            except (TypeError, ValueError) as e:
                logging.warning("Price parse failed for %s: %s", sym, e)

        if prices:
            with self.lock:
                self.cache_prices.update(prices)
                self.cache_time = now

        with self.lock:
            return {t: self.cache_prices.get(t) for t in alpha_tokens}
