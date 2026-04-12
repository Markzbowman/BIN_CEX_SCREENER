# =========================================================
# FILE: module/websocket_client.py
# =========================================================
import json
import logging
import threading
import time

import websocket

from module.config import WS_MAX_DELAY, WS_RECONNECT_DELAY


class SpotWebSocketManager:
    def __init__(self, spot_prices, lock):
        self.spot_prices = spot_prices
        self.lock = lock
        self.tokens = []
        self.token_set = set()
        self.ws_app = None
        self.thread = None
        self.stop_event = threading.Event()
        self.control_lock = threading.Lock()

    def _build_url(self):
        streams = "/".join(f"{token.lower()}usdt@ticker" for token in self.tokens)
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            payload = data.get("data")
            if not payload:
                return

            symbol = payload.get("s")
            price = payload.get("c")

            if not symbol or price is None:
                return

            if not symbol.endswith("USDT"):
                return

            base = symbol[:-4]
            if base not in self.token_set:
                return

            with self.lock:
                self.spot_prices[base] = float(price)

        except Exception as e:
            logging.error("WebSocket message error: %s", e)

    def _on_error(self, ws, error):
        if not self.stop_event.is_set():
            logging.error("WebSocket error: %s", error)

    def _on_close(self, ws, status_code, close_msg):
        if not self.stop_event.is_set():
            logging.warning("WebSocket closed: code=%s msg=%s", status_code, close_msg)

    def _on_open(self, ws):
        logging.info("WebSocket connected for tokens: %s", self.tokens)

    def _run(self):
        delay = WS_RECONNECT_DELAY

        while not self.stop_event.is_set():
            if not self.tokens:
                time.sleep(0.2)
                continue

            url = self._build_url()
            self.ws_app = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            try:
                self.ws_app.run_forever(ping_interval=20, ping_timeout=10)
                delay = WS_RECONNECT_DELAY
            except Exception as e:
                if not self.stop_event.is_set():
                    logging.error("WebSocket run_forever error: %s", e)

            if self.stop_event.is_set():
                break

            time.sleep(delay)
            delay = min(delay * 2, WS_MAX_DELAY)

    def start(self, tokens):
        with self.control_lock:
            self.tokens = list(dict.fromkeys(tokens))
            self.token_set = set(self.tokens)
            self.stop_event.clear()

            if self.thread and self.thread.is_alive():
                return self

            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            return self

    def stop(self):
        with self.control_lock:
            self.stop_event.set()
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception as e:
                    logging.error("WebSocket close error: %s", e)

    def update_tokens(self, tokens):
        with self.control_lock:
            new_tokens = list(dict.fromkeys(tokens))
            if new_tokens == self.tokens:
                return

            self.tokens = new_tokens
            self.token_set = set(new_tokens)
            logging.info("WebSocket token update: %s", self.tokens)

            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception as e:
                    logging.error("WebSocket close on update error: %s", e)


def start_spot_websocket(tokens, spot_prices, lock):
    manager = SpotWebSocketManager(spot_prices, lock)
    manager.start(tokens)
    return manager

