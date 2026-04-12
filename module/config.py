from zoneinfo import ZoneInfo
import re

# Zeitzone
TIMEZONE = ZoneInfo("Europe/Zurich")

# ANSI-Regex
ANSI_ESCAPE_PATTERN = r"\x1b\[[0-9;]*m"
ANSI_ESCAPE = re.compile(ANSI_ESCAPE_PATTERN)

# Standard-Tokenlisten
DEFAULT_SPOT_TOKENS = ("BTC", "BNB", "XRP", "ETH", "ZEC")
DEFAULT_ALPHA_TOKENS = ("ARIA", "RIVER", "SIREN")

# WebSocket-Reconnect
WS_RECONNECT_DELAY = 3
WS_MAX_DELAY = 60

# Logging
LOGFILE = "binance_ticker.log"
LOGLEVEL = "INFO"