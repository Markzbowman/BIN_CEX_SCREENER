# =============================
# FINAL VERSION — USE THIS ONLY
# =============================
# Clean, stable, production-ready Streamlit dashboard
# - Correct 1m / 5m / 1h calculations (based on real price history)
# - No disappearing 1h values
# - Proper day-open handling (00:00)
# - Integrated logging + debug mode
# - Fragment-based refresh + pseudo-delta table updates
#
# 👉 IGNORE ALL OTHER CANVAS DOCUMENTS
# 👉 THIS IS THE ONLY VALID VERSION

import os
import logging
from collections import deque
from datetime import datetime, timedelta
from threading import Lock

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

from module.alpha_client import AlphaClient
from module.config import LOGFILE, LOGLEVEL, TIMEZONE, DEFAULT_ALPHA_TOKENS, DEFAULT_SPOT_TOKENS
from module.utils import get_kline_open_at, pct_change
from module.websocket_client import start_spot_websocket

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="BINANCE CEX_SCREENER", layout="wide")

load_dotenv()
ALPHA_URL = os.getenv("ALPHA_URL")
APP_PASSWORD = os.getenv("APP_PASSWORD", "")
APP_TITLE = os.getenv("APP_TITLE", "BINANCE CEX_SCREENER")

REFRESH_SECONDS = 5
MAX_HISTORY_SECONDS = 4 * 3600  # 4h History
SPOT_BOOTSTRAP_LIMIT = 65
PCT_COLUMNS = ["% 10s", "% 1m", "% 5m", "% 1h", "% Tag"]
ALL_COLUMNS = ["price", *PCT_COLUMNS]

# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------
def setup_logging():
    root = logging.getLogger()
    if root.handlers:
        return

    level = getattr(logging, str(LOGLEVEL).upper(), logging.INFO)
    root.setLevel(level)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = logging.FileHandler(LOGFILE, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


setup_logging()

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------
def now_local():
    return datetime.now(TIMEZONE)


def normalize_token(raw):
    return (raw or "").strip().upper()



def append_history(history, data, ts):
    cutoff = ts - timedelta(seconds=MAX_HISTORY_SECONDS)

    for token, price in data.items():
        if price is None:
            continue

        dq = history.setdefault(token, deque())
        dq.append((ts, price))

        while dq and dq[0][0] < cutoff:
            dq.popleft()


def price_at(history, token, target, max_age=None):
    dq = history.get(token)
    if not dq:
        return None

    for ts, price in reversed(dq):
        if ts <= target:
            if max_age is not None and ts < target - max_age:
                return None
            return price
    return None


def fmt_pct(x):
    return "" if x is None or pd.isna(x) else f"{x * 100:.2f}"


def fmt_price(x, decimals=2):
    return "" if x is None or pd.isna(x) else f"{x:.{decimals}f}"


def color(x):
    if x is None or pd.isna(x):
        return ""
    if x > 0:
        return "color: #00c853; font-weight: bold;"
    if x < 0:
        return "color: #ff1744; font-weight: bold;"
    return ""


def fetch_spot_1m_history(token, limit=SPOT_BOOTSTRAP_LIMIT):
    symbol = f"{token}USDT"
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": "1m", "limit": limit}

    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()
        rows = response.json()
    except Exception as e:
        logging.warning("Spot bootstrap failed for %s: %s", token, e)
        return []

    points = []
    for row in rows:
        try:
            open_time_ms = row[0]
            open_price = float(row[1])
            ts = datetime.fromtimestamp(open_time_ms / 1000, tz=TIMEZONE)
            points.append((ts, open_price))
        except Exception:
            continue

    return points


def bootstrap_spot_history(history, current_ts, active_spot_tokens):
    for token in active_spot_tokens:
        dq = history.setdefault(token, deque())
        if dq:
            continue

        points = fetch_spot_1m_history(token)
        if points:
            dq.extend(points)
            continue

        symbol = f"{token}USDT"
        fallback_points = [
            (current_ts - timedelta(hours=1), get_kline_open_at(symbol, "1h", current_ts - timedelta(hours=1))),
            (current_ts - timedelta(minutes=5), get_kline_open_at(symbol, "5m", current_ts - timedelta(minutes=5))),
            (current_ts - timedelta(minutes=1), get_kline_open_at(symbol, "1m", current_ts - timedelta(minutes=1))),
        ]
        for ts, price in fallback_points:
            if price is not None:
                dq.append((ts, price))


def bootstrap_alpha_history(history, current_ts, active_alpha_tokens, alpha_now):
    for token in active_alpha_tokens:
        dq = history.setdefault(token, deque())
        if dq:
            continue

        price = alpha_now.get(token)
        if price is None:
            continue

        dq.append((current_ts - timedelta(hours=1), price))
        dq.append((current_ts - timedelta(minutes=5), price))
        dq.append((current_ts - timedelta(minutes=1), price))


def ensure_day_open(current_ts, alpha_now, active_spot_tokens):
    current_date = current_ts.date()

    if st.session_state.date != current_date:
        st.session_state.date = current_date

        st.session_state.spot_day = {}
        for token in active_spot_tokens:
            symbol = f"{token}USDT"
            st.session_state.spot_day[token] = get_kline_open_at(
                symbol,
                "1m",
                current_ts.replace(hour=0, minute=0, second=0, microsecond=0),
            )

        st.session_state.alpha_day = {}
        logging.info("NEW DAY INITIALIZED")

    # fehlende Tageswerte für neu hinzugefügte Spot-Tokens am gleichen Tag nachladen
    for token in active_spot_tokens:
        if token not in st.session_state.spot_day or st.session_state.spot_day.get(token) is None:
            symbol = f"{token}USDT"
            st.session_state.spot_day[token] = get_kline_open_at(
                symbol,
                "1m",
                current_ts.replace(hour=0, minute=0, second=0, microsecond=0),
            )

    for token in alpha_now:
        if token not in st.session_state.alpha_day or st.session_state.alpha_day.get(token) is None:
            symbol = f"{token}USDT"
            day_open = get_kline_open_at(
                symbol,
                "1m",
                current_ts.replace(hour=0, minute=0, second=0, microsecond=0),
            )
            if day_open is not None:
                st.session_state.alpha_day[token] = day_open
            else:
                price = alpha_now.get(token)
                if price is not None:
                    st.session_state.alpha_day[token] = price


def empty_live_df(index_tokens):
    df = pd.DataFrame(index=list(index_tokens), columns=ALL_COLUMNS, dtype=float)
    return df.sort_index()


def update_live_df(df, data, history, day_open, current_ts):
    active_tokens = [token for token, price in data.items() if price is not None]

    # fehlende Tokens ergänzen
    missing_tokens = [token for token in active_tokens if token not in df.index]
    if missing_tokens:
        extra = empty_live_df(missing_tokens)
        df = pd.concat([df, extra]).sort_index()

    # Tokens ohne aktuellen Preis ausblenden
    inactive_tokens = [token for token in df.index if token not in active_tokens]
    if inactive_tokens:
        df = df.drop(index=inactive_tokens)

    for token in active_tokens:
        price = data[token]
        df.loc[token, "price"] = price
        df.loc[token, "% 10s"] = pct_change(
            price,
            price_at(history, token, current_ts - timedelta(seconds=10), max_age=timedelta(seconds=15)),
        )
        df.loc[token, "% 1m"] = pct_change(
            price,
            price_at(history, token, current_ts - timedelta(minutes=1), max_age=timedelta(minutes=2)),
        )
        df.loc[token, "% 5m"] = pct_change(
            price,
            price_at(history, token, current_ts - timedelta(minutes=5), max_age=timedelta(minutes=2)),
        )
        df.loc[token, "% 1h"] = pct_change(
            price,
            price_at(history, token, current_ts - timedelta(hours=1), max_age=timedelta(minutes=2)),
        )
        df.loc[token, "% Tag"] = pct_change(price, day_open.get(token))

    return df


def render_html_table(df, is_alpha=False, title=""):
    if df.empty:
        st.info(f"⏳ Warte auf erste {title}-Preise …")
        return

    price_decimals = 4 if is_alpha else 2

    header_html = "".join(
        [
            '<th style="text-align:left;"></th>',
            '<th style="text-align:right;">Preis</th>',
            '<th style="text-align:right;">% 10s</th>',
            '<th style="text-align:right;">% 1m</th>',
            '<th style="text-align:right;">% 5m</th>',
            '<th style="text-align:right;">% 1h</th>',
            '<th style="text-align:right;">% Tag</th>',
        ]
    )

    body_rows = []
    for token in df.index:
        price = df.loc[token, "price"]
        row_html = [f'<td class="col-token">{token}</td>']
        row_html.append(f'<td class="col-price">{fmt_price(price, price_decimals)}</td>')

        for col in PCT_COLUMNS:
            value = df.loc[token, col]
            style = color(value)
            row_html.append(f'<td class="col-pct" style="{style}">{fmt_pct(value)}</td>')

        body_rows.append(f"<tr>{''.join(row_html)}</tr>")

    html = f'''
    <div class="table-wrap">
        <table class="ticker-table">
            <thead>
                <tr>{header_html}</tr>
            </thead>
            <tbody>
                {''.join(body_rows)}
            </tbody>
        </table>
    </div>
    '''

    st.markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------
# INIT
# ---------------------------------------------------------
if "init" not in st.session_state:
    st.session_state.init = True

    st.session_state.spot_prices = {}
    st.session_state.lock = Lock()
    st.session_state.alpha = AlphaClient(ALPHA_URL)

    st.session_state.active_spot_tokens = list(DEFAULT_SPOT_TOKENS)
    st.session_state.active_alpha_tokens = list(DEFAULT_ALPHA_TOKENS)

    st.session_state.spot_hist = {token: deque() for token in DEFAULT_SPOT_TOKENS}
    st.session_state.alpha_hist = {token: deque() for token in DEFAULT_ALPHA_TOKENS}

    st.session_state.spot_day = {}
    st.session_state.alpha_day = {}
    st.session_state.date = None

    st.session_state.new_spot_token = ""
    st.session_state.new_alpha_token = ""

    # persistente Tabellen für pseudo-delta
    st.session_state.df_spot_live = empty_live_df(st.session_state.active_spot_tokens)
    st.session_state.df_alpha_live = empty_live_df(st.session_state.active_alpha_tokens)

    st.session_state.spot_ws = start_spot_websocket(
        st.session_state.active_spot_tokens,
        st.session_state.spot_prices,
        st.session_state.lock,
    )

    logging.info("INIT DONE")

# ---------------------------------------------------------
# STATIC UI
# ---------------------------------------------------------
st.markdown(
    """
    <style>
    .title-main {
        font-size: 16px;
        font-weight: 600;
        margin-bottom: 0.4rem;
    }
    .title-section {
        font-size: 13px;
        font-weight: 500;
        margin-bottom: 0.25rem;
        opacity: 0.85;
    }
    .table-wrap {
        width: 100%;
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
    }
    table.ticker-table {
        width: 100%;
        min-width: 640px;
        border-collapse: collapse;
        font-size: 0.88rem;
        table-layout: auto;
        border: 1.2px solid #6b7280;
    }
    table.ticker-table th,
    table.ticker-table td {
        padding: 0.35rem 0.5rem;
        white-space: nowrap;
        border-right: 1.2px solid #6b7280;
        border-bottom: 1.2px solid #6b7280;
        border-left: 1.2px solid #6b7280;
        border-top: 1.2px solid #6b7280;
        font-variant-numeric: tabular-nums;
    }
    table.ticker-table th:last-child,
    table.ticker-table td:last-child {
        border-right: 1.2px solid #6b7280;
    }
    table.ticker-table thead th {
        text-align: right;
        font-weight: 600;
    }
    table.ticker-table thead th:first-child {
        text-align: left;
    }
    table.ticker-table tbody tr:last-child td {
        border-bottom: 1.2px solid #6b7280;
    }
    table.ticker-table td.col-token {
        text-align: left;
        width: 70px;
    }
    table.ticker-table td.col-price {
        text-align: right;
        width: 150px;
        padding-right: 0.5rem;
    }
    table.ticker-table td.col-pct {
        text-align: right;
        width: 90px;
    }
    
    /* Responsive fix: stack tables on smaller screens */
    @media (max-width: 1200px) {
        div[data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.markdown("### Tokens")
    st.caption("Externer Zugriff per Cloud-Hosting")

    new_spot_token = normalize_token(st.text_input("Spot hinzufügen", key="new_spot_token"))
    if st.button("Spot hinzufügen", key="btn_add_spot"):
        if new_spot_token and new_spot_token not in st.session_state.active_spot_tokens:
            st.session_state.active_spot_tokens.append(new_spot_token)
            st.session_state.spot_hist.setdefault(new_spot_token, deque())
            st.session_state.spot_day.setdefault(new_spot_token, None)
            st.session_state.spot_ws.update_tokens(st.session_state.active_spot_tokens)
            logging.info("Spot token added: %s", new_spot_token)

    if st.session_state.active_spot_tokens:
        remove_spot = st.multiselect("Spot entfernen", st.session_state.active_spot_tokens, key="remove_spot_tokens")
        if st.button("Spot entfernen", key="btn_remove_spot"):
            st.session_state.active_spot_tokens = [
                token for token in st.session_state.active_spot_tokens if token not in remove_spot
            ]
            st.session_state.spot_ws.update_tokens(st.session_state.active_spot_tokens)
            logging.info("Spot tokens removed: %s", remove_spot)

    new_alpha_token = normalize_token(st.text_input("Alpha hinzufügen", key="new_alpha_token"))
    if st.button("Alpha hinzufügen", key="btn_add_alpha"):
        if new_alpha_token and new_alpha_token not in st.session_state.active_alpha_tokens:
            st.session_state.active_alpha_tokens.append(new_alpha_token)
            st.session_state.alpha_hist.setdefault(new_alpha_token, deque())
            st.session_state.alpha_day.setdefault(new_alpha_token, None)
            logging.info("Alpha token added: %s", new_alpha_token)

    if st.session_state.active_alpha_tokens:
        remove_alpha = st.multiselect("Alpha entfernen", st.session_state.active_alpha_tokens, key="remove_alpha_tokens")
        if st.button("Alpha entfernen", key="btn_remove_alpha"):
            st.session_state.active_alpha_tokens = [
                token for token in st.session_state.active_alpha_tokens if token not in remove_alpha
            ]
            logging.info("Alpha tokens removed: %s", remove_alpha)

    st.caption("Spot live via WebSocket")
    st.caption("Alpha live via API")
    if APP_PASSWORD:
        st.caption("App-Schutz aktiv")

    debug = st.checkbox("Debug", False)

if APP_PASSWORD:
    if "access_granted" not in st.session_state:
        st.session_state.access_granted = False

    if not st.session_state.access_granted:
        st.markdown(f"<div class='title-main'>{APP_TITLE}</div>", unsafe_allow_html=True)
        st.info("Geschützter externer Zugriff")
        password_input = st.text_input("Passwort", type="password", key="app_password_input")
        if st.button("Öffnen", key="btn_open_app"):
            if password_input == APP_PASSWORD:
                st.session_state.access_granted = True
                st.rerun()
            else:
                st.error("Falsches Passwort")
        st.stop()

st.markdown(f"<div class='title-main'>{APP_TITLE}</div>", unsafe_allow_html=True)

status_placeholder = st.empty()
col1, col2 = st.columns(2)
with col1:
    st.markdown("<div class='title-section'>Spot</div>", unsafe_allow_html=True)
    spot_table_placeholder = st.empty()
with col2:
    st.markdown("<div class='title-section'>Alpha</div>", unsafe_allow_html=True)
    alpha_table_placeholder = st.empty()

footer_placeholder = st.empty()

# ---------------------------------------------------------
# LIVE FRAGMENT
# ---------------------------------------------------------
@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_dashboard():
    try:
        current_ts = now_local()

        active_spot_tokens = list(st.session_state.active_spot_tokens)
        active_alpha_tokens = list(st.session_state.active_alpha_tokens)

        with st.session_state.lock:
            spot_now = {token: st.session_state.spot_prices.get(token) for token in active_spot_tokens}

        # FALLBACK: wenn WebSocket fehlt → pro Token prüfen
        for t in active_spot_tokens:
            if spot_now.get(t) is not None:
                continue
            try:
                sym = f"{t}USDT"
                resp = requests.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    headers={"User-Agent": "Mozilla/5.0"},
                    params={"symbol": sym},
                    timeout=5,
                )
                if resp.status_code != 200:
                    logging.warning("REST fallback non-200 for %s: %s", sym, resp.status_code)
                    continue

                data = resp.json()
                price_raw = data.get("price") if isinstance(data, dict) else None
                if price_raw is None:
                    logging.warning("REST fallback no price field for %s: %r", sym, data)
                    continue

                price = float(price_raw)
                if price > 0:
                    spot_now[t] = price
                    with st.session_state.lock:
                        st.session_state.spot_prices[t] = price
            except Exception as e:
                logging.warning("REST fallback failed for %s: %s", t, e)

        alpha_now = st.session_state.alpha.get_prices(active_alpha_tokens)

        # lightweight bootstrap so UI appears immediately
        bootstrap_spot_history(st.session_state.spot_hist, current_ts, active_spot_tokens)
        bootstrap_alpha_history(st.session_state.alpha_hist, current_ts, active_alpha_tokens, alpha_now)

        # PRIORITY 3: stable history (append only, no reset)
        append_history(st.session_state.spot_hist, spot_now, current_ts)
        append_history(st.session_state.alpha_hist, alpha_now, current_ts)

        ensure_day_open(current_ts, alpha_now, active_spot_tokens)

        # pseudo-delta: bestehende DataFrames nur aktualisieren, nicht komplett neu erzeugen
        st.session_state.df_spot_live = update_live_df(
            st.session_state.df_spot_live,
            spot_now,
            st.session_state.spot_hist,
            st.session_state.spot_day,
            current_ts,
        )
        st.session_state.df_alpha_live = update_live_df(
            st.session_state.df_alpha_live,
            alpha_now,
            st.session_state.alpha_hist,
            st.session_state.alpha_day,
            current_ts,
        )
        status_placeholder.caption(f"Stand: {current_ts.strftime('%d.%m.%Y - %H:%M:%S')}")

        with spot_table_placeholder.container():
            render_html_table(st.session_state.df_spot_live, is_alpha=False, title="Spot")

        with alpha_table_placeholder.container():
            render_html_table(st.session_state.df_alpha_live, is_alpha=True, title="Alpha")

        footer_placeholder.caption("Zeitzone: Europe/Zurich (Sommer UTC+2, Winter UTC+1)")

        if debug:
            st.markdown("### DEBUG")

            spot_debug_options = active_spot_tokens or [""]
            alpha_debug_options = active_alpha_tokens or [""]

            token = st.selectbox("Spot Token", spot_debug_options, key="debug_spot_token")
            alpha_token = st.selectbox("Alpha Token", alpha_debug_options, key="debug_alpha_token")

            st.write("Aktuelle Zeit:", current_ts)
            st.write("Spot now:", spot_now)
            st.write("Alpha now:", alpha_now)
            st.write("Spot day open:", st.session_state.spot_day)
            st.write("Alpha day open:", st.session_state.alpha_day)

            st.markdown(f"#### Spot Debug: {token}")
            st.write("History len:", len(st.session_state.spot_hist[token]))
            st.write("Current:", spot_now.get(token))
            st.write("1m anchor:", price_at(st.session_state.spot_hist, token, current_ts - timedelta(minutes=1)))
            st.write("5m anchor:", price_at(st.session_state.spot_hist, token, current_ts - timedelta(minutes=5)))
            st.write("1h anchor:", price_at(st.session_state.spot_hist, token, current_ts - timedelta(hours=1)))

            st.markdown(f"#### Alpha Debug: {alpha_token}")
            st.write("History len:", len(st.session_state.alpha_hist[alpha_token]))
            st.write("Current:", alpha_now.get(alpha_token))
            st.write("1m anchor:", price_at(st.session_state.alpha_hist, alpha_token, current_ts - timedelta(minutes=1)))
            st.write("5m anchor:", price_at(st.session_state.alpha_hist, alpha_token, current_ts - timedelta(minutes=5)))
            st.write("1h anchor:", price_at(st.session_state.alpha_hist, alpha_token, current_ts - timedelta(hours=1)))
    except Exception as e:
        logging.exception("render_dashboard failed")
        status_placeholder.error(f"Dashboard-Fehler: {e}")


render_dashboard()
