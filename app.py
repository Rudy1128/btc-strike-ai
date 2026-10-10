import os, time, json, statistics, threading, math
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, jsonify, render_template_string

try:
    import websocket
except Exception:
    websocket = None

app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 1
MEMORY_FILE = "signal_memory.json"

KALSHI_BASES = [
    os.getenv(
        "KALSHI_BASE_URL",
        "https://external-api.kalshi.com/trade-api/v2"
    ).rstrip("/"),
    "https://api.elections.kalshi.com/trade-api/v2",
]

KALSHI_TICKER = os.getenv(
    "KALSHI_TICKER",
    ""
).strip()

KALSHI_SERIES = "KXBTC15M"

session = requests.Session()
session.headers["User-Agent"] = "BTC-Strike-AI/10.0-Heat"

cache = {
    "time": 0,
    "state": None
}

history_cache = {
    "time": 0,
    "candles": []
}

feed_health = {}
market_history = []
signal_memory = []
active_market = None
memory_loaded = False

# Short-term battle-shift history. This lets the dashboard detect a rapid
# change in pressure instead of waiting for the full 1,000-trade window
# to turn bearish/bullish.
shift_history = []
shift_lock = threading.Lock()
SHIFT_HISTORY_MAX = 30

# Market-control heat history.  This is a short rolling record of the
# balance between executed flow, resting liquidity, momentum, price-vs-strike
# and rapid control shifts.  It is a decision aid, not a guarantee of the
# eventual Kalshi outcome.
heat_history = []
heat_lock = threading.Lock()
HEAT_HISTORY_MAX = 60

# Stateful 15-minute winner-forecast history. Unlike the live signal, this
# engine evaluates the path the market has taken and requires persistence
# before changing its projected settlement direction.
forecast_history = []
forecast_lock = threading.Lock()
FORECAST_HISTORY_MAX = 1800
forecast_state = {
    "ticker": None,
    "direction": "WAIT",
    "probability": 50,
    "locked": False,
    "locked_at": None,
    "last_change": 0.0,
    "reason": "Building trajectory history...",
    "opposite_samples": 0,
}

# =========================================================
# REAL-TIME BINANCE STREAM
# =========================================================

live_btc = {
    "price": None,
    "received_at": 0.0,
    "event_at": 0.0,
    "connected": False,
    "source": "Binance WebSocket",
    "last_error": None,
    "reconnects": 0,
}

live_coinbase = {
    "price": None,
    "received_at": 0.0,
    "event_at": 0.0,
    "connected": False,
    "source": "Coinbase WebSocket",
    "last_error": None,
    "reconnects": 0,
}

STREAM_MAX_AGE = 1.5
active_btc_source = "REST fallback"
kalshi_last_update = 0.0

# Real-time microstructure feed: executed trade flow + best bid/ask sizes.
# This is a market-pressure observer, not an order-placement engine.
MICRO_LOCK = threading.Lock()
MICRO_TRADES = deque(maxlen=5000)   # (received_ts, signed_notional, notional, price)
MICRO_PRICES = deque(maxlen=2000)   # (received_ts, price)
MICRO_BOOK = {
    "bid": None, "bid_qty": None, "ask": None, "ask_qty": None,
    "received_at": 0.0, "connected": False, "last_error": None, "reconnects": 0
}
MICRO_MAX_AGE = 3.0


def _binance_stream_loop():
    """Primary BTC stream with heartbeat and rapid reconnects."""
    if websocket is None:
        return

    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    while True:
        ws = None
        try:
            ws = websocket.create_connection(
                url,
                timeout=5,
                http_proxy_host=None,
                http_proxy_port=None,
                http_no_proxy=["stream.binance.com"],
                suppress_origin=True,
                enable_multithread=True
            )

            live_btc["connected"] = True
            live_btc["last_error"] = None

            while True:
                try:
                    raw = ws.recv()
                    if not raw:
                        raise RuntimeError("Empty Binance stream message")

                    data = json.loads(raw)
                    price = number(data.get("p"))
                    if price is None or price <= 0:
                        continue

                    received = time.time()
                    live_btc["price"] = price
                    live_btc["received_at"] = received
                    live_btc["event_at"] = safe_event_time(data.get("T"))
                    record_feed("Binance Live", price)

                except Exception as recv_exc:
                    timeout_cls = getattr(websocket, "WebSocketTimeoutException", None)
                    if timeout_cls is not None and isinstance(recv_exc, timeout_cls):
                        try:
                            ws.ping("btc-strike-heartbeat")
                            continue
                        except Exception:
                            raise
                    raise

        except Exception as exc:
            live_btc["connected"] = False
            live_btc["last_error"] = str(exc)[:240]
            live_btc["reconnects"] = int(live_btc.get("reconnects", 0)) + 1
            feed_health["Binance Live"] = {
                "online": False,
                "last_error": live_btc["last_error"],
                "last_success": live_btc.get("received_at", 0.0),
                "reconnects": live_btc["reconnects"],
            }
            time.sleep(0.5)

        finally:
            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass


def _coinbase_stream_loop():
    """Secondary public Coinbase stream used as a hot backup."""
    if websocket is None:
        return

    url = "wss://ws-feed.exchange.coinbase.com"

    while True:
        ws = None
        try:
            ws = websocket.create_connection(
                url,
                timeout=5,
                http_proxy_host=None,
                http_proxy_port=None,
                http_no_proxy=["ws-feed.exchange.coinbase.com"],
                suppress_origin=True,
                enable_multithread=True
            )

            ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": ["BTC-USD"],
                "channels": ["ticker"]
            }))

            live_coinbase["connected"] = True
            live_coinbase["last_error"] = None

            while True:
                try:
                    raw = ws.recv()
                    if not raw:
                        raise RuntimeError("Empty Coinbase stream message")

                    data = json.loads(raw)
                    price = number(data.get("price"))
                    if price is None or price <= 0:
                        continue

                    received = time.time()
                    live_coinbase["price"] = price
                    live_coinbase["received_at"] = received
                    live_coinbase["event_at"] = safe_event_time(data.get("time"))
                    record_feed("Coinbase Live", price)

                except Exception as recv_exc:
                    timeout_cls = getattr(websocket, "WebSocketTimeoutException", None)
                    if timeout_cls is not None and isinstance(recv_exc, timeout_cls):
                        try:
                            ws.ping("btc-strike-heartbeat")
                            continue
                        except Exception:
                            raise
                    raise

        except Exception as exc:
            live_coinbase["connected"] = False
            live_coinbase["last_error"] = str(exc)[:240]
            live_coinbase["reconnects"] = int(live_coinbase.get("reconnects", 0)) + 1
            feed_health["Coinbase Live"] = {
                "online": False,
                "last_error": live_coinbase["last_error"],
                "last_success": live_coinbase.get("received_at", 0.0),
                "reconnects": live_coinbase["reconnects"],
            }
            time.sleep(0.75)

        finally:
            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass

def _market_microstructure_stream_loop():
    """Consume Binance trades and best bid/ask updates for a fast pressure read."""
    if websocket is None:
        return

    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@bookTicker"
    while True:
        ws = None
        try:
            ws = websocket.create_connection(
                url, timeout=5, http_proxy_host=None, http_proxy_port=None,
                http_no_proxy=["stream.binance.com"], suppress_origin=True,
                enable_multithread=True
            )
            with MICRO_LOCK:
                MICRO_BOOK["connected"] = True
                MICRO_BOOK["last_error"] = None

            while True:
                raw = ws.recv()
                if not raw:
                    raise RuntimeError("Empty Binance microstructure message")
                packet = json.loads(raw)
                data = packet.get("data", packet)
                event = data.get("e", "")
                received = time.time()

                if event == "trade":
                    price = number(data.get("p"))
                    qty = number(data.get("q"))
                    if price is None or qty is None or price <= 0 or qty <= 0:
                        continue
                    notional = price * qty
                    # m=True means buyer was maker, so the aggressor was a seller.
                    signed = -notional if data.get("m") is True else notional
                    with MICRO_LOCK:
                        MICRO_TRADES.append((received, signed, notional, price))
                        MICRO_PRICES.append((received, price))

                elif event == "bookTicker" or ("b" in data and "a" in data and "B" in data and "A" in data):
                    bid = number(data.get("b"))
                    bid_qty = number(data.get("B"))
                    ask = number(data.get("a"))
                    ask_qty = number(data.get("A"))
                    if (bid is None or ask is None or bid_qty is None or ask_qty is None
                            or bid <= 0 or ask <= 0 or ask < bid):
                        continue
                    with MICRO_LOCK:
                        MICRO_BOOK.update({
                            "bid": bid, "bid_qty": bid_qty, "ask": ask,
                            "ask_qty": ask_qty, "received_at": received,
                            "connected": True, "last_error": None
                        })
        except Exception as exc:
            with MICRO_LOCK:
                MICRO_BOOK["connected"] = False
                MICRO_BOOK["last_error"] = str(exc)[:180]
                MICRO_BOOK["reconnects"] = int(MICRO_BOOK.get("reconnects", 0)) + 1
            time.sleep(0.5)
        finally:
            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass


def build_market_maker_read(now=None):
    """Summarize short-window trade flow and top-of-book pressure.

    Scores are rule-based pressure scores, not calibrated probabilities.
    """
    now = now or time.time()
    with MICRO_LOCK:
        trades = [x for x in MICRO_TRADES if now - x[0] <= 10.0]
        prices = [x for x in MICRO_PRICES if now - x[0] <= 10.0]
        book = dict(MICRO_BOOK)

    age = now - book.get("received_at", 0.0) if book.get("received_at") else None
    if age is not None and age > MICRO_MAX_AGE:
        book_fresh = False
    else:
        book_fresh = bool(book.get("bid") and book.get("ask")) and age is not None

    signed_flow = sum(x[1] for x in trades)
    total_flow = sum(x[2] for x in trades)
    trade_delta_pct = (signed_flow / total_flow * 100.0) if total_flow > 0 else None
    buy_notional = sum(x[2] for x in trades if x[1] > 0)
    sell_notional = sum(x[2] for x in trades if x[1] < 0)
    total_aggressive = buy_notional + sell_notional
    buy_pct = buy_notional / total_aggressive * 100.0 if total_aggressive > 0 else None

    bid_qty = book.get("bid_qty") if book_fresh else None
    ask_qty = book.get("ask_qty") if book_fresh else None
    qty_total = (bid_qty + ask_qty) if bid_qty is not None and ask_qty is not None else 0.0
    book_imbalance = ((bid_qty - ask_qty) / qty_total * 100.0) if qty_total > 0 else None

    bid, ask = book.get("bid"), book.get("ask")
    mid = (bid + ask) / 2.0 if book_fresh else None
    microprice = None
    micro_edge_bps = None
    spread_bps = None
    if book_fresh and qty_total > 0 and mid and mid > 0:
        # Queue-size-weighted midpoint: larger bid size tilts it upward.
        microprice = (ask * bid_qty + bid * ask_qty) / qty_total
        micro_edge_bps = (microprice - mid) / mid * 10000.0
        spread_bps = (ask - bid) / mid * 10000.0

    price_move_bps = None
    if len(prices) >= 2:
        first_price = prices[0][1]
        last_price = prices[-1][1]
        if first_price > 0:
            price_move_bps = (last_price - first_price) / first_price * 10000.0

    up_votes = 0
    down_votes = 0
    evidence = []
    if trade_delta_pct is not None and len(trades) >= 5:
        if trade_delta_pct >= 12:
            up_votes += 1; evidence.append("10s aggressive trade flow favors buyers")
        elif trade_delta_pct <= -12:
            down_votes += 1; evidence.append("10s aggressive trade flow favors sellers")
    if book_imbalance is not None:
        if book_imbalance >= 15:
            up_votes += 1; evidence.append("best-quote size favors bids")
        elif book_imbalance <= -15:
            down_votes += 1; evidence.append("best-quote size favors asks")
    if micro_edge_bps is not None:
        if micro_edge_bps >= 0.08:
            up_votes += 1; evidence.append("microprice tilts upward")
        elif micro_edge_bps <= -0.08:
            down_votes += 1; evidence.append("microprice tilts downward")
    if price_move_bps is not None:
        if price_move_bps >= 0.8:
            up_votes += 1; evidence.append("10s price response is positive")
        elif price_move_bps <= -0.8:
            down_votes += 1; evidence.append("10s price response is negative")

    feed_ready = book_fresh and len(trades) >= 5 and total_flow > 0
    if not feed_ready:
        direction, label, score = "WAIT", "⚪ WAIT — BUILDING LIVE DATA", 0
        reason = "Waiting for a fresh best-bid/ask stream and enough recent trades."
    elif up_votes >= 2 and up_votes > down_votes:
        direction, label = "UP", "🟢 EARLY UP PRESSURE"
        score = min(85, 50 + 9 * (up_votes - down_votes))
        reason = "Multiple short-window inputs lean upward; this is not a settlement prediction."
    elif down_votes >= 2 and down_votes > up_votes:
        direction, label = "DOWN", "🔴 EARLY DOWN PRESSURE"
        score = min(85, 50 + 9 * (down_votes - up_votes))
        reason = "Multiple short-window inputs lean downward; this is not a settlement prediction."
    else:
        direction, label, score = "WAIT", "🟡 WAIT — PRESSURE MIXED", 0
        reason = "Short-window inputs do not agree strongly enough."

    return {
        "available": feed_ready, "direction": direction, "label": label,
        "rule_score": score, "up_votes": up_votes, "down_votes": down_votes,
        "trade_count_10s": len(trades),
        "trade_delta_pct_10s": round(trade_delta_pct, 2) if trade_delta_pct is not None else None,
        "aggressive_buy_pct_10s": round(buy_pct, 2) if buy_pct is not None else None,
        "book_imbalance_pct": round(book_imbalance, 2) if book_imbalance is not None else None,
        "microprice_edge_bps": round(micro_edge_bps, 3) if micro_edge_bps is not None else None,
        "spread_bps": round(spread_bps, 3) if spread_bps is not None else None,
        "price_move_bps_10s": round(price_move_bps, 3) if price_move_bps is not None else None,
        "book_age_ms": round(age * 1000, 1) if age is not None else None,
        "stream_connected": bool(book.get("connected")),
        "reconnects": int(book.get("reconnects", 0)),
        "evidence": evidence, "reason": reason,
        "warning": "Market-pressure estimate only; no participant identity or guaranteed future move."
    }


def safe_event_time(value):
    try:
        return float(value) / 1000.0
    except Exception:
        return time.time()


# =========================================================
# BASIC HELPERS
# =========================================================

def number(value):
    try:
        return float(value)
    except:
        return None


def get_json(url, params=None):
    try:
        response = session.get(
            url,
            params=params,
            timeout=TIMEOUT
        )

        response.raise_for_status()

        return response.json()

    except:
        return None


def parse_time(value):

    if not value:
        return None

    try:

        dt = datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00"
            )
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt

    except:
        return None


def seconds_left(value):

    dt = parse_time(value)

    if not dt:
        return None

    return max(
        0,
        int(
            dt.timestamp()
            - time.time()
        )
    )


def record_feed(name, value):

    item = feed_health.setdefault(
        name,
        {}
    )

    if value is not None:

        item["online"] = True
        item["last_success"] = time.time()

    else:

        item["online"] = False

    return value


# =========================================================
# SIGNAL MEMORY
# =========================================================

def load_memory():

    global signal_memory
    global memory_loaded

    if memory_loaded:
        return

    memory_loaded = True

    try:

        with open(
            MEMORY_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        if isinstance(data, list):

            signal_memory = data[-500:]

        else:

            signal_memory = []

    except:

        signal_memory = []


def save_memory():

    try:

        temp_file = MEMORY_FILE + ".tmp"

        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                signal_memory[-500:],
                file
            )

        os.replace(
            temp_file,
            MEMORY_FILE
        )

    except:
        pass


# =========================================================
# BINANCE
# =========================================================

def get_binance_price():
    """Prefer the freshest live WebSocket price; REST is emergency fallback."""
    global active_btc_source

    now = time.time()
    candidates = [
        (live_btc.get("price"), live_btc.get("received_at", 0.0), "Binance Live"),
        (live_coinbase.get("price"), live_coinbase.get("received_at", 0.0), "Coinbase Live"),
    ]

    candidates = [
        item for item in candidates
        if item[0] is not None
        and item[0] > 0
        and item[1]
        and now - item[1] <= STREAM_MAX_AGE
    ]

    if candidates:
        price, _, source = min(candidates, key=lambda item: now - item[1])
        active_btc_source = source
        return price, source

    sources = [
        ("Binance", "https://api.binance.com/api/v3/ticker/price", {"symbol": "BTCUSDT"}),
        ("Binance Data", "https://data-api.binance.vision/api/v3/ticker/price", {"symbol": "BTCUSDT"}),
        ("Binance.US", "https://api.binance.us/api/v3/ticker/price", {"symbol": "BTCUSD"}),
    ]

    for name, url, params in sources:
        data = get_json(url, params)
        if isinstance(data, dict):
            price = number(data.get("price"))
            if price and price > 0:
                active_btc_source = name + " REST"
                return price, name

    active_btc_source = "REST unavailable"
    return None, "REST unavailable"


# =========================================================
# BTC SPOT FEEDS
# =========================================================

def get_spot_feeds():

    binance_price, binance_name = (
        get_binance_price()
    )

    feeds = {}

    feeds["Binance"] = record_feed(
        "Binance",
        binance_price
    )

    if binance_name == "Binance Live":

        record_feed(
            "Binance Live",
            binance_price
        )

    data = get_json(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    )

    try:

        coinbase = number(
            data["data"]["amount"]
        )

    except:

        coinbase = None

    feeds["Coinbase"] = record_feed(
        "Coinbase",
        coinbase
    )

    data = get_json(
        "https://api.kraken.com/0/public/Ticker",
        {
            "pair": "XBTUSD"
        }
    )

    try:

        pair = next(
            iter(
                data["result"]
            )
        )

        kraken = number(
            data["result"][pair]["c"][0]
        )

    except:

        kraken = None

    feeds["Kraken"] = record_feed(
        "Kraken",
        kraken
    )

    data = get_json(
        "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    )

    if isinstance(data, dict):

        bitstamp = number(
            data.get("last")
        )

    else:

        bitstamp = None

    feeds["Bitstamp"] = record_feed(
        "Bitstamp",
        bitstamp
    )

    values = [
        value
        for value in feeds.values()
        if value is not None
        and value > 0
    ]

    if not values:

        return (
            None,
            feeds
        )

    med = statistics.median(
        values
    )

    filtered = [
        value
        for value in values
        if abs(
            value - med
        ) / med <= 0.0035
    ]

    reference = statistics.median(
        filtered or values
    )

    return (
        reference,
        feeds
    )


# =========================================================
# BUYER / SELLER BATTLE
# =========================================================

def get_buy_sell_pressure():

    result = {

        "available": False,

        "buyers": 0.0,

        "sellers": 0.0,

        "buy_pct": None,

        "sell_pct": None,

        "delta": None,

        "winner": "WAIT",

        "strength": "NO DATA",

        "trades": 0,

        "source": None
    }

    endpoints = [

        (
            "Binance",
            "https://api.binance.com/api/v3/aggTrades",
            {
                "symbol": "BTCUSDT",
                "limit": 1000
            }
        ),

        (
            "Binance",
            "https://data-api.binance.vision/api/v3/aggTrades",
            {
                "symbol": "BTCUSDT",
                "limit": 1000
            }
        ),

        (
            "Binance.US",
            "https://api.binance.us/api/v3/aggTrades",
            {
                "symbol": "BTCUSD",
                "limit": 1000
            }
        )
    ]

    for name, url, params in endpoints:

        data = get_json(
            url,
            params
        )

        if not isinstance(data, list):
            continue

        buyers = 0.0
        sellers = 0.0
        count = 0

        for trade in data:

            try:

                price = float(
                    trade.get(
                        "p",
                        0
                    )
                )

                quantity = float(
                    trade.get(
                        "q",
                        0
                    )
                )

                if (
                    price <= 0
                    or
                    quantity <= 0
                ):
                    continue

                notional = (
                    price *
                    quantity
                )

                if trade.get("m") is True:

                    sellers += notional

                else:

                    buyers += notional

                count += 1

            except:

                continue

        total = (
            buyers +
            sellers
        )

        if (
            total <= 0
            or
            count == 0
        ):
            continue

        buy_pct = (
            buyers /
            total
        ) * 100

        sell_pct = (
            sellers /
            total
        ) * 100

        delta = (
            buyers -
            sellers
        )

        if buy_pct >= 60:

            winner = "BUYERS"
            strength = "STRONG BUY"

        elif buy_pct >= 55:

            winner = "BUYERS"
            strength = "BUY"

        elif sell_pct >= 60:

            winner = "SELLERS"
            strength = "STRONG SELL"

        elif sell_pct >= 55:

            winner = "SELLERS"
            strength = "SELL"

        else:

            winner = "BALANCED"
            strength = "BALANCED"

        record_feed(
            "Binance Trades",
            total
        )

        result.update({

            "available": True,

            "buyers": buyers,

            "sellers": sellers,

            "buy_pct": buy_pct,

            "sell_pct": sell_pct,

            "delta": delta,

            "winner": winner,

            "strength": strength,

            "trades": count,

            "source": name
        })

        return result

    return result


# =========================================================
# ORDER BOOK PRESSURE
# =========================================================

def get_order_book_pressure():

    result = {

        "available": False,

        "bid_qty": 0.0,

        "ask_qty": 0.0,

        "bid_pct": None,

        "ask_pct": None,

        "winner": "WAIT",

        "strength": "NO DATA",

        "source": None
    }

    endpoints = [

        (
            "Binance",
            "https://api.binance.com/api/v3/depth",
            {
                "symbol": "BTCUSDT",
                "limit": 100
            }
        ),

        (
            "Binance",
            "https://data-api.binance.vision/api/v3/depth",
            {
                "symbol": "BTCUSDT",
                "limit": 100
            }
        ),

        (
            "Binance.US",
            "https://api.binance.us/api/v3/depth",
            {
                "symbol": "BTCUSD",
                "limit": 100
            }
        )
    ]

    for name, url, params in endpoints:

        data = get_json(
            url,
            params
        )

        if not isinstance(data, dict):
            continue

        bids = data.get(
            "bids",
            []
        )

        asks = data.get(
            "asks",
            []
        )

        try:

            bid_qty = sum(
                float(row[1])
                for row in bids
                if len(row) >= 2
            )

            ask_qty = sum(
                float(row[1])
                for row in asks
                if len(row) >= 2
            )

        except:

            continue

        total = (
            bid_qty +
            ask_qty
        )

        if total <= 0:
            continue

        bid_pct = (
            bid_qty /
            total
        ) * 100

        ask_pct = (
            ask_qty /
            total
        ) * 100

        if bid_pct >= 60:

            winner = "BIDS"
            strength = "STRONG BUY"

        elif bid_pct >= 55:

            winner = "BIDS"
            strength = "BUY"

        elif ask_pct >= 60:

            winner = "ASKS"
            strength = "STRONG SELL"

        elif ask_pct >= 55:

            winner = "ASKS"
            strength = "SELL"

        else:

            winner = "BALANCED"
            strength = "BALANCED"

        result.update({

            "available": True,

            "bid_qty": bid_qty,

            "ask_qty": ask_qty,

            "bid_pct": bid_pct,

            "ask_pct": ask_pct,

            "winner": winner,

            "strength": strength,

            "source": name
        })

        return result

    return result



# =========================================================
# 2 vs 2 BUYER / BEARER POWER ENGINE
# =========================================================

def clamp(value, low=0.0, high=100.0):
    return max(low, min(high, float(value)))


def pressure_score(buy_sell, order_book):
    """
    Combines executed trade pressure and visible order-book pressure.
    Returns independent BUY and SELL power scores from 0-100.
    """
    buy_pct = buy_sell.get("buy_pct")
    sell_pct = buy_sell.get("sell_pct")
    bid_pct = order_book.get("bid_pct")
    ask_pct = order_book.get("ask_pct")

    trade_buy = float(buy_pct) if buy_pct is not None else None
    trade_sell = float(sell_pct) if sell_pct is not None else None
    book_buy = float(bid_pct) if bid_pct is not None else None
    book_sell = float(ask_pct) if ask_pct is not None else None

    buy_parts = [v for v in (trade_buy, book_buy) if v is not None]
    sell_parts = [v for v in (trade_sell, book_sell) if v is not None]

    buy_power = (
        sum(buy_parts) / len(buy_parts)
        if buy_parts else None
    )
    sell_power = (
        sum(sell_parts) / len(sell_parts)
        if sell_parts else None
    )

    return buy_power, sell_power


def momentum_power(signal):
    """
    Converts the existing 1m/5m/15m momentum and structure into
    a directional 0-100 momentum score.
    """
    values = [
        (signal.get("m1"), 0.03),
        (signal.get("m5"), 0.08),
        (signal.get("m15"), 0.15),
    ]

    directional = []

    for value, scale in values:
        if value is None:
            continue

        try:
            ratio = clamp(abs(float(value)) / scale, 0.0, 1.0)

            if float(value) > 0:
                directional.append(50.0 + ratio * 50.0)
            elif float(value) < 0:
                directional.append(50.0 - ratio * 50.0)
            else:
                directional.append(50.0)
        except Exception:
            continue

    if not directional:
        base = 50.0
    else:
        base = sum(directional) / len(directional)

    structure = str(signal.get("structure") or "")

    if structure.startswith("HIGHER"):
        base += 10.0
    elif structure.startswith("LOWER"):
        base -= 10.0

    return clamp(base)


def build_power_battle(signal, buy_sell, order_book, quality):
    """
    Four-signal battle:

      BULL #1 = Buying Power
      BULL #2 = Bullish Momentum

      BEAR #1 = Selling Power
      BEAR #2 = Bearish Momentum

    The final winner is based on power, not simply counting 2 vs 2.
    """
    result = {
        "available": False,
        "bullish": {
            "buying_power": None,
            "momentum_power": None,
            "active": False,
        },
        "bearish": {
            "selling_power": None,
            "momentum_power": None,
            "active": False,
        },
        "bull_power": None,
        "bear_power": None,
        "winner": "WAIT",
        "label": "🟡 WAIT — BUILDING DATA",
        "confidence": 0,
        "reason": "Waiting for buying/selling pressure and momentum data.",
    }

    if quality < 50:
        result["reason"] = "Data quality is too low to choose a side."
        return result

    buy_power, sell_power = pressure_score(
        buy_sell,
        order_book
    )

    momentum = momentum_power(signal)

    if buy_power is None and sell_power is None:
        result["reason"] = "No reliable trade/order-book pressure data."
        return result

    # If one pressure source is unavailable, use the available source.
    if buy_power is None:
        buy_power = 50.0

    if sell_power is None:
        sell_power = 50.0

    bull_momentum = momentum
    bear_momentum = 100.0 - momentum

    bull_power = (
        buy_power * 0.60
        +
        bull_momentum * 0.40
    )

    bear_power = (
        sell_power * 0.60
        +
        bear_momentum * 0.40
    )

    bull_power = clamp(bull_power)
    bear_power = clamp(bear_power)

    # Require an actual edge. Small differences are WAIT.
    difference = abs(bull_power - bear_power)

    if difference < 7:
        winner = "WAIT"
        label = "🟡 WAIT — BATTLE TOO CLOSE"
        confidence = 50
        reason = "Buying and selling power are too close to call."

    elif bull_power > bear_power:
        winner = "BULLS"
        label = "🟢 BULLS WINNING"
        confidence = round(
            clamp(50 + difference * 1.25, 50, 96)
        )
        reason = "Buying power and bullish momentum have the stronger combined score."

    else:
        winner = "BEARS"
        label = "🔴 BEARS WINNING"
        confidence = round(
            clamp(50 + difference * 1.25, 50, 96)
        )
        reason = "Selling power and bearish momentum have the stronger combined score."

    # A stale/missing pressure feed should never create a strong verdict.
    pressure_available = (
        buy_sell.get("available")
        or
        order_book.get("available")
    )

    if not pressure_available:
        winner = "WAIT"
        label = "🟡 WAIT — NO PRESSURE DATA"
        confidence = 0
        reason = "Pressure feeds are unavailable."

    result.update({
        "available": bool(pressure_available),
        "bullish": {
            "buying_power": round(buy_power, 1),
            "momentum_power": round(bull_momentum, 1),
            "active": bull_power > 50,
        },
        "bearish": {
            "selling_power": round(sell_power, 1),
            "momentum_power": round(bear_momentum, 1),
            "active": bear_power > 50,
        },
        "bull_power": round(bull_power, 1),
        "bear_power": round(bear_power, 1),
        "winner": winner,
        "label": label,
        "confidence": confidence,
        "reason": reason,
    })

    return result



# =========================================================
# RAPID BATTLE-SHIFT DETECTOR
# =========================================================

def _signed_number(value):
    try:
        return float(value)
    except Exception:
        return None


def build_shift_detector(signal, buy_sell, order_book, power_battle, price):
    """
    Detect a fast change in control between dashboard snapshots.
    Power Battle answers who is strongest now; this answers who is gaining
    or losing control now.
    """
    current = {
        "time": time.time(),
        "price": _signed_number(price),
        "buy_power": _signed_number((power_battle.get("bullish") or {}).get("buying_power")),
        "sell_power": _signed_number((power_battle.get("bearish") or {}).get("selling_power")),
        "bull_momentum": _signed_number((power_battle.get("bullish") or {}).get("momentum_power")),
        "bear_momentum": _signed_number((power_battle.get("bearish") or {}).get("momentum_power")),
        "buy_pct": _signed_number(buy_sell.get("buy_pct")),
        "sell_pct": _signed_number(buy_sell.get("sell_pct")),
        "delta": _signed_number(buy_sell.get("delta")),
        "bid_pct": _signed_number(order_book.get("bid_pct")),
        "ask_pct": _signed_number(order_book.get("ask_pct")),
        "m1": _signed_number(signal.get("m1")),
        "m5": _signed_number(signal.get("m5")),
        "winner": power_battle.get("winner", "WAIT"),
    }

    with shift_lock:
        previous = shift_history[-1] if shift_history else None
        recent = shift_history[-6:]
        shift_history.append(current)
        if len(shift_history) > SHIFT_HISTORY_MAX:
            del shift_history[:-SHIFT_HISTORY_MAX]

    if previous is None:
        return {
            "available": False,
            "direction": "WAIT",
            "label": "🟡 BUILDING SHIFT HISTORY",
            "score": 0,
            "signals": [],
            "detail": "Watching for a change in control...",
            "bull_shift": 0,
            "bear_shift": 0,
        }

    baseline = {}
    for key in ("buy_power", "sell_power", "bull_momentum", "bear_momentum",
                "buy_pct", "sell_pct", "bid_pct", "ask_pct", "m1", "m5"):
        vals = [x.get(key) for x in recent if x.get(key) is not None]
        baseline[key] = (sum(vals) / len(vals)) if vals else None

    bull = 0
    bear = 0
    bull_signals = []
    bear_signals = []

    def change(key):
        now = current.get(key)
        base = baseline.get(key)
        if now is None or base is None:
            return None
        return now - base

    def add_bull(msg):
        nonlocal bull
        bull += 1
        bull_signals.append(msg)

    def add_bear(msg):
        nonlocal bear
        bear += 1
        bear_signals.append(msg)

    c = change("buy_power")
    if c is not None:
        if c >= 8: add_bull(f"Buying Power +{c:.1f}")
        elif c <= -8: add_bear(f"Buying Power {c:.1f}")

    c = change("sell_power")
    if c is not None:
        if c >= 8: add_bear(f"Selling Power +{c:.1f}")
        elif c <= -8: add_bull(f"Selling Power {c:.1f}")

    c = change("bull_momentum")
    if c is not None:
        if c >= 8: add_bull(f"Bullish Momentum +{c:.1f}")
        elif c <= -8: add_bear(f"Bullish Momentum {c:.1f}")

    c = change("bear_momentum")
    if c is not None:
        if c >= 8: add_bear(f"Bearish Momentum +{c:.1f}")
        elif c <= -8: add_bull(f"Bearish Momentum {c:.1f}")

    c = change("ask_pct")
    if c is not None:
        if c >= 10: add_bear(f"Asks +{c:.1f} pts")
        elif c <= -10: add_bull(f"Asks {c:.1f} pts")

    c = change("bid_pct")
    if c is not None:
        if c >= 10: add_bull(f"Bids +{c:.1f} pts")
        elif c <= -10: add_bear(f"Bids {c:.1f} pts")

    c = change("buy_pct")
    if c is not None:
        if c >= 8: add_bull(f"Buyers +{c:.1f} pts")
        elif c <= -8: add_bear(f"Buyers {c:.1f} pts")

    c = change("sell_pct")
    if c is not None:
        if c >= 8: add_bear(f"Sellers +{c:.1f} pts")
        elif c <= -8: add_bull(f"Sellers {c:.1f} pts")

    old_delta = previous.get("delta")
    new_delta = current.get("delta")
    if old_delta is not None and new_delta is not None:
        if old_delta > 0 and new_delta < 0: add_bear("Delta flipped NEGATIVE")
        elif old_delta < 0 and new_delta > 0: add_bull("Delta flipped POSITIVE")

    old_m1 = previous.get("m1")
    new_m1 = current.get("m1")
    if old_m1 is not None and new_m1 is not None:
        if old_m1 > 0 and new_m1 < 0: add_bear("1m momentum flipped DOWN")
        elif old_m1 < 0 and new_m1 > 0: add_bull("1m momentum flipped UP")

    if bear >= 3 and bear > bull:
        direction, label = "BEAR", "🔴 BEARS TAKING CONTROL"
        detail = " • ".join(bear_signals[:4])
    elif bull >= 3 and bull > bear:
        direction, label = "BULL", "🟢 BULLS TAKING CONTROL"
        detail = " • ".join(bull_signals[:4])
    elif bear >= 2 and bear > bull:
        direction, label = "BEAR", "⚠️ BEARISH SHIFT"
        detail = " • ".join(bear_signals[:4])
    elif bull >= 2 and bull > bear:
        direction, label = "BULL", "⚠️ BULLISH SHIFT"
        detail = " • ".join(bull_signals[:4])
    elif current.get("winner") == "BULLS":
        direction, label = "BULL", "🟢 BULLS STABLE"
        detail = "No rapid control shift detected."
    elif current.get("winner") == "BEARS":
        direction, label = "BEAR", "🔴 BEARS STABLE"
        detail = "No rapid control shift detected."
    else:
        direction, label = "WAIT", "🟡 BATTLE STABLE / MIXED"
        detail = "No strong control shift detected."

    return {
        "available": True,
        "direction": direction,
        "label": label,
        "score": abs(bull - bear),
        "signals": (bear_signals if direction == "BEAR" else bull_signals)[:5],
        "detail": detail,
        "bull_shift": bull,
        "bear_shift": bear,
    }


# =========================================================
# PREDICTION STRENGTH
# =========================================================

def prediction_strength(
    signal,
    price,
    market,
    buy_sell,
    order_book,
    quality,
    memory_rate=None,
    memory_matches=0
):

    up = 0
    down = 0

    if (
        price is not None
        and
        market
    ):

        target = market.get(
            "target"
        )

        if target is not None:

            if price > target:
                up += 1

            elif price < target:
                down += 1

    for value in (
        signal.get("m1"),
        signal.get("m5"),
        signal.get("m15")
    ):

        if value is not None:

            if value > 0:
                up += 1

            elif value < 0:
                down += 1

    structure = signal.get(
        "structure",
        ""
    )

    if structure.startswith(
        "HIGHER"
    ):

        up += 1

    elif structure.startswith(
        "LOWER"
    ):

        down += 1

    if buy_sell.get(
        "available"
    ):

        if buy_sell.get(
            "winner"
        ) == "BUYERS":

            up += 1

        elif buy_sell.get(
            "winner"
        ) == "SELLERS":

            down += 1

    if order_book.get(
        "available"
    ):

        if order_book.get(
            "winner"
        ) == "BIDS":

            up += 1

        elif order_book.get(
            "winner"
        ) == "ASKS":

            down += 1

    yes = yes_mid(
        market
    )

    if yes is not None:

        if yes >= 0.60:
            up += 1

        elif yes <= 0.40:
            down += 1

    if signal.get(
        "reversal"
    ) == "HIGH":

        if up > down:
            up = max(
                0,
                up - 2
            )

        elif down > up:
            down = max(
                0,
                down - 2
            )

    if (
        memory_matches >= 3
        and
        memory_rate is not None
    ):

        if memory_rate >= 70:

            if signal.get(
                "verdict"
            ) == "UP":

                up += 1

            elif signal.get(
                "verdict"
            ) == "DOWN":

                down += 1

        elif memory_rate <= 40:

            if signal.get("verdict") == "UP":

                up = max(
                    0,
                    up - 1
                )

            elif signal.get("verdict") == "DOWN":

                down = max(
                    0,
                    down - 1
                )

    total = (
        up +
        down
    )

    if total == 0:

        direction = "WAIT"
        label = "🟡 WAIT"
        score = 0

    else:

        if up > down:

            direction = "UP"

            score = min(
                10,
                round(
                    up /
                    max(
                        10,
                        total
                    ) * 10
                )
            )

            label = (
                f"🟢 UP — {score}/10"
            )

        elif down > up:

            direction = "DOWN"

            score = min(
                10,
                round(
                    down /
                    max(
                        10,
                        total
                    ) * 10
                )
            )

            label = (
                f"🔴 DOWN — {score}/10"
            )

        else:

            direction = "WAIT"
            score = 5
            label = (
                "🟡 WAIT — BALANCED"
            )

    if signal.get(
        "reversal"
    ) == "HIGH":

        label = (
            "🟡 WAIT — REVERSAL RISK"
        )

        direction = "WAIT"

    if quality < 60:

        label = (
            "🟡 WAIT — DATA QUALITY"
        )

        direction = "WAIT"

    return {

        "direction": direction,

        "score": score,

        "label": label,

        "bullish_points": up,

        "bearish_points": down
    }


# =========================================================
# BTC HISTORY
# =========================================================

def get_history():

    now = time.time()

    if (
        history_cache["candles"]
        and
        now -
        history_cache["time"]
        < 15
    ):

        return history_cache[
            "candles"
        ]

    end = datetime.now(
        timezone.utc
    )

    start = (
        end -
        timedelta(
            minutes=21
        )
    )

    sources = [

        (
            "Coinbase",
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            {
                "granularity": 60,
                "start":
                    start.isoformat(),
                "end":
                    end.isoformat()
            }
        ),

        (
            "Kraken",
            "https://api.kraken.com/0/public/OHLC",
            {
                "pair": "XBTUSD",
                "interval": 1
            }
        ),

        (
            "Binance",
            "https://api.binance.com/api/v3/klines",
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 21
            }
        ),

        (
            "Binance",
            "https://data-api.binance.vision/api/v3/klines",
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 21
            }
        ),

        (
            "Binance.US",
            "https://api.binance.us/api/v3/klines",
            {
                "symbol": "BTCUSD",
                "interval": "1m",
                "limit": 21
            }
        )
    ]

    for mode, url, params in sources:

        data = get_json(
            url,
            params
        )

        candles = []

        try:

            if mode == "Coinbase":

                for row in (
                    data
                    if isinstance(
                        data,
                        list
                    )
                    else []
                ):

                    close = number(
                        row[4]
                    )

                    if close is not None:

                        candles.append(
                            (
                                float(row[0]),
                                close
                            )
                        )

            elif mode == "Kraken":

                result = data[
                    "result"
                ]

                pair = next(
                    key
                    for key in result
                    if key != "last"
                )

                for row in result[
                    pair
                ][-21:]:

                    close = number(
                        row[4]
                    )

                    if close is not None:

                        candles.append(
                            (
                                float(row[0]),
                                close
                            )
                        )

            else:

                for row in (
                    data
                    if isinstance(
                        data,
                        list
                    )
                    else []
                ):

                    close = number(
                        row[4]
                    )

                    if close is not None:

                        candles.append(
                            (
                                float(
                                    row[0]
                                ) / 1000,
                                close
                            )
                        )

        except:

            candles = []

        candles.sort()

        if len(candles) >= 16:

            history_cache[
                "time"
            ] = now

            history_cache[
                "candles"
            ] = candles

            record_feed(
                mode +
                " Candles",
                candles[-1][1]
            )

            return candles

    history_cache[
        "time"
    ] = now

    history_cache[
        "candles"
    ] = []

    return []


# =========================================================
# MOMENTUM
# =========================================================

def momentum(
    candles,
    minutes
):

    if len(candles) < 2:
        return None

    current_time, current = (
        candles[-1]
    )

    target_time = (
        current_time -
        minutes * 60
    )

    previous = None

    for timestamp, close in reversed(
        candles[:-1]
    ):

        if timestamp <= target_time:

            previous = close
            break

    if not previous:
        return None

    return (
        (
            current -
            previous
        )
        /
        previous
    ) * 100


# =========================================================
# STRUCTURE
# =========================================================

def price_structure(candles):

    if len(candles) < 8:
        return "WAIT"

    values = [
        close
        for _, close
        in candles[-8:]
    ]

    first = values[:4]
    second = values[4:]

    if (
        max(second) > max(first)
        and
        min(second) > min(first)
    ):

        return (
            "HIGHER HIGHS / "
            "HIGHER LOWS"
        )

    if (
        max(second) < max(first)
        and
        min(second) < min(first)
    ):

        return (
            "LOWER HIGHS / "
            "LOWER LOWS"
        )

    return "MIXED"


# =========================================================
# KALSHI NORMALIZATION
# =========================================================

def normalize_market(market):

    if (
        not isinstance(
            market,
            dict
        )
        or
        not market.get(
            "ticker"
        )
    ):

        return None

    def probability(*keys):

        for key in keys:

            value = number(
                market.get(
                    key
                )
            )

            if value is not None:

                if value > 1:

                    return (
                        value /
                        100
                    )

                return value

        return None

    # Kalshi market schemas differ by market type/version. Check all
    # documented strike fields, and reject zero/negative placeholders.
    target = None
    target_source = None

    for key in (
        "floor_strike",
        "strike_price",
        "strike",
        "target",
        "functional_strike",
        "custom_strike",
        "cap_strike",
    ):
        value = number(market.get(key))
        if value is not None and math.isfinite(value) and value > 0:
            target = value
            target_source = key
            break

    # Some responses expose structured strike metadata rather than a
    # top-level scalar. Only accept an explicitly numeric strike value.
    if target is None:
        for container_key in ("strike", "strike_details", "price_level"):
            details = market.get(container_key)
            if isinstance(details, dict):
                for key in ("value", "price", "strike_price", "target", "floor_strike"):
                    value = number(details.get(key))
                    if value is not None and math.isfinite(value) and value > 0:
                        target = value
                        target_source = f"{container_key}.{key}"
                        break
            if target is not None:
                break

    return {

        "ticker":
            market.get(
                "ticker"
            ),

        "target":
            target,

        "target_source":
            target_source,

        "yes_bid":
            probability(
                "yes_bid_dollars",
                "yes_bid"
            ),

        "yes_ask":
            probability(
                "yes_ask_dollars",
                "yes_ask"
            ),

        "last":
            probability(
                "last_price_dollars",
                "last_price"
            ),

        "close_time":
            (
                market.get(
                    "close_time"
                )
                or
                market.get(
                    "expiration_time"
                )
            )
    }


# =========================================================
# KALSHI
# =========================================================

def get_kalshi():

    global kalshi_last_update

    if KALSHI_TICKER:

        for base in KALSHI_BASES:

            data = get_json(
                f"{base}/markets/{KALSHI_TICKER}"
            )

            if isinstance(
                data,
                dict
            ):

                market = normalize_market(
                    data.get(
                        "market",
                        data
                    )
                )

                if market:

                    close = parse_time(
                        market.get(
                            "close_time"
                        )
                    )

                    if (
                        close is None
                        or
                        close.timestamp()
                        > time.time()
                    ):

                        kalshi_last_update = (
                            time.time()
                        )

                        return market

    for base in KALSHI_BASES:

        data = get_json(
            f"{base}/markets",
            {
                "series_ticker":
                    KALSHI_SERIES,

                "status":
                    "open",

                "limit":
                    100
            }
        )

        markets = (
            data.get(
                "markets",
                []
            )
            if isinstance(
                data,
                dict
            )
            else []
        )

        candidates = []

        now = time.time()

        for market in markets:

            ticker = str(
                market.get(
                    "ticker",
                    ""
                )
            ).upper()

            if not ticker.startswith(
                KALSHI_SERIES
            ):

                continue

            close = parse_time(
                market.get(
                    "close_time"
                )
                or
                market.get(
                    "expiration_time"
                )
            )

            if (
                close
                and
                close.timestamp()
                > now
            ):

                candidates.append(
                    market
                )

        candidates.sort(
            key=lambda market:
            parse_time(
                market.get(
                    "close_time"
                )
                or
                market.get(
                    "expiration_time"
                )
            ).timestamp()
        )

        if candidates:

            kalshi_last_update = (
                time.time()
            )

            return normalize_market(
                candidates[0]
            )

    return None


def yes_mid(market):

    if not market:
        return None

    bid = market.get(
        "yes_bid"
    )

    ask = market.get(
        "yes_ask"
    )

    if (
        bid is not None
        and
        ask is not None
    ):

        return (
            bid +
            ask
        ) / 2

    return market.get(
        "last"
    )


# =========================================================
# QUALITY
# =========================================================

def calculate_quality(
    feeds,
    market,
    candles
):

    values = [
        value
        for value in feeds.values()
        if value is not None
    ]

    live = len(values)

    score = 0

    if live >= 4:

        score += 35

    elif live == 3:

        score += 30

    elif live == 2:

        score += 20

    elif live == 1:

        score += 8

    score += 25

    spread = 0

    if len(values) >= 2:

        med = statistics.median(
            values
        )

        if med:

            spread = (
                max(values)
                -
                min(values)
            ) / med * 100

    if spread <= 0.03:

        score += 30

    elif spread <= 0.08:

        score += 25

    elif spread <= 0.20:

        score += 15

    elif spread <= 0.35:

        score += 5

    kalshi_score = 0

    if market:

        if market.get(
            "ticker"
        ):

            kalshi_score += 30

        if market.get(
            "target"
        ) is not None:

            kalshi_score += 25

        if yes_mid(
            market
        ) is not None:

            kalshi_score += 25

        if market.get(
            "close_time"
        ):

            kalshi_score += 20

    score = (
        score * 0.60
        +
        kalshi_score * 0.25
    )

    if len(candles) >= 20:

        score += 15

    elif len(candles) >= 16:

        score += 10

    elif len(candles) >= 10:

        score += 5

    score = round(
        min(
            100,
            score
        )
    )

    grade = (

        "HIGH"
        if score >= 85

        else
        "GOOD"
        if score >= 70

        else
        "FAIR"
        if score >= 50

        else
        "LOW"
    )

    return (
        score,
        grade
    )


# =========================================================
# SIGNAL ENGINE
# =========================================================

def build_signal(
    price,
    market,
    candles,
    quality
):

    m1 = momentum(
        candles,
        1
    )

    m5 = momentum(
        candles,
        5
    )

    m15 = momentum(
        candles,
        15
    )

    structure = price_structure(
        candles
    )

    signal = {

        "verdict":
            "WAIT",

        "label":
            "🟡 WAIT",

        "confidence":
            0,

        "score":
            0,

        "bullish":
            0,

        "bearish":
            0,

        "m1":
            m1,

        "m5":
            m5,

        "m15":
            m15,

        "structure":
            structure,

        "acceleration":
            "STABLE / MIXED",

        "reversal":
            "LOW",

        "quality_score":
            quality,

        "reasons":
            []
    }

    if (
        price is None
        or
        not market
        or
        market.get(
            "target"
        ) is None
        or
        len(candles) < 16
    ):

        signal["reasons"] = [
            "Waiting for live BTC history and Kalshi target."
        ]

        return signal

    target = market[
        "target"
    ]

    bullish = 0
    bearish = 0

    if price > target:

        bullish += 2

        signal[
            "reasons"
        ].append(
            "BTC is above the Kalshi target."
        )

    else:

        bearish += 2

        signal[
            "reasons"
        ].append(
            "BTC is below the Kalshi target."
        )

    for value in (
        m1,
        m5,
        m15
    ):

        if value is None:
            continue

        if value > 0:

            bullish += 2

        elif value < 0:

            bearish += 2

    if structure.startswith(
        "HIGHER"
    ):

        bullish += 2

        signal[
            "reasons"
        ].append(
            "Price structure is bullish."
        )

    elif structure.startswith(
        "LOWER"
    ):

        bearish += 2

        signal[
            "reasons"
        ].append(
            "Price structure is bearish."
        )

    if (
        m1 is not None
        and
        m5 is not None
        and
        m15 is not None
    ):

        if (
            m1 < -0.01
            and
            m5 < -0.02
            and
            m15 < -0.04
        ):

            signal[
                "acceleration"
            ] = (
                "ACCELERATING DOWN"
            )

            bearish += 2

        elif (
            m1 > 0.01
            and
            m5 > 0.02
            and
            m15 > 0.04
        ):

            signal[
                "acceleration"
            ] = (
                "ACCELERATING UP"
            )

            bullish += 2

        if (
            m15 < 0
            and
            m5 < 0
            and
            m1 > 0
        ) or (
            m15 > 0
            and
            m5 > 0
            and
            m1 < 0
        ):

            signal[
                "reversal"
            ] = "HIGH"

    yes = yes_mid(
        market
    )

    if yes is not None:

        if yes >= 0.60:

            bullish += 1

            signal[
                "reasons"
            ].append(
                "Kalshi YES is favoring UP."
            )

        elif yes <= 0.40:

            bearish += 1

            signal[
                "reasons"
            ].append(
                "Kalshi YES is favoring DOWN."
            )

    total = (
        bullish +
        bearish
    )

    difference = abs(
        bullish -
        bearish
    )

    if (
        total >= 7
        and
        difference >= 3
    ):

        if bullish > bearish:

            signal[
                "verdict"
            ] = "UP"

            signal[
                "label"
            ] = (
                "🟢 UP — STRONG CONFIRMATION"
            )

        else:

            signal[
                "verdict"
            ] = "DOWN"

            signal[
                "label"
            ] = (
                "🔴 DOWN — STRONG CONFIRMATION"
            )

        signal[
            "confidence"
        ] = min(
            96,
            60
            +
            difference * 5
            +
            min(
                quality * 0.10,
                8
            )
        )

    elif (
        total >= 5
        and
        difference >= 2
    ):

        if bullish > bearish:

            signal[
                "verdict"
            ] = "UP"

            signal[
                "label"
            ] = (
                "🟢 UP — CONFIRMING"
            )

        else:

            signal[
                "verdict"
            ] = "DOWN"

            signal[
                "label"
            ] = (
                "🔴 DOWN — CONFIRMING"
            )

        signal[
            "confidence"
        ] = min(
            88,
            55
            +
            difference * 5
            +
            min(
                quality * 0.10,
                8
            )
        )

    else:

        signal[
            "reasons"
        ].append(
            "Signals are not aligned strongly enough."
        )

    if (
        signal[
            "reversal"
        ]
        ==
        "HIGH"
    ):

        signal[
            "verdict"
        ] = "WAIT"

        signal[
            "label"
        ] = (
            "🟡 WAIT — REVERSAL RISK"
        )

        signal[
            "confidence"
        ] = 0

    countdown = seconds_left(
        market.get(
            "close_time"
        )
    )

    if (
        countdown is not None
        and
        countdown <= 60
    ):

        signal[
            "verdict"
        ] = "WAIT"

        signal[
            "label"
        ] = (
            "🟡 WAIT — FINAL-MINUTE BRAKE"
        )

        signal[
            "confidence"
        ] = 0

    signal[
        "bullish"
    ] = bullish

    signal[
        "bearish"
    ] = bearish

    signal[
        "score"
    ] = (
        bullish -
        bearish
    )

    return signal


# =========================================================
# SIGNAL MEMORY
# =========================================================

def update_memory(
    market,
    price,
    signal,
    distance_pct
):

    global active_market

    load_memory()

    close = parse_time(
        market.get(
            "close_time"
        )
    )

    close_ts = (
        close.timestamp()
        if close
        else
        time.time() + 900
    )

    ticker = market.get(
        "ticker"
    )

    # Resolve an expired previous market BEFORE replacing its tracking
    # record with the next ticker. The old implementation could silently
    # discard that record on market rollover.
    if (
        active_market is not None
        and active_market.get("ticker") != ticker
        and time.time() >= active_market.get("close_ts", float("inf"))
    ):
        _resolve_active_market(source="last_observed_price_estimate")

    # If the same market is still active but its close time has passed,
    # resolve it before updating its price with a post-close quote.
    if (
        active_market is not None
        and active_market.get("ticker") == ticker
        and time.time() >= active_market.get("close_ts", float("inf"))
    ):
        _resolve_active_market(source="last_observed_price_estimate")

    # Start tracking a new 15-minute market.
    if (
        active_market is None
        or
        active_market.get(
            "ticker"
        )
        != ticker
    ):

        active_market = {

            "ticker":
                ticker,

            "close_ts":
                close_ts,

            "target":
                market.get(
                    "target"
                ),

            # The first real UP/DOWN signal is the prediction we score.
            # WAIT is deliberately not counted as a prediction.
            "direction":
                (
                    signal[
                        "verdict"
                    ]
                    if signal[
                        "verdict"
                    ] in (
                        "UP",
                        "DOWN"
                    )
                    else
                    "WAIT"
                ),

            "confidence":
                (
                    signal.get(
                        "confidence"
                    )
                    if signal.get(
                        "verdict"
                    ) in (
                        "UP",
                        "DOWN"
                    )
                    else
                    None
                ),

            "signal_score":
                signal.get(
                    "score"
                ),

            "prediction_locked":
                signal.get(
                    "verdict"
                ) in (
                    "UP",
                    "DOWN"
                ),

            "prediction_locked_at":
                (
                    datetime.now(
                        timezone.utc
                    ).isoformat()
                    if signal.get(
                        "verdict"
                    ) in (
                        "UP",
                        "DOWN"
                    )
                    else
                    None
                ),

            "m1":
                signal.get(
                    "m1"
                ),

            "m5":
                signal.get(
                    "m5"
                ),

            "m15":
                signal.get(
                    "m15"
                ),

            "distance":
                distance_pct,

            "structure":
                signal.get(
                    "structure"
                ),

            "last_price":
                price
        }

    else:

        active_market[
            "last_price"
        ] = price

        # Keep the original prediction snapshot locked.
        # These fields are updated only for diagnostic context.
        active_market[
            "current_m1"
        ] = signal.get(
            "m1"
        )

        active_market[
            "current_m5"
        ] = signal.get(
            "m5"
        )

        active_market[
            "current_m15"
        ] = signal.get(
            "m15"
        )

        active_market[
            "current_distance"
        ] = distance_pct

        active_market[
            "current_structure"
        ] = signal.get(
            "structure"
        )

        if (
            signal.get(
                "verdict"
            ) in (
                "UP",
                "DOWN"
            )
            and
            active_market.get(
                "direction"
            ) == "WAIT"
        ):

            active_market[
                "direction"
            ] = signal[
                "verdict"
            ]

            active_market[
                "confidence"
            ] = signal.get(
                "confidence"
            )

            active_market[
                "signal_score"
            ] = signal.get(
                "score"
            )

            active_market[
                "prediction_locked"
            ] = True

            active_market[
                "prediction_locked_at"
            ] = (
                datetime.now(
                    timezone.utc
                ).isoformat()
            )

            # Save the exact conditions at the moment the first
            # real prediction was made.
            active_market[
                "m1"
            ] = signal.get(
                "m1"
            )

            active_market[
                "m5"
            ] = signal.get(
                "m5"
            )

            active_market[
                "m15"
            ] = signal.get(
                "m15"
            )

            active_market[
                "distance"
            ] = distance_pct

            active_market[
                "structure"
            ] = signal.get(
                "structure"
            )

    if (
        active_market is not None
        and time.time() >= active_market.get("close_ts", 0)
    ):
        _resolve_active_market(source="last_observed_price_estimate")


def _resolve_active_market(source="last_observed_price_estimate"):
    """Store one completed tracker record without silently losing it.

    Important: this app-side last observed spot price is an estimate, not
    an official Kalshi/CF Benchmarks settlement result. Keep that provenance
    in the record so scoreboard results are not mistaken for verified fills.
    """
    global active_market

    if not active_market:
        return

    record = active_market
    target = record.get("target")
    final_price = record.get("last_price")
    direction = record.get("direction")
    ticker = record.get("ticker")

    # Avoid duplicate records if the app retries the same rollover.
    if any(item.get("ticker") == ticker for item in signal_memory):
        active_market = None
        return

    if target is not None and final_price is not None:
        if final_price > target:
            outcome = "UP"
        elif final_price < target:
            outcome = "DOWN"
        else:
            outcome = "PUSH"

        if direction in ("UP", "DOWN"):
            result = "WIN" if direction == outcome else "LOSS"
            scored = True
        else:
            result = "UNSCORED"
            scored = False

        signal_memory.append({
            **record,
            "outcome": outcome,
            "result": result,
            "scored": scored,
            "outcome_source": source,
            "outcome_verified": False,
            "resolved": datetime.now(timezone.utc).isoformat()
        })
        signal_memory[:] = signal_memory[-500:]
        save_memory()

    active_market = None


def performance_stats():

    load_memory()

    wins = 0
    losses = 0
    pushes = 0
    unscored = 0

    results = []

    confidence_buckets = {
        "90-96": {
            "wins": 0,
            "losses": 0
        },
        "80-89": {
            "wins": 0,
            "losses": 0
        },
        "70-79": {
            "wins": 0,
            "losses": 0
        },
        "50-69": {
            "wins": 0,
            "losses": 0
        }
    }

    # Read both the new records and older memory records so an existing
    # signal_memory.json does not have to be deleted.
    for record in signal_memory:

        direction = record.get(
            "direction"
        )

        outcome = record.get(
            "outcome"
        )

        result = record.get(
            "result"
        )

        # Legacy records have no "result" field.
        if result not in (
            "WIN",
            "LOSS",
            "PUSH",
            "UNSCORED"
        ):

            if (
                direction in (
                    "UP",
                    "DOWN"
                )
                and
                outcome in (
                    "UP",
                    "DOWN"
                )
            ):

                result = (
                    "WIN"
                    if direction == outcome
                    else
                    "LOSS"
                )

            elif outcome == "PUSH":

                result = "PUSH"

            else:

                result = "UNSCORED"

        if result == "WIN":

            wins += 1

        elif result == "LOSS":

            losses += 1

        elif result == "PUSH":

            pushes += 1

        else:

            unscored += 1

        if result in (
            "WIN",
            "LOSS"
        ):

            results.append({
                "result": result,
                "confidence":
                    record.get(
                        "confidence"
                    ),
                "resolved":
                    record.get(
                        "resolved"
                    ),
                "direction":
                    direction
            })

            confidence = record.get(
                "confidence"
            )

            if confidence is not None:

                try:
                    confidence = float(
                        confidence
                    )
                except Exception:
                    confidence = None

            if confidence is not None:

                if confidence >= 90:
                    bucket = "90-96"

                elif confidence >= 80:
                    bucket = "80-89"

                elif confidence >= 70:
                    bucket = "70-79"

                else:
                    bucket = "50-69"

                if result == "WIN":
                    confidence_buckets[
                        bucket
                    ]["wins"] += 1

                elif result == "LOSS":
                    confidence_buckets[
                        bucket
                    ]["losses"] += 1

    decided = wins + losses

    accuracy = (
        (wins / decided) * 100
        if decided
        else
        None
    )

    recent = results[-10:]

    recent_wins = sum(
        1
        for item in recent
        if item["result"] == "WIN"
    )

    recent_losses = sum(
        1
        for item in recent
        if item["result"] == "LOSS"
    )

    recent_decided = (
        recent_wins +
        recent_losses
    )

    recent_accuracy = (
        (recent_wins / recent_decided) * 100
        if recent_decided
        else
        None
    )

    streak_type = None
    streak = 0

    for item in reversed(results):

        result = item["result"]

        if streak_type is None:

            streak_type = result
            streak = 1

        elif result == streak_type:

            streak += 1

        else:

            break

    bucket_stats = {}

    for name, bucket in confidence_buckets.items():

        total = (
            bucket["wins"] +
            bucket["losses"]
        )

        bucket_stats[name] = {

            "wins":
                bucket["wins"],

            "losses":
                bucket["losses"],

            "decided":
                total,

            "accuracy":
                (
                    bucket["wins"] /
                    total *
                    100
                    if total
                    else
                    None
                )
        }

    return {

        "total_records":
            len(signal_memory),

        "decided":
            decided,

        "wins":
            wins,

        "losses":
            losses,

        "pushes":
            pushes,

        "unscored":
            unscored,

        "accuracy":
            accuracy,

        "recent_decided":
            recent_decided,

        "recent_accuracy":
            recent_accuracy,

        "streak_type":
            streak_type,

        "streak":
            streak,

        "last10":
            [
                item["result"]
                for item in recent
            ],

        "confidence_buckets":
            bucket_stats,

        # 20 decided predictions is a useful minimum sample marker.
        # It does NOT mean 20 is statistically sufficient for every use.
        "enough_data":
            decided >= 20,

        "minimum_sample":
            20
    }


def memory_match(
    signal,
    distance_pct
):

    load_memory()

    if signal.get(
        "verdict"
    ) not in (
        "UP",
        "DOWN"
    ):

        return (
            0,
            None
        )

    current = {

        "direction":
            signal[
                "verdict"
            ],

        "m1":
            signal.get(
                "m1"
            ),

        "m5":
            signal.get(
                "m5"
            ),

        "m15":
            signal.get(
                "m15"
            ),

        "distance":
            distance_pct,

        "structure":
            signal.get(
                "structure"
            )
    }

    matches = []

    for record in signal_memory:

        if (
            record.get(
                "direction"
            )
            not in (
                "UP",
                "DOWN"
            )
        ):

            continue

        if (
            record.get(
                "outcome"
            )
            not in (
                "UP",
                "DOWN"
            )
        ):

            continue

        points = 0
        total = 0

        for key, tolerance in (

            (
                "m1",
                0.025
            ),

            (
                "m5",
                0.05
            ),

            (
                "m15",
                0.08
            ),

            (
                "distance",
                0.10
            )
        ):

            a = current.get(
                key
            )

            b = record.get(
                key
            )

            if (
                a is not None
                and
                b is not None
            ):

                total += 1

                if abs(
                    a - b
                ) <= tolerance:

                    points += 1

        if (
            current.get(
                "structure"
            )
            ==
            record.get(
                "structure"
            )
        ):

            total += 1
            points += 1

        if (
            total
            and
            points / total >= 0.67
        ):

            matches.append(
                record
            )

    matches = matches[-25:]

    if not matches:

        return (
            0,
            None
        )

    wins = sum(
        1
        for record in matches
        if record.get(
            "outcome"
        )
        ==
        current[
            "direction"
        ]
    )

    rate = (
        wins /
        len(matches)
    ) * 100

    return (
        len(matches),
        rate
    )


# =========================================================
# MARKET CONTROL HEAT ENGINE
# =========================================================

def _paired_heat(value, default=50.0):
    """Return a 0-100 buyer-side heat value."""
    try:
        return clamp(float(value))
    except Exception:
        return default


def build_market_heat(
    signal,
    buy_sell,
    order_book,
    power_battle,
    shift_detector,
    price,
    target
):
    """
    Build a rolling BUYER-vs-SELLER control heat score.

    Components:
      30% executed trade pressure
      30% visible order-book liquidity
      20% momentum/structure
      10% price vs Kalshi strike
      10% rapid battle shift

    The engine intentionally measures *control* rather than claiming it can
    know the future.  The rolling history makes acceleration visible.
    """
    execution = _paired_heat(buy_sell.get("buy_pct")) if buy_sell.get("available") else None
    liquidity = _paired_heat(order_book.get("bid_pct")) if order_book.get("available") else None

    momentum = _signed_number((power_battle.get("bullish") or {}).get("momentum_power"))
    momentum = _paired_heat(momentum) if momentum is not None else None

    structure = str(signal.get("structure") or "")
    if structure.startswith("HIGHER"):
        structure_heat = 75.0
    elif structure.startswith("LOWER"):
        structure_heat = 25.0
    else:
        structure_heat = 50.0

    if price is not None and target is not None:
        try:
            if price > target:
                strike_heat = 70.0
            elif price < target:
                strike_heat = 30.0
            else:
                strike_heat = 50.0
        except Exception:
            strike_heat = 50.0
    else:
        strike_heat = 50.0

    # Recent heat gives us a short-term acceleration measurement.
    with heat_lock:
        recent = heat_history[-8:]
        previous_heat = heat_history[-1] if heat_history else None

    base_parts = []
    weights = []
    for value, weight in (
        (execution, 0.30),
        (liquidity, 0.30),
        (momentum, 0.20),
        (strike_heat, 0.10),
        (structure_heat, 0.10),
    ):
        if value is not None:
            base_parts.append(value * weight)
            weights.append(weight)

    if not base_parts:
        buyer_heat = 50.0
    else:
        buyer_heat = sum(base_parts) / sum(weights)

    # Rapid-shift contribution is deliberately small so one noisy snapshot
    # cannot overpower actual executed flow and liquidity.
    shift_direction = shift_detector.get("direction", "WAIT")
    shift_score = _signed_number(shift_detector.get("score")) or 0.0
    if shift_direction == "BULL":
        buyer_heat += min(7.0, abs(shift_score) * 0.7)
    elif shift_direction == "BEAR":
        buyer_heat -= min(7.0, abs(shift_score) * 0.7)

    buyer_heat = clamp(buyer_heat)
    seller_heat = clamp(100.0 - buyer_heat)

    # Compare with the short rolling baseline.
    baseline = None
    if recent:
        vals = [x.get("buyer_heat") for x in recent if x.get("buyer_heat") is not None]
        if vals:
            baseline = sum(vals) / len(vals)

    acceleration = 0.0 if baseline is None else buyer_heat - baseline
    instant_change = 0.0
    if previous_heat and previous_heat.get("buyer_heat") is not None:
        instant_change = buyer_heat - previous_heat.get("buyer_heat")

    if buyer_heat >= 62 and buyer_heat > seller_heat:
        winner = "BUYERS"
    elif seller_heat >= 62 and seller_heat > buyer_heat:
        winner = "SELLERS"
    else:
        winner = "BALANCED"

    if winner == "BUYERS":
        label = "🔥 BUYERS IN CONTROL"
    elif winner == "SELLERS":
        label = "🔥 SELLERS IN CONTROL"
    else:
        label = "🟡 HEAT BALANCED"

    if acceleration >= 5:
        acceleration_label = "BUYER HEAT ACCELERATING"
        acceleration_direction = "BULL"
    elif acceleration <= -5:
        acceleration_label = "SELLER HEAT ACCELERATING"
        acceleration_direction = "BEAR"
    else:
        acceleration_label = "CONTROL STABLE"
        acceleration_direction = "WAIT"

    spread = buyer_heat - seller_heat
    heat_confidence = round(clamp(50.0 + abs(spread) * 1.15, 50.0, 96.0))

    early_edge = False
    if winner == "BUYERS" and acceleration >= 5:
        early_edge = True
    elif winner == "SELLERS" and acceleration <= -5:
        early_edge = True

    snapshot = {
        "time": time.time(),
        "buyer_heat": round(buyer_heat, 1),
        "seller_heat": round(seller_heat, 1),
        "winner": winner,
        "acceleration": round(acceleration, 1),
        "instant_change": round(instant_change, 1),
    }

    with heat_lock:
        heat_history.append(snapshot)
        if len(heat_history) > HEAT_HISTORY_MAX:
            del heat_history[:-HEAT_HISTORY_MAX]
        history_for_ui = list(heat_history[-24:])

    return {
        "available": bool(base_parts),
        "buyer_heat": round(buyer_heat, 1),
        "seller_heat": round(seller_heat, 1),
        "winner": winner,
        "label": label,
        "heat_confidence": heat_confidence,
        "acceleration": round(acceleration, 1),
        "instant_change": round(instant_change, 1),
        "acceleration_label": acceleration_label,
        "acceleration_direction": acceleration_direction,
        "early_edge": early_edge,
        "execution_heat": round(execution, 1) if execution is not None else None,
        "liquidity_heat": round(liquidity, 1) if liquidity is not None else None,
        "momentum_heat": round(momentum, 1) if momentum is not None else None,
        "strike_heat": round(strike_heat, 1),
        "structure_heat": round(structure_heat, 1),
        "history": history_for_ui,
        "note": "Control heat combines flow and liquidity; it is not a guarantee of the final result.",
    }


# =========================================================
# STATEFUL 15-MINUTE WINNER FORECAST
# =========================================================

def build_winner_forecast(
    signal,
    market,
    buy_sell,
    order_book,
    power_battle,
    shift_detector,
    market_heat,
    price,
    target,
):
    """
    Forecast the likely SETTLEMENT direction, not the latest candle.

    The engine deliberately uses a stateful trajectory:
      1. Build a composite state from the existing independent signals.
      2. Store the state through time.
      3. Compare recent state with the preceding state (trajectory).
      4. Require persistence before declaring a direction.
      5. Use hysteresis so a single noisy update cannot flip the forecast.
      6. Allow a reversal only after sustained opposing evidence.

    This is a forecast, not a guarantee of the final Kalshi outcome.
    """
    global forecast_state

    now = time.time()
    ticker = market.get("ticker") if market else None
    close_ts = None
    if market:
        close_dt = parse_time(market.get("close_time"))
        if close_dt:
            close_ts = close_dt.timestamp()
    remaining = max(0.0, close_ts - now) if close_ts else None

    # A new KXBTC15M contract starts a new forecasting episode.
    with forecast_lock:
        if ticker != forecast_state.get("ticker"):
            forecast_history.clear()
            forecast_state = {
                "ticker": ticker,
                "direction": "WAIT",
                "probability": 50,
                "locked": False,
                "locked_at": None,
                "last_change": now,
                "reason": "Building a fresh market trajectory...",
                "opposite_samples": 0,
            }

    if not market or price is None or target is None:
        return {
            "available": False,
            "direction": "WAIT",
            "label": "⚪ BUILDING FORECAST",
            "probability": 50,
            "locked": False,
            "persistence": 0,
            "trajectory": 0.0,
            "current_edge": 0.0,
            "time_remaining": remaining,
            "reason": "Waiting for enough live market data.",
        }

    def centered(value):
        try:
            return max(-1.0, min(1.0, (float(value) - 50.0) / 50.0))
        except Exception:
            return 0.0

    # Existing signals are inputs; the forecast is based on their evolution.
    heat_edge = centered(market_heat.get("buyer_heat", 50.0))
    flow_edge = centered(buy_sell.get("buy_pct", 50.0)) if buy_sell.get("available") else 0.0
    book_edge = centered(order_book.get("bid_pct", 50.0)) if order_book.get("available") else 0.0

    battle_edge = 0.0
    try:
        bull = power_battle.get("bullish") or {}
        bear = power_battle.get("bearish") or {}
        bp = float(bull.get("buying_power", 50.0))
        bm = float(bull.get("momentum_power", 50.0))
        bs = float(bear.get("selling_power", 50.0))
        bsm = float(bear.get("momentum_power", 50.0))
        battle_edge = max(-1.0, min(1.0, ((bp + bm) - (bs + bsm)) / 200.0))
    except Exception:
        pass

    momentum_values = [signal.get("m1"), signal.get("m5"), signal.get("m15")]
    momentum_values = [float(x) for x in momentum_values if isinstance(x, (int, float))]
    momentum_edge = max(-1.0, min(1.0, sum(momentum_values) / 0.15)) if momentum_values else 0.0

    signal_edge = max(-1.0, min(1.0, float(signal.get("score", 0)) / 10.0))
    distance_edge = 1.0 if price > target else -1.0 if price < target else 0.0
    shift_edge = 0.0
    if shift_detector.get("direction") == "BULL":
        shift_edge = min(1.0, abs(float(shift_detector.get("score") or 0)) / 10.0)
    elif shift_detector.get("direction") == "BEAR":
        shift_edge = -min(1.0, abs(float(shift_detector.get("score") or 0)) / 10.0)

    current_edge = (
        heat_edge * 0.24
        + flow_edge * 0.20
        + book_edge * 0.14
        + battle_edge * 0.14
        + momentum_edge * 0.10
        + signal_edge * 0.08
        + distance_edge * 0.06
        + shift_edge * 0.04
    )

    with forecast_lock:
        forecast_history.append({
            "time": now,
            "edge": current_edge,
            "heat": heat_edge,
            "flow": flow_edge,
            "book": book_edge,
            "battle": battle_edge,
            "price": price,
        })
        if len(forecast_history) > FORECAST_HISTORY_MAX:
            del forecast_history[:-FORECAST_HISTORY_MAX]
        hist = list(forecast_history)

    # Compare the current regime with the immediately preceding regime.
    # This is what stops the engine from simply echoing the newest candle.
    recent = hist[-8:]
    prior = hist[-16:-8]
    recent_avg = sum(x["edge"] for x in recent) / len(recent)
    prior_avg = sum(x["edge"] for x in prior) / len(prior) if prior else recent_avg
    trajectory = recent_avg - prior_avg

    bull_samples = sum(1 for x in recent if x["edge"] > 0.06)
    bear_samples = sum(1 for x in recent if x["edge"] < -0.06)
    persistence = max(bull_samples, bear_samples)
    candidate = "UP" if bull_samples > bear_samples else "DOWN" if bear_samples > bull_samples else "WAIT"

    # Convert evidence + trajectory into a probability-like score.
    strength = abs(current_edge) * 100.0
    trend_bonus = min(18.0, abs(trajectory) * 180.0)
    persistence_bonus = min(15.0, max(0, persistence - 3) * 3.0)
    raw_conf = 50.0 + strength * 0.34 + trend_bonus + persistence_bonus
    probability = int(round(max(50.0, min(96.0, raw_conf))))

    # A forecast is not allowed to lock immediately. It needs both direction
    # and persistence, then remains sticky until a genuine sustained reversal.
    with forecast_lock:
        old_direction = forecast_state.get("direction", "WAIT")
        old_locked = bool(forecast_state.get("locked"))
        opposite_samples = int(forecast_state.get("opposite_samples", 0))

        if candidate in ("UP", "DOWN") and persistence >= 5 and abs(current_edge) >= 0.12:
            if old_locked and candidate != old_direction:
                opposite_samples += 1
                forecast_state["opposite_samples"] = opposite_samples
                # Require sustained opposing evidence rather than one update.
                if opposite_samples >= 15 and abs(trajectory) >= 0.015:
                    forecast_state["direction"] = candidate
                    forecast_state["probability"] = probability
                    forecast_state["locked"] = True
                    forecast_state["locked_at"] = now
                    forecast_state["last_change"] = now
                    forecast_state["opposite_samples"] = 0
            else:
                forecast_state["direction"] = candidate
                forecast_state["probability"] = probability
                if persistence >= 8 and probability >= 65:
                    forecast_state["locked"] = True
                    if not forecast_state.get("locked_at"):
                        forecast_state["locked_at"] = now
                forecast_state["last_change"] = now if candidate != old_direction else forecast_state.get("last_change", now)
                forecast_state["opposite_samples"] = 0
        else:
            # Weak/noisy evidence never flips an existing locked forecast.
            if not old_locked:
                forecast_state["direction"] = "WAIT"
                forecast_state["probability"] = 50

        direction = forecast_state.get("direction", "WAIT")
        locked = bool(forecast_state.get("locked"))
        final_probability = int(forecast_state.get("probability", 50))

    if direction == "UP":
        label = "🟢 PROJECTED WINNER: UP"
    elif direction == "DOWN":
        label = "🔴 PROJECTED WINNER: DOWN"
    else:
        label = "⚪ NO CLEAR WINNER"

    if locked:
        reason = (
            f"Forecast held by {persistence}/8 recent samples; "
            f"trajectory {trajectory:+.3f}. Opposing evidence must persist before reversal."
        )
    elif persistence < 5:
        reason = "Building persistence; not enough agreement to force a settlement forecast."
    else:
        reason = f"Trajectory {trajectory:+.3f}; waiting for stronger persistence before locking."

    return {
        "available": True,
        "direction": direction,
        "label": label,
        "probability": final_probability,
        "locked": locked,
        "persistence": persistence,
        "trajectory": round(trajectory, 4),
        "current_edge": round(current_edge, 4),
        "time_remaining": remaining,
        "opposite_samples": int(forecast_state.get("opposite_samples", 0)),
        "samples": len(hist),
        "reason": reason,
        "method": "Stateful trajectory + persistence + reversal hysteresis",
    }


# =========================================================
# COLLECT STATE
# =========================================================


# =========================================================
# INDEPENDENT 15-MINUTE STATISTICAL MODEL
# =========================================================

def normal_cdf(x):
    try:
        return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))
    except Exception:
        return 0.5


def statistical_15m_model(price, target, candles, close_time):
    """Estimate P(final BTC price >= Kalshi strike) from recent 1m returns.

    This is deliberately independent of the existing signal/heat engines.
    It is a statistical estimate, not a guarantee and not a broker signal.
    """
    now = time.time()
    if price is None or target is None:
        return {"direction":"WAIT", "probability":50, "confidence":0,
                "reason":"Waiting for live BTC price and Kalshi strike.", "samples":0}

    try:
        remaining = max(0.0, parse_time(close_time).timestamp() - now) if close_time else 900.0
    except Exception:
        remaining = 900.0

    closes = [float(x[1]) for x in (candles or []) if isinstance(x, (list, tuple)) and len(x) >= 2 and number(x[1]) is not None]
    if len(closes) < 8:
        return {"direction":"WAIT", "probability":50, "confidence":0,
                "reason":"Not enough 1-minute history for the statistical model.", "samples":len(closes),
                "time_remaining":remaining}

    returns = []
    for a,b in zip(closes[:-1], closes[1:]):
        if a > 0 and b > 0:
            returns.append(math.log(b/a))
    if len(returns) < 6:
        return {"direction":"WAIT", "probability":50, "confidence":0,
                "reason":"Not enough valid returns for the statistical model.", "samples":len(returns),
                "time_remaining":remaining}

    # Recent-return volatility, annualization is intentionally avoided.
    vol_1m = statistics.pstdev(returns[-20:]) if len(returns[-20:]) > 1 else 0.0
    recent_mean = statistics.mean(returns[-5:]) if returns[-5:] else 0.0
    medium_mean = statistics.mean(returns[-15:]) if returns[-15:] else recent_mean

    # Blend recent drift with a weaker medium-term drift and shrink aggressively
    # so one unusually large candle cannot dominate the 15-minute estimate.
    drift_1m = max(-0.0008, min(0.0008, 0.65*recent_mean + 0.35*medium_mean))
    minutes = max(0.25, min(15.0, remaining / 60.0))
    sigma = max(vol_1m * math.sqrt(minutes), 0.00025)

    # Mean-reverting drift when the strike is far away, which keeps the model
    # from becoming unrealistically certain during noisy periods.
    distance = (float(price) - float(target)) / float(target)
    distance = max(-0.02, min(0.02, distance))
    expected_log_return = drift_1m * minutes
    z = (math.log(float(price)/float(target)) + expected_log_return) / sigma
    p_up = normal_cdf(z)

    # Uncertainty penalty for very short history and very high volatility.
    history_factor = min(1.0, len(returns) / 20.0)
    vol_penalty = min(0.35, max(0.0, vol_1m / 0.003) * 0.10)
    p_up = 0.5 + (p_up - 0.5) * history_factor * (1.0 - vol_penalty)
    p_up = max(0.05, min(0.95, p_up))

    # p_up is always the probability that BTC finishes AT/ABOVE the strike.
    # The dashboard, however, should display the probability of the SELECTED
    # direction.  Otherwise a DOWN call could incorrectly show a low number
    # such as 11%, even though 89% of the model probability is DOWN.
    up_probability = int(round(p_up * 100))
    up_probability = max(5, min(95, up_probability))
    edge = abs(up_probability - 50)
    confidence = int(round(min(95, edge * 2.2)))

    # Near expiry, require a meaningful edge instead of forcing a direction.
    if remaining <= 60 and edge < 12:
        direction = "WAIT"
    elif up_probability >= 55:
        direction = "UP"
    elif up_probability <= 45:
        direction = "DOWN"
    else:
        direction = "WAIT"

    directional_probability = (
        up_probability if direction == "UP"
        else 100 - up_probability if direction == "DOWN"
        else 50
    )

    reason = (
        f"Strike distance {distance*100:+.3f}% • {minutes:.1f}m remaining • "
        f"1m volatility {vol_1m*100:.3f}% • drift {drift_1m*100:+.3f}%/m"
    )

    return {
        "direction": direction,
        "probability": directional_probability,
        "up_probability": up_probability,
        "confidence": confidence,
        "reason": reason,
        "samples": len(returns),
        "time_remaining": round(remaining, 1),
        "strike_distance_pct": round(distance * 100, 4),
        "volatility_1m_pct": round(vol_1m * 100, 4),
        "drift_1m_pct": round(drift_1m * 100, 4),
        "z_score": round(z, 3),
    }


def ensemble_15m_forecast(stat_model, winner_forecast, signal, market_heat, quality_score):
    """Combine model families without treating a rule-strength score as a probability.

    IMPORTANT: displayed probabilities are still heuristic estimates until the
    app has enough clean, independently resolved outcomes to calibrate them.
    """
    models = []

    # Statistical model: use its directional probability only when it has
    # enough observations to be meaningful.
    if (stat_model and stat_model.get("direction") in ("UP", "DOWN")
            and int(stat_model.get("samples", 0) or 0) >= 6):
        try:
            p = max(50.0, min(95.0, float(stat_model.get("probability", 50))))
            models.append({"name": "statistical", "direction": stat_model["direction"],
                           "probability": p, "weight": 0.45})
        except (TypeError, ValueError):
            pass

    # Stateful trajectory forecast.
    if winner_forecast and winner_forecast.get("direction") in ("UP", "DOWN"):
        try:
            p = max(50.0, min(95.0, float(winner_forecast.get("probability", 50))))
            models.append({"name": "trajectory", "direction": winner_forecast["direction"],
                           "probability": p, "weight": 0.35})
        except (TypeError, ValueError):
            pass

    # FIX: signal['confidence'] is a rule-strength score, NOT a probability.
    # Convert the signed signal score into a conservative heuristic strength.
    # Only include it if its sign agrees with the signal's stated direction.
    if signal and signal.get("verdict") in ("UP", "DOWN"):
        try:
            signed_score = float(signal.get("score", 0) or 0)
            direction = signal["verdict"]
            sign_matches = (signed_score > 0 if direction == "UP" else signed_score < 0)
            if sign_matches and abs(signed_score) > 0:
                p = 50.0 + min(20.0, abs(signed_score) * 2.0)
                models.append({"name": "price_signal", "direction": direction,
                               "probability": p, "weight": 0.20})
        except (TypeError, ValueError):
            pass

    if not models:
        return {"direction": "WAIT", "probability": 50, "confidence": 0, "agreement": 0,
                "reason": "No independent model has enough evidence yet.", "models_used": 0,
                "probability_type": "HEURISTIC — NOT CALIBRATED"}

    up_weight = down_weight = 0.0
    votes = {"UP": [], "DOWN": []}
    for model in models:
        # Strength is distance from neutral, never the raw displayed confidence.
        strength = min(0.50, abs(model["probability"] - 50.0) / 50.0)
        contribution = model["weight"] * strength
        if model["direction"] == "UP":
            up_weight += contribution
        else:
            down_weight += contribution
        votes[model["direction"]].append(model["name"])

    total = up_weight + down_weight
    agreement = int(round(100 * max(up_weight, down_weight) / total)) if total else 0
    direction = "UP" if up_weight > down_weight else "DOWN" if down_weight > up_weight else "WAIT"

    raw_up_probability = 50.0 + (up_weight - down_weight) * 50.0
    raw_up_probability = max(5.0, min(95.0, raw_up_probability))
    directional_probability = (raw_up_probability if direction == "UP" else
                                100.0 - raw_up_probability if direction == "DOWN" else 50.0)
    probability = int(round(directional_probability))
    quality = max(0.0, min(100.0, float(quality_score or 0)))
    quality_factor = max(0.0, min(1.0, quality / 100.0))
    confidence = int(round(min(85.0, abs(raw_up_probability - 50.0) * 2.0 * quality_factor)))

    stat_dir = stat_model.get("direction") if stat_model else "WAIT"
    winner_dir = winner_forecast.get("direction") if winner_forecast else "WAIT"
    conflict = (stat_dir in ("UP", "DOWN") and winner_dir in ("UP", "DOWN")
                and stat_dir != winner_dir)

    if quality < 45:
        direction, probability, confidence = "WAIT", 50, min(confidence, 20)
        reason = "Data quality is too low; do not force a directional call."
    elif conflict:
        direction, probability, confidence = "WAIT", 50, min(confidence, 35)
        reason = "Statistical and trajectory models disagree — waiting for confirmation."
    elif confidence < 20 or agreement < 58:
        direction, probability = "WAIT", 50
        reason = "Evidence is too weak or balanced for a clean 15-minute call."
    else:
        reason = f"{len(models)} model inputs evaluated; {agreement}% of weighted directional evidence favors the leading side."

    return {
        "direction": direction,
        "probability": probability,
        "confidence": confidence,
        "agreement": agreement,
        "reason": reason,
        "models_used": len(models),
        "model_votes": {"UP": votes["UP"], "DOWN": votes["DOWN"]},
        "statistical": stat_dir,
        "trajectory": winner_dir,
        "signal": signal.get("verdict", "WAIT") if signal else "WAIT",
        "heat": (market_heat or {}).get("label", "") if isinstance(market_heat, dict) else "",
        "data_quality": round(quality),
        "probability_type": "HEURISTIC — NOT CALIBRATED",
        "warning": "This is not a verified win probability. Validate against completed market outcomes before relying on it.",
    }


# =========================================================
# ADAPTIVE MODE — EARLY WARNING / CONFIRMATION / REVERSAL / WAIT
# =========================================================

def build_adaptive_mode(signal, ensemble, winner_forecast, power_battle,
                        shift_detector, market_heat, quality, countdown,
                        price, target):
    """Rule-based adaptive state. This is a decision aid, not a guarantee."""
    seconds = countdown if isinstance(countdown, (int, float)) else None
    seconds = max(0, seconds) if seconds is not None else None

    def direction(value):
        value = str(value or "").upper()
        if value in ("UP", "BULL", "BULLS"):
            return "UP"
        if value in ("DOWN", "BEAR", "BEARS"):
            return "DOWN"
        return "WAIT"

    evidence = {"UP": [], "DOWN": []}

    def add(group, side, detail):
        if side in evidence and detail:
            # One vote per evidence group avoids counting repeated labels twice.
            existing = [x for x in evidence[side] if x.startswith(group + ":")]
            if not existing:
                evidence[side].append(group + ": " + detail)

    side = direction((ensemble or {}).get("direction"))
    if side != "WAIT":
        add("ensemble", side, "ensemble leans " + side)

    side = direction((winner_forecast or {}).get("direction"))
    if side != "WAIT":
        add("trajectory", side, "15-minute trajectory leans " + side)

    side = direction((signal or {}).get("verdict"))
    if side != "WAIT":
        add("price", side, "price/momentum signal leans " + side)

    side = direction((power_battle or {}).get("winner"))
    if side != "WAIT":
        add("pressure", side, "buyer/seller power favors " + side)

    heat_side = direction((market_heat or {}).get("direction"))
    if heat_side == "WAIT":
        heat_side = direction((market_heat or {}).get("winner"))
    if heat_side != "WAIT":
        add("heat", heat_side, "market-control heat favors " + heat_side)

    up_count, down_count = len(evidence["UP"]), len(evidence["DOWN"])
    total = up_count + down_count
    conflict = up_count > 0 and down_count > 0
    leading = "UP" if up_count > down_count else "DOWN" if down_count > up_count else "WAIT"
    lead_count = max(up_count, down_count)
    opposite = "DOWN" if leading == "UP" else "UP" if leading == "DOWN" else "WAIT"

    shift_side = direction((shift_detector or {}).get("direction"))
    shift_signals = (shift_detector or {}).get("signals") or []
    shift_is_opposing = leading != "WAIT" and shift_side == opposite and len(shift_signals) >= 2
    reversal = (signal or {}).get("reversal") == "HIGH" or shift_is_opposing

    # Distance from strike is context, not a direction vote.
    distance_pct = None
    try:
        if price is not None and target not in (None, 0):
            distance_pct = (float(price) - float(target)) / float(target) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        pass

    if quality < 50 or total == 0 or leading == "WAIT":
        stage = "WAIT"
        label = "🟡 WAIT — BUILDING / CONFLICTING DATA"
        detail = "Not enough aligned evidence to select a direction safely."
        chosen = "WAIT"
    elif reversal:
        stage = "REVERSAL WARNING"
        label = "🟠 REVERSAL WARNING"
        detail = "The current directional read may be weakening; wait for the shift to settle."
        chosen = "WAIT"
    elif quality >= 60 and lead_count >= 3 and not conflict:
        stage = "CONFIRMATION"
        label = ("🟢 CONFIRMED UP BIAS" if leading == "UP" else "🔴 CONFIRMED DOWN BIAS")
        detail = "At least three distinct evidence groups align and data quality is adequate."
        chosen = leading
    elif lead_count >= 1:
        stage = "EARLY WARNING"
        label = ("🟢 EARLY UP WARNING" if leading == "UP" else "🔴 EARLY DOWN WARNING")
        detail = "Directional evidence is emerging, but confirmation is not strong enough yet."
        chosen = leading if not conflict and quality >= 50 else "WAIT"
    else:
        stage = "WAIT"
        label = "🟡 WAIT — NO CLEAR EDGE"
        detail = "The signals are balanced or contradictory."
        chosen = "WAIT"

    # Near settlement, require stronger agreement; the final seconds can be noisy.
    if seconds is not None and seconds <= 60 and chosen != "WAIT":
        if quality < 70 or lead_count < 3 or conflict:
            stage = "WAIT"
            label = "🟡 WAIT — LATE-WINDOW FILTER"
            detail = "Late-window entry filter: agreement or data quality is insufficient."
            chosen = "WAIT"

    if conflict and chosen != "WAIT":
        detail += " Opposing evidence is present, so confidence should be treated cautiously."

    confidence = min(85, 50 + abs(up_count - down_count) * 8 + max(0, quality - 60) // 5)
    if chosen == "WAIT":
        confidence = min(confidence, 55)
    if quality < 60:
        confidence = min(confidence, 55)

    return {
        "mode": "ADAPTIVE",
        "stage": stage,
        "label": label,
        "direction": chosen,
        "leading_direction": leading,
        "confidence_score": int(confidence),
        "up_evidence": evidence["UP"],
        "down_evidence": evidence["DOWN"],
        "up_groups": up_count,
        "down_groups": down_count,
        "conflict": conflict,
        "reversal_risk": bool(reversal),
        "quality": quality,
        "seconds_remaining": seconds,
        "distance_from_strike_pct": round(distance_pct, 5) if distance_pct is not None else None,
        "detail": detail,
        "disclaimer": "Rule-based signal, not a calibrated probability or guarantee."
    }



# =========================================================
# CROSS-EXCHANGE BENCHMARK PROXY (NOT OFFICIAL CF BENCHMARKS BRTI)
# Uses public spot order books as a transparent proxy.  Official BRTI is
# a separate benchmark feed; never label this proxy as the official index.
# =========================================================

benchmark_proxy_cache = {"at": 0.0, "data": None}
benchmark_proxy_lock = threading.Lock()
benchmark_proxy_history = []


def build_benchmark_proxy():
    """Estimate cross-exchange midprice and near-book pressure from public books."""
    now = time.time()
    with benchmark_proxy_lock:
        if benchmark_proxy_cache["data"] is not None and now - benchmark_proxy_cache["at"] < 2.0:
            return dict(benchmark_proxy_cache["data"])

    endpoints = [
        ("Coinbase", "https://api.exchange.coinbase.com/products/BTC-USD/book", {"level": 2}),
        ("Kraken", "https://api.kraken.com/0/public/Depth", {"pair": "XBTUSD", "count": 20}),
        ("Bitstamp", "https://www.bitstamp.net/api/v2/order_book/btcusd/", {"limit": 20}),
    ]
    rows = []
    errors = []
    for name, url, params in endpoints:
        data = get_json(url, params)
        try:
            if name == "Kraken":
                result = data.get("result", {})
                book = result[next(iter(result))]
                bids, asks = book.get("bids", []), book.get("asks", [])
            else:
                bids, asks = data.get("bids", []), data.get("asks", [])
            # Normalize exchange rows into (price, BTC quantity).
            bids = [(float(x[0]), float(x[1])) for x in bids[:20] if len(x) >= 2 and float(x[0]) > 0 and float(x[1]) > 0]
            asks = [(float(x[0]), float(x[1])) for x in asks[:20] if len(x) >= 2 and float(x[0]) > 0 and float(x[1]) > 0]
            if not bids or not asks:
                raise ValueError("empty order book")
            best_bid, best_ask = bids[0][0], asks[0][0]
            if best_bid <= 0 or best_ask <= best_bid:
                raise ValueError("invalid spread")
            mid = (best_bid + best_ask) / 2.0
            # USD notional, weighted toward nearer levels to avoid distant spoof-like depth.
            bid_usd = sum(px * qty / (1 + i * 0.12) for i, (px, qty) in enumerate(bids))
            ask_usd = sum(px * qty / (1 + i * 0.12) for i, (px, qty) in enumerate(asks))
            imbalance = (bid_usd - ask_usd) / max(1.0, bid_usd + ask_usd)
            rows.append({"exchange": name, "mid": mid, "bid": best_bid, "ask": best_ask,
                         "spread_bps": (best_ask - best_bid) / mid * 10000,
                         "bid_usd": bid_usd, "ask_usd": ask_usd,
                         "imbalance": imbalance})
        except Exception as exc:
            errors.append(name + ": unavailable/invalid book")

    if rows:
        mids = [x["mid"] for x in rows]
        proxy = statistics.median(mids)
        # Exclude an exchange whose mid deviates materially from the cross-exchange median.
        good = [x for x in rows if abs(x["mid"] - proxy) / proxy <= 0.002]
        if good:
            proxy = statistics.median([x["mid"] for x in good])
            rows = good
        imbalance = statistics.mean(x["imbalance"] for x in rows)
        spread = statistics.mean(x["spread_bps"] for x in rows)
        benchmark_proxy_history.append({"t": now, "price": proxy, "imbalance": imbalance})
        del benchmark_proxy_history[:-90]
        recent = [x for x in benchmark_proxy_history if now - x["t"] <= 15]
        move_bps = ((proxy / recent[0]["price"] - 1) * 10000) if len(recent) >= 2 and recent[0]["price"] else 0.0
        # Pressure only counts as control when price response is aligned.
        book_side = "UP" if imbalance >= 0.08 else "DOWN" if imbalance <= -0.08 else "NEUTRAL"
        price_side = "UP" if move_bps >= 1.0 else "DOWN" if move_bps <= -1.0 else "FLAT"
        if book_side == price_side and book_side in ("UP", "DOWN"):
            control = book_side
            status = "ALIGNED"
        elif book_side in ("UP", "DOWN") and price_side in ("UP", "DOWN") and book_side != price_side:
            control = "CONTESTED"
            status = "ABSORPTION / DIVERGENCE"
        elif price_side in ("UP", "DOWN") and (book_side == "NEUTRAL" or book_side == price_side):
            control = price_side
            status = "PRICE-LED"
        else:
            control = "WAIT"
            status = "NO CLEAR CONTROL"
        result = {
            "available": True, "label": "CROSS-EXCHANGE BOOK PROXY",
            "official_brtI": False, "price": round(proxy, 2),
            "exchange_count": len(rows), "exchanges": rows,
            "book_imbalance_pct": round(imbalance * 100, 1),
            "price_move_bps_15s": round(move_bps, 2),
            "book_side": book_side, "price_side": price_side,
            "control": control, "status": status,
            "avg_spread_bps": round(spread, 2), "updated_at": now,
            "note": "Public-exchange proxy, not the official CF Benchmarks BRTI settlement feed."
        }
    else:
        result = {"available": False, "label": "CROSS-EXCHANGE BOOK PROXY", "official_brtI": False,
                  "control": "WAIT", "status": "BOOK DATA UNAVAILABLE", "exchanges": [],
                  "note": "Could not read enough public exchange order books."}
    with benchmark_proxy_lock:
        benchmark_proxy_cache["at"] = now
        benchmark_proxy_cache["data"] = result
    return dict(result)


def build_control_read(proxy, buy_sell, order_book, signal, adaptive):
    """Quality-gated control read: require fresh, multi-source evidence and avoid treating a score as probability."""
    if not isinstance(proxy, dict) or not proxy.get("available"):
        return {"direction": "WAIT", "status": "INSUFFICIENT DATA", "score": 0,
                "detail": "Cross-exchange order-book proxy is unavailable."}

    now = time.time()
    age = max(0.0, now - float(proxy.get("updated_at", now) or now))
    exchange_count = int(proxy.get("exchange_count", 0) or 0)
    spread = proxy.get("avg_spread_bps")
    if age > 12:
        return {"direction": "WAIT", "status": "STALE DATA", "score": 0,
                "detail": f"Cross-exchange data is {age:.1f}s old. Waiting for fresh books."}
    if exchange_count < 2:
        return {"direction": "WAIT", "status": "TOO FEW EXCHANGES", "score": 0,
                "detail": "At least two valid exchanges are required; one exchange is not enough to confirm control."}
    if spread is not None and float(spread) > 20:
        return {"direction": "WAIT", "status": "WIDE SPREAD / LOW QUALITY", "score": 0,
                "detail": f"Average spread is {float(spread):.1f} bps. Thin or unstable books can distort pressure readings."}

    votes = {"UP": 0.0, "DOWN": 0.0}
    sources = {"UP": set(), "DOWN": set()}
    details = []

    def add_vote(side, weight, source, label):
        # Normalize each feed's native vocabulary into the same UP/DOWN
        # direction before voting. Without this mapping, BUYERS/SELLERS and
        # BIDS/ASKS were silently discarded because only UP/DOWN were accepted.
        raw_side = str(side or "WAIT").strip().upper()
        aliases = {
            "BUYERS": "UP", "BUY": "UP", "BIDS": "UP", "BID": "UP",
            "BULL": "UP", "BULLISH": "UP", "LONG": "UP",
            "SELLERS": "DOWN", "SELL": "DOWN", "ASKS": "DOWN", "ASK": "DOWN",
            "BEAR": "DOWN", "BEARISH": "DOWN", "SHORT": "DOWN",
            "NEUTRAL": "WAIT", "FLAT": "WAIT", "CONTESTED": "WAIT",
        }
        side = aliases.get(raw_side, raw_side)
        if side in votes:
            votes[side] += weight
            sources[side].add(source)
            details.append(label + ": " + side + (" (" + raw_side + ")" if raw_side != side else ""))

    bside = proxy.get("book_side")
    pside = proxy.get("price_side")
    add_vote(bside, 1.0, "cross_book", "Cross-exchange book pressure")
    add_vote(pside, 2.0, "price_response", "Observed cross-exchange price response")

    flow = str((buy_sell or {}).get("winner", "WAIT")).upper()
    add_vote(flow, 1.25, "trade_flow", "Executed trade flow")
    local_book = str((order_book or {}).get("winner", "WAIT")).upper()
    add_vote(local_book, 0.5, "local_book", "Primary-exchange book")
    sig = str((signal or {}).get("verdict", "WAIT")).upper()
    add_vote(sig, 0.75, "momentum", "Price/momentum signal")

    if proxy.get("status") == "ABSORPTION / DIVERGENCE":
        return {"direction": "WAIT", "status": "CONTESTED — PRICE DISAGREES WITH PRESSURE", "score": 0,
                "up_score": round(votes["UP"], 2), "down_score": round(votes["DOWN"], 2),
                "detail": "Order-book pressure and price movement disagree. Treat this as a possible absorption/reversal zone, not a confirmed trade. " + "; ".join(details)}

    diff = votes["UP"] - votes["DOWN"]
    direction = "UP" if diff > 0 else "DOWN" if diff < 0 else "WAIT"
    opposing = "DOWN" if direction == "UP" else "UP" if direction == "DOWN" else "WAIT"
    total = max(1.0, votes["UP"] + votes["DOWN"])
    margin = abs(diff) / total

    # A directional call needs price response plus at least one independent confirmation.
    has_price = "price_response" in sources[direction] if direction in votes else False
    independent_confirmation = len(sources[direction] - {"price_response", "cross_book"}) if direction in votes else 0
    if direction == "WAIT" or abs(diff) < 1.25 or not has_price or independent_confirmation < 1:
        direction = "WAIT"
        status = "WAIT — NEED INDEPENDENT CONFIRMATION"
        score = int(min(59, 40 + margin * 30))
        detail = "Price response plus independent trade-flow/momentum confirmation is required before calling control."
    elif votes[opposing] >= votes[direction] * 0.72:
        direction = "WAIT"
        status = "WAIT — OPPOSING EVIDENCE TOO STRONG"
        score = int(min(59, 40 + margin * 25))
        detail = "Signals are too divided for a clean control read."
    else:
        status = "UP CONTROL — CONFIRMED BY MULTIPLE INPUTS" if direction == "UP" else "DOWN CONTROL — CONFIRMED BY MULTIPLE INPUTS"
        # This is a rule-strength score, NOT a win probability.
        score = int(min(88, 45 + margin * 28 + min(3, len(sources[direction])) * 5))
        detail = "Multiple inputs lean the same way; this still does not guarantee the 15-minute settlement."

    return {"direction": direction, "status": status, "score": score,
            "up_score": round(votes["UP"], 2), "down_score": round(votes["DOWN"], 2),
            "source_count": len(sources.get(direction, set())) if direction in votes else 0,
            "data_age_seconds": round(age, 1),
            "detail": detail + (" Evidence: " + "; ".join(details) if details else " Waiting for independent evidence.")}


def collect_state():

    load_memory()

    # Fetch independent market inputs concurrently. Previously each REST source
    # waited for the prior source to finish, which made the dashboard lag when
    # one provider was slow. Each function keeps its existing fallback behavior.
    with ThreadPoolExecutor(max_workers=6, thread_name_prefix="btc-feed") as pool:
        future_price = pool.submit(get_spot_feeds)
        future_candles = pool.submit(get_history)
        future_market = pool.submit(get_kalshi)
        future_proxy = pool.submit(build_benchmark_proxy)
        future_flow = pool.submit(get_buy_sell_pressure)
        future_book = pool.submit(get_order_book_pressure)

        # Resolve all six results. Individual feed functions already catch
        # provider errors; this outer guard keeps one unexpected exception from
        # crashing the complete dashboard refresh.
        try:
            price, feeds = future_price.result()
        except Exception:
            price, feeds = None, {}
        try:
            candles = future_candles.result()
        except Exception:
            candles = []
        try:
            market = future_market.result()
        except Exception:
            market = None
        try:
            benchmark_proxy = future_proxy.result()
        except Exception:
            benchmark_proxy = {"available": False, "control": "WAIT", "status": "PROXY ERROR", "exchanges": [], "official_brtI": False}
        try:
            buy_sell = future_flow.result()
        except Exception:
            buy_sell = {"available": False, "winner": "WAIT", "strength": "FEED ERROR"}
        try:
            order_book = future_book.result()
        except Exception:
            order_book = {"available": False, "winner": "WAIT", "strength": "FEED ERROR"}

    quality_score, quality_grade = (
        calculate_quality(
            feeds,
            market,
            candles
        )
    )

    signal = build_signal(
        price,
        market,
        candles,
        quality_score
    )

    power_battle = build_power_battle(
        signal,
        buy_sell,
        order_book,
        quality_score
    )

    shift_detector = build_shift_detector(
        signal,
        buy_sell,
        order_book,
        power_battle,
        price
    )

    target = (
        market.get(
            "target"
        )
        if market
        else
        None
    )

    distance_pct = None

    if (
        price is not None
        and
        target is not None
    ):

        distance_pct = (
            (
                price -
                target
            )
            /
            target
        ) * 100

    market_heat = build_market_heat(
        signal,
        buy_sell,
        order_book,
        power_battle,
        shift_detector,
        price,
        target
    )

    winner_forecast = build_winner_forecast(
        signal,
        market,
        buy_sell,
        order_book,
        power_battle,
        shift_detector,
        market_heat,
        price,
        target
    )

    if (
        market
        and
        price is not None
    ):

        update_memory(
            market,
            price,
            signal,
            distance_pct
        )

    matches = 0
    rate = None

    if (
        market
        and
        signal[
            "verdict"
        ] in (
            "UP",
            "DOWN"
        )
    ):

        matches, rate = (
            memory_match(
                signal,
                distance_pct
            )
        )

    strength = prediction_strength(
        signal,
        price,
        market,
        buy_sell,
        order_book,
        quality_score,
        rate,
        matches
    )

    stat_model = statistical_15m_model(
        price,
        target,
        candles,
        market.get("close_time") if market else None
    )

    ensemble_forecast = ensemble_15m_forecast(
        stat_model,
        winner_forecast,
        signal,
        market_heat,
        quality_score
    )

    adaptive_mode = build_adaptive_mode(
        signal,
        ensemble_forecast,
        winner_forecast,
        power_battle,
        shift_detector,
        market_heat,
        quality_score,
        seconds_left(market.get("close_time")) if market else None,
        price,
        target
    )

    control_read = build_control_read(
        benchmark_proxy, buy_sell, order_book, signal, adaptive_mode
    )

    close_dt = (
        parse_time(
            market.get(
                "close_time"
            )
        )
        if market
        else
        None
    )

    return {

        "updated":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "refresh_mode": "PARALLEL FEED COLLECTION",

        "btc":
            price,

        "feeds":
            feeds,

        "market":
            market,

        "countdown":
            (
                seconds_left(
                    market.get(
                        "close_time"
                    )
                )
                if market
                else
                None
            ),

        "market_close_ts":
            (
                close_dt.timestamp()
                if close_dt
                else
                None
            ),

        "candles":
            len(candles),

        "candle_history":
            [
                {"time": ts, "price": close}
                for ts, close in candles
            ],

        "signal":
            signal,

        "data_quality":
            {
                "score":
                    quality_score,

                "grade":
                    quality_grade
            },

        "buy_sell":
            buy_sell,

        "order_book":
            order_book,

        "power_battle":
            power_battle,

        "shift_detector":
            shift_detector,

        "market_heat":
            market_heat,

        "winner_forecast":
            winner_forecast,

        "statistical_model":
            stat_model,

        "ensemble_forecast":
            ensemble_forecast,

        "adaptive_mode":
            adaptive_mode,

        "benchmark_proxy":
            benchmark_proxy,

        "control_read":
            control_read,

        "prediction_strength":
            strength,

        "market_maker":
            build_market_maker_read(),

        "latency":
            {

                "btc_age_ms":
                    (
                        round(
                            max(
                                0.0,
                                time.time()
                                -
                                (
                                    live_btc.get("received_at", 0.0)
                                    if active_btc_source == "Binance Live"
                                    else live_coinbase.get("received_at", 0.0)
                                )
                            ) * 1000,
                            1
                        )
                        if active_btc_source in ("Binance Live", "Coinbase Live")
                        else None
                    ),

                "btc_stream":
                    active_btc_source in ("Binance Live", "Coinbase Live"),

                "btc_source": active_btc_source,

                "binance_connected": bool(live_btc.get("connected")),

                "coinbase_connected": bool(live_coinbase.get("connected")),

                "binance_age_ms":
                    (
                        round(max(0.0, time.time() - live_btc.get("received_at", 0.0)) * 1000, 1)
                        if live_btc.get("received_at") else None
                    ),

                "coinbase_age_ms":
                    (
                        round(max(0.0, time.time() - live_coinbase.get("received_at", 0.0)) * 1000, 1)
                        if live_coinbase.get("received_at") else None
                    ),

                "binance_reconnects": live_btc.get("reconnects", 0),
                "coinbase_reconnects": live_coinbase.get("reconnects", 0),

                "kalshi_age_ms":
                    (
                        round(
                            max(0.0, time.time() - kalshi_last_update) * 1000,
                            1
                        )
                        if kalshi_last_update
                        else None
                    )
            },

        "memory":
            {

                "records":
                    len(
                        signal_memory
                    ),

                "matches":
                    matches,

                "rate":
                    rate,

                "up":
                    sum(
                        r.get(
                            "outcome"
                        ) == "UP"
                        for r
                        in signal_memory
                    ),

                "down":
                    sum(
                        r.get(
                            "outcome"
                        ) == "DOWN"
                        for r
                        in signal_memory
                    )
            },

        "performance":
            performance_stats()
    }


# =========================================================
# START BINANCE STREAM
# =========================================================

if websocket is not None:

    threading.Thread(
        target=_binance_stream_loop,
        name="binance-live-stream",
        daemon=True
    ).start()

    threading.Thread(
        target=_coinbase_stream_loop,
        name="coinbase-live-stream",
        daemon=True
    ).start()

    threading.Thread(
        target=_market_microstructure_stream_loop,
        name="binance-market-microstructure",
        daemon=True
    ).start()


# =========================================================
# DASHBOARD
# =========================================================

PAGE = r"""
<!doctype html>

<html>

<head>

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>BTC Strike AI — Heat Engine</title>

<style>

body{
margin:0;
background:#071019;
color:#eef4f8;
font-family:Arial,sans-serif
}

.wrap{
max-width:1150px;
margin:auto;
padding:18px
}

.grid{
display:grid;
grid-template-columns:repeat(4,1fr);
gap:12px
}

.card{
background:#0d1822;
border:1px solid #1c2c38;
border-radius:14px;
padding:16px
}

.wide{
grid-column:1/-1
}

.verdict{
text-align:center;
padding:25px;
border-radius:16px;
margin-bottom:14px
}

.up{
background:#092b1a;
border:1px solid #1b9b5c
}

.down{
background:#321014;
border:1px solid #d44754
}

.wait{
background:#30280b;
border:1px solid #c4a63a
}

.label{
font-size:30px;
font-weight:900
}

.big{
font-size:27px;
font-weight:800;
margin-top:8px
}

.value{
font-size:20px;
font-weight:700;
margin-top:8px
}

.small{
color:#91a3b0;
font-size:13px;
margin-top:5px;
white-space:pre-wrap
}

.ok{
color:#43d184
}

.bad{
color:#ff6570
}

.battle{
display:flex;
justify-content:space-between;
gap:10px;
margin-top:12px;
font-size:16px;
font-weight:800
}

.battleBar{
height:12px;
background:#182632;
border-radius:8px;
overflow:hidden;
margin-top:10px
}

.battleBuy{
height:100%;
background:#1b9b5c
}

.powerGrid{
display:grid;
grid-template-columns:1fr 1fr;
gap:12px;
margin-top:12px
}

.powerSide{
background:#101f2b;
border:1px solid #243847;
border-radius:12px;
padding:14px
}

.powerSide.bull{
border-color:#1b9b5c
}

.powerSide.bear{
border-color:#d44754
}

.powerTitle{
font-size:18px;
font-weight:900
}

.powerValue{
font-size:25px;
font-weight:900;
margin-top:7px
}

.powerBar{
height:10px;
background:#182632;
border-radius:8px;
overflow:hidden;
margin-top:7px
}

.powerBullFill{
height:100%;
background:#1b9b5c
}

.powerBearFill{
height:100%;
background:#d44754
}

.powerWinner{
font-size:28px;
font-weight:900;
text-align:center;
margin-top:14px
}

.strikeHero{margin-top:14px;padding:16px;background:#0b151e;border:1px solid #263845;border-radius:16px}
.strikeTop{display:flex;justify-content:space-between;gap:12px}
.strikePrice{font-size:32px;font-weight:900;margin-top:5px}
.strikeTime{text-align:right;font-size:27px;font-weight:900}
.strikeStatus{display:inline-block;margin-top:7px;padding:5px 9px;border-radius:8px;font-size:12px;font-weight:900}
.strikeAbove{color:#43d184;background:#092b1a;border:1px solid #1b9b5c}
.strikeBelow{color:#ff6570;background:#321014;border:1px solid #d44754}
.strikeWait{color:#e0c45b;background:#30280b;border:1px solid #c4a63a}
.strikeStats{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:14px}
.strikeStat{background:#101f2b;border:1px solid #20323f;border-radius:10px;padding:10px}
.strikeStat span{display:block;color:#91a3b0;font-size:10px;font-weight:800}
.strikeStat strong{display:block;font-size:17px;margin-top:4px}
.strikeChartBox{height:280px;margin-top:12px;background:#071019;border:1px solid #1c2c38;border-radius:12px;overflow:hidden}
#strikeChart{width:100%;height:100%;display:block}
.strikeLegend{display:flex;justify-content:space-between;margin-top:9px;color:#91a3b0;font-size:11px;font-weight:700}
@media(max-width:520px){.strikeHero{padding:12px}.strikePrice{font-size:27px}.strikeTime{font-size:23px}.strikeChartBox{height:250px}.strikeStat strong{font-size:15px}.strikeLegend{font-size:10px}}
.shiftBox{
margin-top:14px;
padding:14px;
border-radius:12px;
background:#101f2b;
border:1px solid #243847;
text-align:center
}

.shiftTitle{
font-size:20px;
font-weight:900
}

.shiftDetail{
color:#91a3b0;
font-size:13px;
margin-top:7px;
line-height:1.45
}

.shiftBull{
border-color:#1b9b5c;
background:#092b1a
}

.shiftBear{
border-color:#d44754;
background:#321014
}

.shiftWait{
border-color:#c4a63a;
background:#30280b
}

@media(max-width:520px){
.powerGrid{
grid-template-columns:1fr
}
}

@media(max-width:800px){

.grid{
grid-template-columns:repeat(2,1fr)
}

}

@media(max-width:520px){

.grid{
grid-template-columns:1fr
}

.wide{
grid-column:auto
}

}


/* =======================================================
   MARKET CONTROL HEAT
   ======================================================= */
.heatPanel{
background:#09141d;
border:1px solid #253744;
border-radius:16px;
padding:18px;
margin-top:10px
}
.heatHeader{
display:flex;
justify-content:space-between;
align-items:center;
gap:10px;
flex-wrap:wrap
}
.heatWinner{
font-size:25px;
font-weight:900
}
.heatSub{
color:#91a3b0;
font-size:13px;
margin-top:4px
}
.heatRows{
display:grid;
grid-template-columns:1fr;
gap:9px;
margin-top:16px
}
.heatRow{
display:grid;
grid-template-columns:115px 1fr 58px;
align-items:center;
gap:10px
}
.heatName{
font-size:13px;
color:#a9b9c4
}
.heatTrack{
height:12px;
background:#172631;
border-radius:99px;
overflow:hidden;
position:relative
}
.heatFill{
height:100%;
width:50%;
transition:width .25s ease;
background:#3ed37f
}
.heatFill.sell{
background:#ef5360
}
.heatNumber{
text-align:right;
font-weight:800
}
.heatBattle{
display:grid;
grid-template-columns:1fr 1fr;
gap:10px;
margin-top:16px
}
.heatSide{
padding:12px;
border-radius:12px;
background:#0f202b
}
.heatSideTitle{
font-size:13px;
color:#9db0bc;
margin-bottom:5px
}
.heatBig{
font-size:30px;
font-weight:900
}
.heatAccel{
margin-top:14px;
padding:11px 13px;
border-radius:12px;
background:#111f28;
font-weight:800
}
.heatAccel.bull{
border:1px solid #28b86b;
color:#53df91
}
.heatAccel.bear{
border:1px solid #d84a58;
color:#ff6a76
}
.heatAccel.wait{
border:1px solid #596873;
color:#b3c0c8
}
.heatHistory{
display:flex;
gap:3px;
height:22px;
margin-top:14px;
width:100%;
}
.heatCell{
flex:1;
border-radius:3px;
background:#44525b;
min-width:3px
}
.heatCell.bull{background:#35cf7b}
.heatCell.bear{background:#ef5260}
.heatCell.flat{background:#68757d}
@media(max-width:700px){
.heatRow{grid-template-columns:92px 1fr 52px}
.heatBattle{grid-template-columns:1fr}
}

</style>

</head>

<body>

<div class="wrap">

<h1>BTC Strike AI</h1>

<div class="small">
KXBTC15M • Binance Live • Signal Memory 🧠
</div>

<div class="strikeHero">
  <div class="strikeTop">
    <div>
      <div class="small">₿ BTC 15 MIN • LIVE STRIKE</div>
      <div id="strikePrice" class="strikePrice">--</div>
      <div id="strikeStatus" class="strikeStatus strikeWait">WAITING FOR TARGET</div>
    </div>
    <div>
      <div class="small">TIME LEFT</div>
      <div id="strikeCountdown" class="strikeTime">--:--</div>
    </div>
  </div>
  <div class="strikeStats">
    <div class="strikeStat"><span>TARGET / STRIKE</span><strong id="strikeTarget">--</strong></div>
    <div class="strikeStat"><span>NOW</span><strong id="strikeNow">--</strong></div>
    <div class="strikeStat"><span>DIFFERENCE</span><strong id="strikeDiff">--</strong></div>
  </div>
  <div class="strikeChartBox"><canvas id="strikeChart"></canvas></div>
  <div class="strikeLegend"><span style="color:#ff6570">🔴 BELOW TARGET</span><span style="color:#e0c45b">🎯 TARGET</span><span style="color:#43d184">🟢 ABOVE TARGET</span></div>
</div>

<div id="highConfidenceCard" class="card" style="margin-top:14px;text-align:center;border:2px solid rgba(255,255,255,.22);background:linear-gradient(135deg,rgba(16,31,42,.98),rgba(9,20,28,.98));">
  <div class="small">🎯 HIGH-CONFIDENCE TARGET SIGNAL</div>
  <div id="highConfidenceLabel" class="big" style="margin-top:8px;">🟡 WAIT — CHECKING EVIDENCE</div>
  <div id="highConfidenceScore" style="font-size:27px;font-weight:900;margin-top:5px;">Evidence strength: --</div>
  <div id="highConfidenceMeta" class="small" style="margin-top:5px;">Waiting for independent models and reliable live data.</div>
  <div id="highConfidenceReason" class="small" style="margin-top:8px;opacity:.88;">A strong signal requires agreement, data quality, and target-aware confirmation.</div>
  <div class="small" style="margin-top:9px;color:#f0c36a;">90%–100% verified win probability: NOT ESTABLISHED YET. Displayed evidence strength is not a win probability.</div>
</div>

<div id="platformBridgeCard" class="card" style="margin-top:14px;border:2px solid rgba(100,160,220,.35);">
  <div class="small">🔌 FOUR-PLATFORM RESEARCH BRIDGE</div>
  <div class="big" style="text-align:center;margin:8px 0;">SOURCE STATUS — HONEST MODE</div>
  <div class="small" style="margin-bottom:12px;opacity:.82;">These links are included for quick reference. The external sites are NOT represented as connected feeds unless a supported data interface is verified. Your existing Binance/Coinbase/Kalshi inputs remain the live sources for this app.</div>
  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px;">
    <div style="padding:12px;border:1px solid rgba(255,255,255,.14);border-radius:10px;">
      <div style="font-weight:800;">Midnight Terminal</div>
      <div class="small" style="margin:5px 0;">STATUS: EXTERNAL REFERENCE</div>
      <a href="https://midnightterminal.com/" target="_blank" rel="noopener noreferrer">Open platform ↗</a>
      <div class="small" style="margin-top:6px;opacity:.75;">No documented public data API verified for this integration.</div>
    </div>
    <div style="padding:12px;border:1px solid rgba(255,255,255,.14);border-radius:10px;">
      <div style="font-weight:800;">Bitcoin UpDown</div>
      <div class="small" style="margin:5px 0;">STATUS: EXTERNAL REFERENCE</div>
      <a href="https://bitcoinupdown.com/" target="_blank" rel="noopener noreferrer">Open platform ↗</a>
      <div class="small" style="margin-top:6px;opacity:.75;">Its published forecast is not imported into this app; no supported public signal API has been verified.</div>
    </div>
    <div style="padding:12px;border:1px solid rgba(255,255,255,.14);border-radius:10px;">
      <div style="font-weight:800;">PM Countdown</div>
      <div class="small" style="margin:5px 0;">STATUS: EXTERNAL REFERENCE</div>
      <a href="https://pmcountdown.com/markets/crypto/btc/btc-15m?tf=1" target="_blank" rel="noopener noreferrer">Open BTC 15-Min ↗</a>
      <div class="small" style="margin-top:6px;opacity:.75;">This app obtains its market/strike from Kalshi directly; PM Countdown is not treated as an independent live feed.</div>
    </div>
    <div style="padding:12px;border:1px solid rgba(255,255,255,.14);border-radius:10px;">
      <div style="font-weight:800;">Bitcoin Edge 15-Min</div>
      <div class="small" style="margin:5px 0;">STATUS: LOCAL MATH ACTIVE</div>
      <a href="https://predictionmarketspicks.com/tools/bitcoin-edge-15m" target="_blank" rel="noopener noreferrer">Open Bitcoin Edge ↗</a>
      <div id="platformEdgeLocal" class="small" style="margin-top:6px;">Local comparison warming up: strike distance, volatility, and time remaining.</div>
    </div>
  </div>
  <div class="small" style="margin-top:12px;color:#f0c36a;">Important: this is not a claim that the four sites are API-connected. External signals must not be counted as confirmations until their data is actually retrieved, timestamped, and validated.</div>
</div>

<div id="ensembleForecast" class="card" style="margin-top:14px;text-align:center;border:2px solid rgba(255,255,255,.16);">
<div class="small">🤖 INDEPENDENT 15-MINUTE ENSEMBLE</div>
<div id="ensembleLabel" class="big">⚪ WAIT</div>
<div id="ensembleProbability" style="font-size:32px;font-weight:800;">50%</div>
<div id="ensembleProbabilityLabel" class="small">Directional probability</div>
<div id="ensembleMeta" class="small">Waiting for model agreement...</div>
<div id="ensembleReason" class="small" style="margin-top:8px;opacity:.85;">Waiting for enough data.</div>
</div>

<div id="adaptiveModeCard" class="card" style="margin-top:14px;border:2px solid rgba(255,255,255,.16);">
  <div class="small">🧠 ADAPTIVE MODE • EARLY WARNING → CONFIRMATION → REVERSAL / WAIT</div>
  <div id="adaptiveModeLabel" class="big" style="text-align:center;margin-top:8px;">🟡 WAIT — BUILDING DATA</div>
  <div id="adaptiveModeMeta" class="small" style="text-align:center;">Waiting for independent evidence groups...</div>
  <div id="adaptiveModeDetail" class="small" style="margin-top:8px;">The adaptive read will appear when data is available.</div>
  <div id="adaptiveEvidence" class="small" style="margin-top:8px;white-space:pre-wrap;"></div>
  <div class="small" style="margin-top:8px;opacity:.7;">Signals are decision aids, not guaranteed outcomes or calibrated probabilities.</div>
</div>

<div id="benchmarkProxyCard" class="card" style="margin-top:14px;border:2px solid rgba(255,255,255,.16);">
  <div class="small">🌐 CROSS-EXCHANGE PRESSURE • BENCHMARK PROXY</div>
  <div id="benchmarkProxyLabel" class="big" style="text-align:center;margin-top:8px;">WAITING FOR BOOKS</div>
  <div id="benchmarkProxyMeta" class="small" style="text-align:center;">Checking multiple exchange order books...</div>
  <div id="benchmarkProxyDetail" class="small" style="margin-top:8px;">This is a public-exchange proxy, not the official CF Benchmarks BRTI feed.</div>
  <div id="controlReadLabel" class="big" style="text-align:center;margin-top:12px;">🟡 CONTROL: WAIT</div>
  <div id="controlReadDetail" class="small" style="margin-top:8px;">Waiting for independent price and pressure evidence.</div>
  <div class="small" style="margin-top:8px;opacity:.72;">Important: a large bid/ask wall alone is not proof of direction. This panel checks whether price actually responds to pressure.</div>
</div>

<div id="marketMakerCard" class="card" style="margin-top:14px;border:2px solid rgba(255,255,255,.16);">
  <div class="small">⚡ REAL-TIME MICROSTRUCTURE • MARKET-MAKER-STYLE PRESSURE</div>
  <div id="marketMakerLabel" class="big" style="text-align:center;margin-top:8px;">⚪ WAIT — BUILDING LIVE DATA</div>
  <div id="marketMakerMeta" class="small" style="text-align:center;">Waiting for live trades and best bid/ask sizes...</div>
  <div id="marketMakerMetrics" class="small" style="margin-top:8px;line-height:1.7;">Trade flow -- • book imbalance -- • microprice -- • spread --</div>
  <div id="marketMakerEvidence" class="small" style="margin-top:8px;white-space:pre-wrap;">Waiting for evidence.</div>
  <div id="marketMakerWarning" class="small" style="margin-top:8px;opacity:.72;">Pressure estimate only; no participant identity or guaranteed future move.</div>
</div>

<div id="winnerForecast" class="card" style="margin-top:14px;text-align:center;border:2px solid rgba(255,255,255,.16);">
<div class="small">🎯 15-MINUTE WINNER PREDICTION</div>
<div id="winnerLabel" class="big">⚪ BUILDING FORECAST</div>
<div id="winnerProbability" style="font-size:32px;font-weight:800;">50%</div>
<div id="winnerMeta" class="small">Building trajectory...</div>
<div id="winnerReason" class="small" style="margin-top:8px;opacity:.85;">Waiting for enough market history.</div>
</div>

<div id="verdict"
class="verdict wait">

<div id="label"
class="label">
🟡 WAIT
</div>

<div id="confidence">
0%
</div>

<div id="agreement">
</div>

</div>

<div class="grid">

<div class="card">

<div class="small">
BTC REFERENCE
</div>

<div id="btc"
class="big">
--
</div>

<div id="feeds"
class="small">
--
</div>

</div>

<div class="card">

<div class="small">
⚡ DATA LATENCY
</div>

<div id="btcAge"
class="big">
--
</div>

<div id="latencyDetail"
class="small">
Starting live stream...
</div>

</div>

<div class="card">

<div class="small">
KALSHI TARGET
</div>

<div id="target"
class="big">
--
</div>

<div id="ticker"
class="small">
--
</div>

</div>

<div class="card">

<div class="small">
BTC VS TARGET
</div>

<div id="distance"
class="big">
--
</div>

<div id="distancePct"
class="small">
--
</div>

</div>

<div class="card">

<div class="small">
COUNTDOWN
</div>

<div id="countdown"
class="big">
--
</div>

<div class="small">
until market close
</div>

</div>

<div class="card">

<div class="small">
1 MIN
</div>

<div id="m1"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
5 MIN
</div>

<div id="m5"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
15 MIN
</div>

<div id="m15"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
STRUCTURE
</div>

<div id="structure"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
MOMENTUM
</div>

<div id="acceleration"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
REVERSAL RISK
</div>

<div id="reversal"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
KALSHI YES
</div>

<div id="yes"
class="value">
--
</div>

</div>

<div class="card">

<div class="small">
SIGNAL SCORE
</div>

<div id="score"
class="value">
--
</div>

</div>

<div class="card wide">

<div class="small">
⚔️ BUYER / SELLER BATTLE
</div>

<div id="battleWinner"
class="value">
--
</div>

<div class="battle">

<span id="buyPct">
🟢 Buyers --
</span>

<span id="sellPct">
🔴 Sellers --
</span>

</div>

<div class="battleBar">

<div id="buyBar"
class="battleBuy"
style="width:50%">
</div>

</div>

<div id="battleDelta"
class="small">
Delta: --
</div>

<div id="battleTrades"
class="small">
Trades: --
</div>

</div>

<div class="card wide">

<div class="small">
⚔️ 2 vs 2 POWER BATTLE
</div>

<div class="powerGrid">

<div class="powerSide bull">

<div class="powerTitle">
🟢 BULLISH
</div>

<div class="small">
#1 Buying Power
</div>

<div id="bullBuying" class="powerValue">
--
</div>

<div class="powerBar">
<div id="bullBuyingBar"
class="powerBullFill"
style="width:50%">
</div>
</div>

<div class="small">
#2 Bullish Momentum
</div>

<div id="bullMomentum" class="powerValue">
--
</div>

<div class="powerBar">
<div id="bullMomentumBar"
class="powerBullFill"
style="width:50%">
</div>
</div>

</div>

<div class="powerSide bear">

<div class="powerTitle">
🔴 BEARISH
</div>

<div class="small">
#1 Selling Power
</div>

<div id="bearSelling" class="powerValue">
--
</div>

<div class="powerBar">
<div id="bearSellingBar"
class="powerBearFill"
style="width:50%">
</div>
</div>

<div class="small">
#2 Bearish Momentum
</div>

<div id="bearMomentum" class="powerValue">
--
</div>

<div class="powerBar">
<div id="bearMomentumBar"
class="powerBearFill"
style="width:50%">
</div>
</div>

</div>

</div>

<div id="powerWinner"
class="powerWinner">
🟡 WAIT
</div>

<div id="powerDetail"
class="small"
style="text-align:center">
Waiting for data...
</div>

<div id="shiftBox" class="shiftBox shiftWait">
<div id="shiftTitle" class="shiftTitle">🟡 BUILDING SHIFT HISTORY</div>
<div id="shiftDetail" class="shiftDetail">Watching for a change in control...</div>
</div>

</div>

<div class="card wide">

<div class="small">
🔥 MARKET CONTROL HEAT
</div>

<div class="heatPanel">
  <div class="heatHeader">
    <div>
      <div id="heatWinner" class="heatWinner">🟡 HEAT BALANCED</div>
      <div id="heatConfidence" class="heatSub">Control heat: --</div>
    </div>
    <div id="heatEdge" class="heatSub">Watching for control acceleration...</div>
  </div>

  <div class="heatRows">
    <div class="heatRow">
      <div class="heatName">🟢 Buyer Heat</div>
      <div class="heatTrack"><div id="buyerHeatBar" class="heatFill" style="width:50%"></div></div>
      <div id="buyerHeat" class="heatNumber">50</div>
    </div>
    <div class="heatRow">
      <div class="heatName">🔴 Seller Heat</div>
      <div class="heatTrack"><div id="sellerHeatBar" class="heatFill sell" style="width:50%"></div></div>
      <div id="sellerHeat" class="heatNumber">50</div>
    </div>
  </div>

  <div class="heatBattle">
    <div class="heatSide">
      <div class="heatSideTitle">EXECUTED FLOW</div>
      <div id="executionHeat" class="heatBig">--</div>
      <div class="heatSub">Buyer-side heat</div>
    </div>
    <div class="heatSide">
      <div class="heatSideTitle">LIQUIDITY / BOOK</div>
      <div id="liquidityHeat" class="heatBig">--</div>
      <div class="heatSub">Bid-side liquidity heat</div>
    </div>
    <div class="heatSide">
      <div class="heatSideTitle">MOMENTUM</div>
      <div id="momentumHeat" class="heatBig">--</div>
      <div class="heatSub">Directional momentum heat</div>
    </div>
    <div class="heatSide">
      <div class="heatSideTitle">STRIKE / STRUCTURE</div>
      <div id="strikeStructureHeat" class="heatBig">--</div>
      <div class="heatSub">Price + market structure</div>
    </div>
  </div>

  <div id="heatAccel" class="heatAccel wait">🟡 CONTROL STABLE</div>
  <div id="heatHistory" class="heatHistory"></div>
  <div id="heatNote" class="heatSub">Control heat combines flow and liquidity; it is not a guarantee of the final result.</div>
</div>

</div>

<div class="card wide">

<div class="small">
📖 ORDER-BOOK PRESSURE
</div>

<div id="bookWinner"
class="value">
--
</div>

<div class="battle">

<span id="bidPct">
🟢 Bids --
</span>

<span id="askPct">
🔴 Asks --
</span>

</div>

<div id="bookText"
class="small">
--
</div>

</div>

<div class="card wide">

<div class="small">
🧠 PREDICTION STRENGTH
</div>

<div id="prediction"
class="big">
--
</div>

<div id="predictionDetail"
class="small">
--
</div>

</div>

<div class="card wide">

<div class="small">
DATA QUALITY BRAIN
</div>

<div id="quality"
class="big">
--
</div>

<div id="qualityText"
class="small">
--
</div>

</div>

<div class="card wide">

<div class="small">
SIGNAL MEMORY 🧠
</div>

<div id="memory"
class="value">
0 resolved setups stored
</div>

<div id="memoryMatch"
class="small">
Building pattern history...
</div>

<div id="memoryStats"
class="small">
</div>

</div>

<div class="card wide">

<div class="small">
🏆 PROVEN PERFORMANCE — ACTUAL COMPLETED 15-MIN RESULTS
</div>

<div id="performanceStatus"
class="big">
BUILDING DATA
</div>

<div id="performanceMain"
class="value">
0 wins • 0 losses • 0% actual accuracy
</div>

<div id="performanceDetail"
class="small">
The engine's confidence score is NOT the same as proven accuracy.
</div>

<div id="performanceStreak"
class="small">
Streak: --
</div>

<div id="performanceLast10"
class="small">
Last 10: --
</div>

<div id="performanceBuckets"
class="small">
Confidence vs actual accuracy: building data...
</div>

</div>

<div class="card wide">

<div class="small">
WHY THE ENGINE CHOSE THIS
</div>

<div id="reasons"
class="small">
Waiting...
</div>

</div>

<div class="card wide">

<div class="small">
FEED HEALTH
</div>

<div id="health"
class="small">
--
</div>

</div>

</div>

</div>

<script>

function money(value){

if(value==null)
return "--";

return "$"
+
Number(value).toLocaleString(
undefined,
{
minimumFractionDigits:2,
maximumFractionDigits:2
}
);

}


function percent(value){

if(value==null)
return "--";

return (
value>=0 ? "+" : ""
)
+
Number(value).toFixed(3)
+
"%";

}


function clock(seconds){

if(seconds==null)
return "--";

return String(
Math.floor(seconds/60)
).padStart(2,"0")
+
":"
+
String(
seconds%60
).padStart(2,"0");

}


function setText(id,value){

document.getElementById(
id
).textContent =
value;

}


let marketCloseMs = null;

let countdownRefreshPending =
false;



const strikeChartState={ticker:null,target:null,history:[],live:[]};

function strikeMoney(v){
  if(v==null || !Number.isFinite(Number(v))) return "--";
  return "$"+Number(v).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
}

function updateStrikeHero(price,target){
  // Do not coerce null/empty target values to numeric zero.
  const p=(price===null || price===undefined || price==="")?NaN:Number(price);
  const t=(target===null || target===undefined || target==="")?NaN:Number(target);
  const targetEl=document.getElementById("strikeTarget");
  const st=document.getElementById("strikeStatus"), d=document.getElementById("strikeDiff");
  if(targetEl) targetEl.textContent=Number.isFinite(t) && t>0 ? strikeMoney(t) : "TARGET UNAVAILABLE";
  if(!Number.isFinite(p)) return;
  document.getElementById("strikePrice").textContent=strikeMoney(p);
  document.getElementById("strikeNow").textContent=strikeMoney(p);
  if(!Number.isFinite(t) || t<=0){
    d.textContent="--";
    st.className="strikeStatus strikeWait";
    st.textContent="🟡 WAITING FOR VALID KALSHI TARGET";
    return;
  }
  const diff=p-t;
  d.textContent=(diff>=0?"+":"")+strikeMoney(diff);
  if(diff>0){st.className="strikeStatus strikeAbove";st.textContent="🟢 ABOVE TARGET";}
  else if(diff<0){st.className="strikeStatus strikeBelow";st.textContent="🔴 BELOW TARGET";}
  else{st.className="strikeStatus strikeWait";st.textContent="🎯 AT TARGET";}
}

function drawStrikeChart(){
  const c=document.getElementById("strikeChart"); if(!c)return;
  const box=c.parentElement,r=box.getBoundingClientRect(),dpr=window.devicePixelRatio||1;
  if(r.width<20)return;
  c.width=r.width*dpr;c.height=r.height*dpr;
  const ctx=c.getContext("2d");ctx.setTransform(dpr,0,0,dpr,0,0);
  const W=r.width,H=r.height;ctx.clearRect(0,0,W,H);
  const target=(strikeChartState.target===null || strikeChartState.target===undefined || strikeChartState.target==="")?NaN:Number(strikeChartState.target);
  if(!Number.isFinite(target) || target<=0){ctx.fillStyle="#91a3b0";ctx.font="700 13px Arial";ctx.textAlign="center";ctx.fillText("Waiting for valid Kalshi target...",W/2,H/2);return;}
  let pts=[...strikeChartState.history,...strikeChartState.live].filter(p=>Number.isFinite(p.time)&&Number.isFinite(p.price)).sort((a,b)=>a.time-b.time);
  const cutoff=Date.now()/1000-15*60;pts=pts.filter(p=>p.time>=cutoff);
  const clean=[];for(const p of pts){const q=clean[clean.length-1];if(q&&Math.abs(q.time-p.time)<.2)q.price=p.price;else clean.push({...p});}pts=clean;
  let vals=pts.map(p=>p.price);if(!vals.length)vals=[target];
  let min=Math.min(...vals,target),max=Math.max(...vals,target),range=max-min||Math.max(target*.001,1),pad=Math.max(range*.16,target*.00015);min-=pad;max+=pad;
  const L=10,R=70,T=16,B=25,PW=W-L-R,PH=H-T-B;
  const t0=pts.length?pts[0].time:Date.now()/1000-900,t1=pts.length?Math.max(pts[pts.length-1].time,Date.now()/1000):Date.now()/1000,tr=Math.max(1,t1-t0);
  const X=t=>L+(t-t0)/tr*PW,Y=v=>T+(max-v)/(max-min)*PH,ty=Y(target);
  ctx.strokeStyle="rgba(145,163,176,.10)";ctx.lineWidth=1;
  for(let i=0;i<=4;i++){let gy=T+PH*i/4;ctx.beginPath();ctx.moveTo(L,gy);ctx.lineTo(L+PW,gy);ctx.stroke();}
  if(pts.length>1){
    for(let i=1;i<pts.length;i++){
      let a=pts[i-1],b=pts[i],x1=X(a.time),x2=X(b.time),y1=Y(a.price),y2=Y(b.price);
      const fill=(xA,yA,xB,yB,col)=>{ctx.beginPath();ctx.moveTo(xA,yA);ctx.lineTo(xB,yB);ctx.lineTo(xB,ty);ctx.lineTo(xA,ty);ctx.closePath();ctx.fillStyle=col;ctx.fill();};
      if((a.price-target)*(b.price-target)>=0) fill(x1,y1,x2,y2,a.price>=target?"rgba(27,155,92,.18)":"rgba(212,71,84,.18)");
      else {let f=(target-a.price)/(b.price-a.price),xc=x1+(x2-x1)*f;if(a.price>=target){fill(x1,y1,xc,ty,"rgba(27,155,92,.18)");fill(xc,ty,x2,y2,"rgba(212,71,84,.18)");}else{fill(x1,y1,xc,ty,"rgba(212,71,84,.18)");fill(xc,ty,x2,y2,"rgba(27,155,92,.18)");}}
    }
    ctx.beginPath();pts.forEach((p,i)=>i?ctx.lineTo(X(p.time),Y(p.price)):ctx.moveTo(X(p.time),Y(p.price)));
    ctx.strokeStyle=pts[pts.length-1].price>=target?"#43d184":"#ff6570";ctx.lineWidth=2.7;ctx.lineJoin="round";ctx.lineCap="round";ctx.stroke();
  }
  ctx.save();ctx.setLineDash([7,5]);ctx.strokeStyle="#e0c45b";ctx.lineWidth=1.5;ctx.beginPath();ctx.moveTo(L,ty);ctx.lineTo(L+PW,ty);ctx.stroke();ctx.restore();
  ctx.fillStyle="#e0c45b";ctx.font="800 11px Arial";ctx.textAlign="left";ctx.fillText("TARGET "+strikeMoney(target),L+PW+5,Math.max(12,Math.min(H-8,ty+4)));
  if(pts.length){let p=pts[pts.length-1],cx=X(p.time),cy=Y(p.price);ctx.beginPath();ctx.arc(cx,cy,5,0,Math.PI*2);ctx.fillStyle=p.price>=target?"#43d184":"#ff6570";ctx.fill();ctx.strokeStyle="#fff";ctx.lineWidth=2;ctx.stroke();ctx.fillStyle=p.price>=target?"#43d184":"#ff6570";ctx.font="900 11px Arial";ctx.textAlign="right";ctx.fillText(strikeMoney(p.price),Math.min(W-5,cx+62),Math.max(12,cy-9));}
}

function setStrikeHistory(data){
  const m=data.market||{},ticker=m.ticker||null;
  // Number(null) is 0 in JavaScript; preserve a missing target as null.
  const target=(m.target===null || m.target===undefined || m.target==="")?null:Number(m.target);
  const validTarget=Number.isFinite(target) && target>0 ? target : null;
  if(strikeChartState.ticker!==ticker || strikeChartState.target!==validTarget){
    strikeChartState.ticker=ticker;strikeChartState.target=validTarget;strikeChartState.live=[];
  }
  strikeChartState.history=(Array.isArray(data.candle_history)?data.candle_history:[]).map(p=>({time:Number(p.time),price:Number(p.price)})).filter(p=>Number.isFinite(p.time)&&Number.isFinite(p.price));
  updateStrikeHero(data.btc,validTarget);
  if(data.btc!=null) strikeChartState.live.push({time:Date.now()/1000,price:Number(data.btc)});
  const cut=Date.now()/1000-900;strikeChartState.live=strikeChartState.live.filter(p=>p.time>=cut);
  drawStrikeChart();
}

function pushStrikeLive(price,time){
  if(price==null)return;
  const p=Number(price),t=Number(time)||Date.now()/1000;
  const last=strikeChartState.live[strikeChartState.live.length-1];
  if(last&&Math.abs(t-last.time)<.2)last.price=p;else strikeChartState.live.push({time:t,price:p});
  const cut=Date.now()/1000-900;strikeChartState.live=strikeChartState.live.filter(x=>x.time>=cut);
  updateStrikeHero(p,strikeChartState.target);drawStrikeChart();
}

function tickCountdown(){

const el =
document.getElementById(
"countdown"
);

if(
!el
||
marketCloseMs == null
){

return;

}

const remaining =
Math.max(
0,
Math.ceil(
(
marketCloseMs -
Date.now()
) / 1000
)
);

el.textContent =
clock(
remaining
);
const heroCountdown =
document.getElementById("strikeCountdown");
if(heroCountdown){
  heroCountdown.textContent = clock(remaining);
}

if(
remaining <= 0
&&
!countdownRefreshPending
){

countdownRefreshPending =
true;

refresh().finally(
() => {

countdownRefreshPending =
false;

}
);

}

}


async function refreshLive(){

try{

const response = await fetch(
"/api/live?x=" +
Date.now(),
{
cache:"no-store"
}
);

const live =
await response.json();

if(live.price != null){

setText(
"btc",
money(
live.price
)
);

pushStrikeLive(
live.price,
live.received_at
);

}

if(live.age_ms != null){

setText(
"btcAge",
Number(
live.age_ms
).toFixed(0)
+
" ms"
);

}

setText(
"latencyDetail",
(
live.connected
?
"⚡ Binance live stream"
:
"↩ REST fallback"
)
+
" • live BTC feed"
);

}catch(error){}

}


async function refresh(){

try{

const response =
await fetch(
"/api/state?x=" +
Date.now(),
{
cache:"no-store"
}
);

const data =
await response.json();

const signal =
data.signal || {};

const market =
data.market || {};

setStrikeHistory(data);

if(
data.market_close_ts != null
){

marketCloseMs =
Number(
data.market_close_ts
) * 1000;

}

else if(
market.close_time
){

const parsed =
Date.parse(
market.close_time
);

marketCloseMs =
Number.isFinite(
parsed
)
?
parsed
:
null;

}

else{

marketCloseMs =
null;

}


const quality =
data.data_quality || {};

const memory =
data.memory || {};

const buySell =
data.buy_sell || {};

const orderBook =
data.order_book || {};

const powerBattle =
data.power_battle || {};

const shiftDetector =
data.shift_detector || {};

const prediction =
data.prediction_strength || {};

const winnerForecast = data.winner_forecast || {};

if(winnerForecast.direction === "UP"){
  setText("winnerLabel", "🟢 PROJECTED WINNER: UP");
  document.getElementById("winnerForecast").style.borderColor = "rgba(60,220,120,.65)";
}else if(winnerForecast.direction === "DOWN"){
  setText("winnerLabel", "🔴 PROJECTED WINNER: DOWN");
  document.getElementById("winnerForecast").style.borderColor = "rgba(255,70,70,.65)";
}else{
  setText("winnerLabel", "⚪ NO CLEAR WINNER");
  document.getElementById("winnerForecast").style.borderColor = "rgba(255,255,255,.16)";
}

setText("winnerProbability", (winnerForecast.probability == null ? 50 : winnerForecast.probability) + "%");
setText("winnerMeta", (winnerForecast.locked ? "🔒 FORECAST LOCKED" : "🧠 BUILDING FORECAST") + " • persistence " + (winnerForecast.persistence || 0) + "/8 • trajectory " + (winnerForecast.trajectory == null ? "--" : Number(winnerForecast.trajectory).toFixed(3)) + " • " + clock(Math.max(0, Math.ceil(winnerForecast.time_remaining || 0))) + " remaining");
setText("winnerReason", winnerForecast.reason || "Waiting for enough market history.");

const ensemble = data.ensemble_forecast || {};
const ensembleBox = document.getElementById("ensembleForecast");
const ensembleDir = ensemble.direction || "WAIT";
if(ensembleDir === "UP"){
  setText("ensembleLabel", "🟢 ENSEMBLE: UP");
  ensembleBox.style.borderColor = "rgba(60,220,120,.75)";
}else if(ensembleDir === "DOWN"){
  setText("ensembleLabel", "🔴 ENSEMBLE: DOWN");
  ensembleBox.style.borderColor = "rgba(255,70,70,.75)";
}else{
  setText("ensembleLabel", "🟡 ENSEMBLE: WAIT");
  ensembleBox.style.borderColor = "rgba(196,166,58,.65)";
}
const ensembleProb = ensemble.probability == null ? 50 : Number(ensemble.probability);
const ensembleDirText = ensembleDir === "UP" ? "UP probability" : ensembleDir === "DOWN" ? "DOWN probability" : "Directional probability";
setText("ensembleProbability", ensembleProb + "%");
setText("ensembleProbabilityLabel", ensembleDirText);
setText("ensembleMeta", "Confidence " + (ensemble.confidence || 0) + "% • agreement " + (ensemble.agreement || 0) + "% • " + (ensemble.models_used || 0) + "/3 models");
setText("ensembleReason", (ensemble.reason || "Waiting for model agreement.") + " • HEURISTIC SCORE — NOT A VERIFIED WIN PROBABILITY");

// High-confidence panel: distinguish evidence strength from a calibrated win probability.
const highCard = document.getElementById("highConfidenceCard");
const highDirection = ensemble.direction || "WAIT";
const highScore = Number(ensemble.confidence || 0);
const highAgreement = Number(ensemble.agreement || 0);
const highModels = Number(ensemble.models_used || 0);
const qualityScore = Number((data.data_quality || {}).score || 0);
const statModel = data.statistical_model || {};
const statDirection = statModel.direction || "WAIT";
const edgeLocalText = statModel.samples
  ? "LOCAL CALCULATION (not imported from the website): strike distance " +
    (statModel.strike_distance_pct == null ? "--" : (Number(statModel.strike_distance_pct) > 0 ? "+" : "") + statModel.strike_distance_pct + "%") +
    " • 1m volatility " + (statModel.volatility_1m_pct == null ? "--" : statModel.volatility_1m_pct + "%") +
    " • time left " + (statModel.time_remaining == null ? "--" : Math.ceil(Number(statModel.time_remaining)) + "s") +
    " • model P(UP) " + (statModel.up_probability == null ? "--" : statModel.up_probability + "%") +
    " • samples " + statModel.samples
  : "Local calculation waiting for enough valid candle history. The external Bitcoin Edge website is not connected.";
setText("platformEdgeLocal", edgeLocalText);
const trajectoryDirection = (data.winner_forecast || {}).direction || "WAIT";
const modelConflict = ["UP", "DOWN"].includes(statDirection) && ["UP", "DOWN"].includes(trajectoryDirection) && statDirection !== trajectoryDirection;
const strongConfirmation = ["UP", "DOWN"].includes(highDirection) && highScore >= 60 && highAgreement >= 75 && highModels >= 2 && qualityScore >= 70 && !modelConflict;
if (strongConfirmation) {
  setText("highConfidenceLabel", (highDirection === "UP" ? "🟢" : "🔴") + " STRONG CONFIRMATION: " + highDirection);
  highCard.style.borderColor = highDirection === "UP" ? "rgba(60,220,120,.8)" : "rgba(255,70,70,.8)";
  setText("highConfidenceReason", "Independent models agree and data quality passes the gate. This is a stronger setup, not a guarantee.");
} else if (["UP", "DOWN"].includes(highDirection)) {
  setText("highConfidenceLabel", "🟡 " + highDirection + " SIGNAL — NEEDS CONFIRMATION");
  highCard.style.borderColor = "rgba(196,166,58,.7)";
  let blockers = [];
  if (highModels < 2) blockers.push("need 2+ models");
  if (highAgreement < 75) blockers.push("model agreement below 75%");
  if (highScore < 60) blockers.push("evidence strength below threshold");
  if (qualityScore < 70) blockers.push("data quality below threshold");
  if (modelConflict) blockers.push("statistical/trajectory disagreement");
  setText("highConfidenceReason", blockers.length ? blockers.join(" • ") : "Waiting for sustained confirmation.");
} else {
  setText("highConfidenceLabel", "🟡 WAIT — NO CONFIRMED EDGE");
  highCard.style.borderColor = "rgba(255,255,255,.22)";
  setText("highConfidenceReason", ensemble.reason || "Signals are mixed or too weak. Waiting is intentional.");
}
setText("highConfidenceScore", "Evidence strength: " + highScore + "/100");
setText("highConfidenceMeta", "Agreement " + highAgreement + "% • models " + highModels + "/3 • data quality " + Math.round(qualityScore) + "% • strike-aware direction");

const adaptive = data.adaptive_mode || {};
const adaptiveCard = document.getElementById("adaptiveModeCard");
const adaptiveDirection = adaptive.direction || "WAIT";
if (adaptive.stage === "CONFIRMATION" && adaptiveDirection === "UP") {
  adaptiveCard.style.borderColor = "rgba(60,220,120,.8)";
} else if (adaptive.stage === "CONFIRMATION" && adaptiveDirection === "DOWN") {
  adaptiveCard.style.borderColor = "rgba(255,70,70,.8)";
} else if (adaptive.stage === "REVERSAL WARNING") {
  adaptiveCard.style.borderColor = "rgba(255,170,55,.85)";
} else {
  adaptiveCard.style.borderColor = "rgba(196,166,58,.65)";
}
setText("adaptiveModeLabel", adaptive.label || "🟡 WAIT — BUILDING DATA");
setText("adaptiveModeMeta",
  "Stage: " + (adaptive.stage || "WAIT") +
  " • Direction: " + adaptiveDirection +
  " • Rule score: " + (adaptive.confidence_score == null ? "--" : adaptive.confidence_score + "%") +
  " • Quality: " + (adaptive.quality == null ? "--" : adaptive.quality + "%") +
  " • UP groups: " + (adaptive.up_groups || 0) +
  " / DOWN groups: " + (adaptive.down_groups || 0) +
  (adaptive.seconds_remaining == null ? "" : " • " + clock(Math.max(0, Math.ceil(adaptive.seconds_remaining))) + " left")
);
setText("adaptiveModeDetail", adaptive.detail || "Waiting for enough data.");
const upEvidence = (adaptive.up_evidence || []).map(x => "🟢 " + x);
const downEvidence = (adaptive.down_evidence || []).map(x => "🔴 " + x);
setText("adaptiveEvidence", [...upEvidence, ...downEvidence].join("\n") || "No directional evidence groups yet.");


const proxy = data.benchmark_proxy || {};
const proxyCard = document.getElementById("benchmarkProxyCard");
const proxyControl = proxy.control || "WAIT";
if (proxyControl === "UP") proxyCard.style.borderColor = "rgba(60,220,120,.8)";
else if (proxyControl === "DOWN") proxyCard.style.borderColor = "rgba(255,70,70,.8)";
else proxyCard.style.borderColor = "rgba(196,166,58,.65)";
if (proxy.available) {
  setText("benchmarkProxyLabel", "$" + Number(proxy.price).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}) + " • " + (proxy.status || "BOOK DATA"));
  setText("benchmarkProxyMeta", (proxy.exchange_count || 0) + " exchanges • book imbalance " + (proxy.book_imbalance_pct == null ? "--" : proxy.book_imbalance_pct + "%") + " • 15s move " + (proxy.price_move_bps_15s == null ? "--" : proxy.price_move_bps_15s + " bps") + " • avg spread " + (proxy.avg_spread_bps == null ? "--" : proxy.avg_spread_bps + " bps"));
} else {
  setText("benchmarkProxyLabel", "🟡 BOOK DATA UNAVAILABLE");
  setText("benchmarkProxyMeta", "Waiting for valid order books from multiple exchanges.");
}
setText("benchmarkProxyDetail", proxy.note || "Proxy data is not the official CF Benchmarks BRTI feed.");
const control = data.control_read || {};
const controlDir = control.direction || "WAIT";
setText("controlReadLabel", (controlDir === "UP" ? "🟢 CONTROL: UP" : controlDir === "DOWN" ? "🔴 CONTROL: DOWN" : "🟡 CONTROL: WAIT") + " • score " + (control.score == null ? "--" : control.score + "/100"));
setText("controlReadDetail", (control.status || "INSUFFICIENT DATA") + " • " + (control.detail || "Waiting for evidence."));

// ---------------------------------------------------------
// REAL-TIME MICROSTRUCTURE / MARKET-MAKER-STYLE PRESSURE
// ---------------------------------------------------------
const mm = data.market_maker || {};
const mmCard = document.getElementById("marketMakerCard");
const mmDir = mm.direction || "WAIT";
if (mmDir === "UP") mmCard.style.borderColor = "rgba(60,220,120,.8)";
else if (mmDir === "DOWN") mmCard.style.borderColor = "rgba(255,70,70,.8)";
else mmCard.style.borderColor = "rgba(196,166,58,.65)";
setText("marketMakerLabel", mm.label || "⚪ WAIT — BUILDING LIVE DATA");
setText("marketMakerMeta", (mm.available ? "LIVE • " : "FEED WARMING • ") + (mm.trade_count_10s || 0) + " trades / 10s • rule score " + (mm.rule_score || 0) + "/100 • book age " + (mm.book_age_ms == null ? "--" : mm.book_age_ms + " ms"));
setText("marketMakerMetrics",
  "Trade delta " + (mm.trade_delta_pct_10s == null ? "--" : (mm.trade_delta_pct_10s > 0 ? "+" : "") + mm.trade_delta_pct_10s + "%") +
  " • Aggressive buys " + (mm.aggressive_buy_pct_10s == null ? "--" : mm.aggressive_buy_pct_10s + "%") +
  " • Top-book imbalance " + (mm.book_imbalance_pct == null ? "--" : (mm.book_imbalance_pct > 0 ? "+" : "") + mm.book_imbalance_pct + "%") +
  " • Microprice edge " + (mm.microprice_edge_bps == null ? "--" : (mm.microprice_edge_bps > 0 ? "+" : "") + mm.microprice_edge_bps + " bps") +
  " • Spread " + (mm.spread_bps == null ? "--" : mm.spread_bps + " bps") +
  " • 10s move " + (mm.price_move_bps_10s == null ? "--" : (mm.price_move_bps_10s > 0 ? "+" : "") + mm.price_move_bps_10s + " bps")
);
setText("marketMakerEvidence", (mm.evidence || []).map(x => "• " + x).join("\n") || (mm.reason || "Waiting for independent evidence."));
setText("marketMakerWarning", (mm.warning || "Market-pressure estimate only.") + " • reconnects " + (mm.reconnects || 0));


const verdict =
document.getElementById(
"verdict"
);

// The top board predicts the final side of the target, not generic price direction.
verdict.className =
"verdict "
+
(
signal.verdict === "UP"
?
"up"
:
signal.verdict === "DOWN"
?
"down"
:
"wait"
);


setText(
"label",
signal.label ||
"🟡 WAIT"
);


setText(
"confidence",
(
signal.confidence || 0
)
+
"%"
);


setText(
"agreement",
(
signal.bullish || 0
)
+
" BULLISH / "
+
(
signal.bearish || 0
)
+
" BEARISH"
);


if(data.btc != null){

setText(
"btc",
money(
data.btc
)
);

}


const feedCount =
Object.values(
data.feeds || {}
).filter(
x =>
x != null
).length;


setText(
"feeds",
feedCount +
" live feeds"
);


const latency =
data.latency || {};


setText(
"btcAge",
latency.btc_age_ms == null
?
"--"
:
Number(
latency.btc_age_ms
).toFixed(0)
+
" ms"
);


const source = latency.btc_source || "REST fallback";
const sourceIcon = source === "Binance Live" || source === "Coinbase Live" ? "⚡" : "↩";
const binanceStatus = latency.binance_connected ? "Binance 🟢" : "Binance 🔴";
const coinbaseStatus = latency.coinbase_connected ? "Coinbase 🟢" : "Coinbase 🔴";
setText(
"latencyDetail",
sourceIcon + " " + source +
" • " + binanceStatus +
" • " + coinbaseStatus +
" • Kalshi " +
(latency.kalshi_age_ms == null ? "--" : Number(latency.kalshi_age_ms).toFixed(0) + " ms")
);


setText(
"target",
money(
market.target
)
);


setText(
"ticker",
market.ticker ||
"--"
);


if(
data.btc != null
&&
market.target != null
){

const difference =
data.btc -
market.target;


setText(
"distance",
(
difference >= 0
?
"+"
:
"-"
)
+
money(
Math.abs(
difference
)
)
);


setText(
"distancePct",
percent(
difference /
market.target *
100
)
);

}


tickCountdown();


setText(
"m1",
percent(
signal.m1
)
);


setText(
"m5",
percent(
signal.m5
)
);


setText(
"m15",
percent(
signal.m15
)
);


setText(
"structure",
signal.structure ||
"--"
);


setText(
"acceleration",
signal.acceleration ||
"--"
);


setText(
"reversal",
signal.reversal ||
"--"
);


if(
market.yes_bid != null
&&
market.yes_ask != null
){

setText(
"yes",
(
(
market.yes_bid +
market.yes_ask
) /
2 *
100
).toFixed(1)
+
"%"
);

}else{

setText(
"yes",
"--"
);

}


setText(
"score",
signal.score == null
?
"--"
:
signal.score
);


setText(
"battleWinner",
buySell.strength ||
"--"
);


if(
buySell.buy_pct != null
&&
buySell.sell_pct != null
){

setText(
"buyPct",
"🟢 Buyers "
+
Number(
buySell.buy_pct
).toFixed(1)
+
"%"
);


setText(
"sellPct",
"🔴 Sellers "
+
Number(
buySell.sell_pct
).toFixed(1)
+
"%"
);


document.getElementById(
"buyBar"
).style.width =
Number(
buySell.buy_pct
) +
"%";


setText(
"battleDelta",
"Delta: "
+
(
buySell.delta >= 0
?
"+$"
:
"-$"
)
+
Math.abs(
buySell.delta || 0
).toLocaleString(
undefined,
{
maximumFractionDigits:0
}
)
);


setText(
"battleTrades",
"Trades: "
+
(
buySell.trades || 0
).toLocaleString()
);

}


setText(
"bookWinner",
orderBook.strength ||
"--"
);


if(
orderBook.bid_pct != null
&&
orderBook.ask_pct != null
){

setText(
"bidPct",
"🟢 Bids "
+
Number(
orderBook.bid_pct
).toFixed(1)
+
"%"
);


setText(
"askPct",
"🔴 Asks "
+
Number(
orderBook.ask_pct
).toFixed(1)
+
"%"
);


setText(
"bookText",
"Order-book pressure: "
+
(
orderBook.winner ||
"BALANCED"
)
);

}


setText(
"bullBuying",
powerBattle.bullish &&
powerBattle.bullish.buying_power != null
?
Number(powerBattle.bullish.buying_power).toFixed(1) + "%"
:
"--"
);

setText(
"bullMomentum",
powerBattle.bullish &&
powerBattle.bullish.momentum_power != null
?
Number(powerBattle.bullish.momentum_power).toFixed(1) + "%"
:
"--"
);

setText(
"bearSelling",
powerBattle.bearish &&
powerBattle.bearish.selling_power != null
?
Number(powerBattle.bearish.selling_power).toFixed(1) + "%"
:
"--"
);

setText(
"bearMomentum",
powerBattle.bearish &&
powerBattle.bearish.momentum_power != null
?
Number(powerBattle.bearish.momentum_power).toFixed(1) + "%"
:
"--"
);

if(
powerBattle.bullish &&
powerBattle.bullish.buying_power != null
){
document.getElementById("bullBuyingBar").style.width =
Number(powerBattle.bullish.buying_power) + "%";
}

if(
powerBattle.bullish &&
powerBattle.bullish.momentum_power != null
){
document.getElementById("bullMomentumBar").style.width =
Number(powerBattle.bullish.momentum_power) + "%";
}

if(
powerBattle.bearish &&
powerBattle.bearish.selling_power != null
){
document.getElementById("bearSellingBar").style.width =
Number(powerBattle.bearish.selling_power) + "%";
}

if(
powerBattle.bearish &&
powerBattle.bearish.momentum_power != null
){
document.getElementById("bearMomentumBar").style.width =
Number(powerBattle.bearish.momentum_power) + "%";
}

setText(
"powerWinner",
powerBattle.label ||
"🟡 WAIT"
);

setText(
"powerDetail",
(
powerBattle.bull_power != null &&
powerBattle.bear_power != null
)
?
(
"Bull Power " +
Number(powerBattle.bull_power).toFixed(1) +
"% • Bear Power " +
Number(powerBattle.bear_power).toFixed(1) +
"% • " +
Number(powerBattle.confidence || 0).toFixed(0) +
"% confidence"
)
:
(
powerBattle.reason ||
"Waiting for data..."
)
);

const shiftBox = document.getElementById("shiftBox");
const shiftDirection = shiftDetector.direction || "WAIT";
shiftBox.className = "shiftBox " +
(shiftDirection === "BEAR" ? "shiftBear" :
 shiftDirection === "BULL" ? "shiftBull" : "shiftWait");
setText(
"shiftTitle",
shiftDetector.label ||
"🟡 BUILDING SHIFT HISTORY"
);
setText(
"shiftDetail",
shiftDetector.detail ||
"Watching for a change in control..."
);

// ---------------------------------------------------------
// MARKET CONTROL HEAT
// ---------------------------------------------------------
const heat = data.market_heat || {};
const buyerHeat = Number(heat.buyer_heat == null ? 50 : heat.buyer_heat);
const sellerHeat = Number(heat.seller_heat == null ? 50 : heat.seller_heat);

setText(
"heatWinner",
heat.label || "🟡 HEAT BALANCED"
);

setText(
"heatConfidence",
"Control heat: " +
(heat.heat_confidence == null ? "--" : Number(heat.heat_confidence).toFixed(0) + "%") +
" • spread " +
Math.abs(buyerHeat - sellerHeat).toFixed(1) +
" pts"
);

setText(
"heatEdge",
heat.early_edge
? "⚡ EARLY EDGE — CONTROL ACCELERATING"
: (heat.acceleration_label || "Watching for control acceleration...")
);

setText("buyerHeat", buyerHeat.toFixed(1));
setText("sellerHeat", sellerHeat.toFixed(1));

document.getElementById("buyerHeatBar").style.width = buyerHeat + "%";
document.getElementById("sellerHeatBar").style.width = sellerHeat + "%";

setText(
"executionHeat",
heat.execution_heat == null ? "--" : Number(heat.execution_heat).toFixed(1) + "%"
);
setText(
"liquidityHeat",
heat.liquidity_heat == null ? "--" : Number(heat.liquidity_heat).toFixed(1) + "%"
);
setText(
"momentumHeat",
heat.momentum_heat == null ? "--" : Number(heat.momentum_heat).toFixed(1) + "%"
);
setText(
"strikeStructureHeat",
(heat.strike_heat == null ? "--" : Number(heat.strike_heat).toFixed(0)) +
" / " +
(heat.structure_heat == null ? "--" : Number(heat.structure_heat).toFixed(0))
);

const heatAccel = document.getElementById("heatAccel");
const heatDir = heat.acceleration_direction || "WAIT";
heatAccel.className = "heatAccel " +
(heatDir === "BULL" ? "bull" : heatDir === "BEAR" ? "bear" : "wait");
setText(
"heatAccel",
(heat.acceleration_label || "🟡 CONTROL STABLE") +
" • " +
((Number(heat.acceleration || 0) >= 0 ? "+" : "") + Number(heat.acceleration || 0).toFixed(1) + " pts")
);

const heatHistoryEl = document.getElementById("heatHistory");
if(heatHistoryEl){
  heatHistoryEl.innerHTML = "";
  (heat.history || []).forEach(item => {
    const cell = document.createElement("div");
    cell.className = "heatCell " +
      (item.winner === "BUYERS" ? "bull" : item.winner === "SELLERS" ? "bear" : "flat");
    const value = Number(item.buyer_heat == null ? 50 : item.buyer_heat);
    cell.title = "Buyer heat " + value.toFixed(1) + " / Seller heat " + (100-value).toFixed(1);
    heatHistoryEl.appendChild(cell);
  });
}


setText(
"prediction",
prediction.label ||
"--"
);


setText(
"predictionDetail",
(
prediction.bullish_points ||
0
)
+
" bullish confirmations • "
+
(
prediction.bearish_points ||
0
)
+
" bearish confirmations"
);


setText(
"quality",
(
quality.score ||
0
)
+
"/100 "
+
(
quality.grade ||
"--"
)
);


setText(
"qualityText",
data.candles +
" history candles"
);


setText(
"memory",
(
memory.records ||
0
)
+
" resolved setups stored"
);


if(
memory.matches
){

setText(
"memoryMatch",
memory.matches
+
" similar setups • "
+
Number(
memory.rate
).toFixed(0)
+
"% historical support"
);

}else{

setText(
"memoryMatch",
"Building pattern history..."
);

}


setText(
"memoryStats",
"Stored outcomes: "
+
(
memory.up ||
0
)
+
" UP • "
+
(
memory.down ||
0
)
+
" DOWN"
);


// ---------------------------------------------------------
// PROVEN PERFORMANCE TRACKER
// ---------------------------------------------------------
const performance =
data.performance || {};

const decided =
Number(
performance.decided || 0
);

const wins =
Number(
performance.wins || 0
);

const losses =
Number(
performance.losses || 0
);

const pushes =
Number(
performance.pushes || 0
);

const accuracy =
performance.accuracy;

setText(
"performanceStatus",
performance.enough_data
?
"🟢 TRACKER ACTIVE"
:
"🟡 BUILDING DATA — " +
decided +
"/" +
(
performance.minimum_sample ||
20
)
);

setText(
"performanceMain",
wins +
" wins • " +
losses +
" losses • " +
(
accuracy == null
?
"--"
:
Number(accuracy).toFixed(1) + "%"
) +
" actual accuracy"
);

setText(
"performanceDetail",
"Decided: " +
decided +
" • Pushes: " +
pushes +
" • Total stored: " +
(
performance.total_records ||
0
) +
" • Recent accuracy: " +
(
performance.recent_accuracy == null
?
"--"
:
Number(
performance.recent_accuracy
).toFixed(1) + "%"
)
);

setText(
"performanceStreak",
"Current streak: " +
(
performance.streak
?
(
performance.streak +
" " +
(
performance.streak_type === "WIN"
?
"WIN"
:
"LOSS"
)
)
:
"--"
)
);

setText(
"performanceLast10",
"Last 10: " +
(
performance.last10 &&
performance.last10.length
?
performance.last10.join(" • ")
:
"--"
)
);

const buckets =
performance.confidence_buckets || {};

const bucketText =
Object.entries(
buckets
).map(
([name,bucket]) =>
name +
"% confidence: " +
(
bucket.decided
?
Number(bucket.accuracy).toFixed(0) + "%"
:
"--"
) +
" (" +
(
bucket.decided || 0
) +
" decided)"
).join(" • ");

setText(
"performanceBuckets",
"Confidence vs actual accuracy: " +
(
bucketText ||
"building data..."
)
);


setText(
"reasons",
(
signal.reasons || []
).map(
reason =>
"• " +
reason
).join("\n")
);


setText(
"health",
Object.entries(
data.feeds || {}
).map(
([name,value]) =>
name +
": " +
(
value != null
?
"LIVE"
:
"OFFLINE"
)
).join(
" • "
)
);


}catch(error){

setText(
"health",
"Dashboard reconnecting..."
);

}

}


refresh();

refreshLive();

tickCountdown();


setInterval(
tickCountdown,
250
);


setInterval(
refresh,
1000
);


setInterval(
refreshLive,
250
);

window.addEventListener("resize", drawStrikeChart);

</script>

</body>

</html>
"""


# =========================================================
# ROUTES
# =========================================================

@app.get("/")
def index():

    return render_template_string(
        PAGE
    )


@app.get("/api/live")
def api_live():

    now = time.time()
    candidates = [
        (live_btc.get("price"), live_btc.get("received_at", 0.0), "Binance Live", live_btc.get("connected")),
        (live_coinbase.get("price"), live_coinbase.get("received_at", 0.0), "Coinbase Live", live_coinbase.get("connected")),
    ]
    fresh = [x for x in candidates if x[0] and x[1] and now - x[1] <= STREAM_MAX_AGE]
    selected = min(fresh, key=lambda x: now - x[1]) if fresh else (None, 0.0, active_btc_source, False)

    return jsonify({
        "price": selected[0],
        "received_at": selected[1],
        "age_ms": round(max(0.0, now - selected[1]) * 1000, 1) if selected[1] else None,
        "connected": bool(selected[3]),
        "source": selected[2],
        "binance_connected": bool(live_btc.get("connected")),
        "coinbase_connected": bool(live_coinbase.get("connected")),
        "binance_age_ms": round(max(0.0, now - live_btc.get("received_at", 0.0)) * 1000, 1) if live_btc.get("received_at") else None,
        "coinbase_age_ms": round(max(0.0, now - live_coinbase.get("received_at", 0.0)) * 1000, 1) if live_coinbase.get("received_at") else None,
    })


@app.get("/api/state")
def api_state():

    now = time.time()

    cached_market = (
        cache.get(
            "state",
            {}
        ).get(
            "market"
        )
        if isinstance(
            cache.get("state"),
            dict
        )
        else None
    )

    cached_close = parse_time(
        cached_market.get(
            "close_time"
        )
        if isinstance(
            cached_market,
            dict
        )
        else None
    )

    cached_market_expired = (
        cached_close is not None
        and
        cached_close.timestamp()
        <= now
    )

    if (
        cache["state"] is not None
        and
        not cached_market_expired
        and
        now -
        cache["time"]
        <
        CACHE_SECONDS
    ):

        return jsonify(
            cache["state"]
        )

    state = collect_state()

    cache["time"] = now

    cache["state"] = state

    return jsonify(
        state
    )


# =========================================================
# LOCAL DEVELOPMENT
# =========================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        )
    )
