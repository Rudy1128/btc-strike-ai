import os, time, json, statistics, threading
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
session.headers["User-Agent"] = "BTC-Strike-AI/9.0"

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

# =========================================================
# REAL-TIME BINANCE STREAM
# =========================================================

live_btc = {
    "price": None,
    "received_at": 0.0,
    "event_at": 0.0,
    "connected": False,
    "source": "Binance WebSocket"
}

kalshi_last_update = 0.0


def _binance_stream_loop():
    if websocket is None:
        return

    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    while True:
        ws = None

        try:
            ws = websocket.create_connection(
                url,
                timeout=10,
                http_proxy_host=None,
                http_proxy_port=None,
                http_no_proxy=["stream.binance.com"],
                suppress_origin=True
            )

            live_btc["connected"] = True

            while True:
                raw = ws.recv()

                if not raw:
                    raise RuntimeError(
                        "Empty Binance stream message"
                    )

                data = json.loads(raw)
                price = number(
                    data.get("p")
                )

                if price is None or price <= 0:
                    continue

                received = time.time()

                live_btc["price"] = price
                live_btc["received_at"] = received
                live_btc["event_at"] = (
                    safe_event_time(
                        data.get("T")
                    )
                )

                record_feed(
                    "Binance Live",
                    price
                )

        except Exception as exc:

            live_btc["connected"] = False

            feed_health["Binance Live"] = {
                "online": False,
                "last_error": str(exc),
                "last_success":
                    live_btc.get(
                        "received_at",
                        0.0
                    )
            }

            time.sleep(1)

        finally:

            try:
                if ws is not None:
                    ws.close()
            except Exception:
                pass


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

    live_price = live_btc.get(
        "price"
    )

    received_at = live_btc.get(
        "received_at",
        0.0
    )

    if (
        live_price is not None
        and
        received_at
        and
        time.time() - received_at < 3
    ):

        return (
            live_price,
            "Binance Live"
        )

    sources = [

        (
            "Binance",
            "https://api.binance.com/api/v3/ticker/price",
            {
                "symbol": "BTCUSDT"
            }
        ),

        (
            "Binance Data",
            "https://data-api.binance.vision/api/v3/ticker/price",
            {
                "symbol": "BTCUSDT"
            }
        ),

        (
            "Binance.US",
            "https://api.binance.us/api/v3/ticker/price",
            {
                "symbol": "BTCUSD"
            }
        )
    ]

    for name, url, params in sources:

        data = get_json(
            url,
            params
        )

        if isinstance(data, dict):

            price = number(
                data.get("price")
            )

            if price and price > 0:

                return (
                    price,
                    name
                )

    return (
        None,
        "Binance"
    )


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

    target = None

    for key in (
        "floor_strike",
        "strike_price",
        "strike",
        "target",
        "cap_strike"
    ):

        value = number(
            market.get(
                key
            )
        )

        if value is not None:

            target = value
            break

    return {

        "ticker":
            market.get(
                "ticker"
            ),

        "target":
            target,

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
        time.time()
        >=
        active_market.get(
            "close_ts",
            0
        )
    ):

        record = active_market

        target = record.get(
            "target"
        )

        final_price = record.get(
            "last_price"
        )

        direction = record.get(
            "direction"
        )

        if (
            target is not None
            and
            final_price is not None
        ):

            if final_price > target:

                outcome = "UP"

            elif final_price < target:

                outcome = "DOWN"

            else:

                outcome = "PUSH"

            # Only UP/DOWN predictions are scored for win/loss.
            # WAIT remains an unscored pass.
            if direction in (
                "UP",
                "DOWN"
            ):

                result = (
                    "WIN"
                    if direction == outcome
                    else
                    "LOSS"
                )

                scored = True

            else:

                result = "UNSCORED"
                scored = False

            signal_memory.append({

                **record,

                "outcome":
                    outcome,

                "result":
                    result,

                "scored":
                    scored,

                "resolved":
                    datetime.now(
                        timezone.utc
                    ).isoformat()
            })

            signal_memory[:] = (
                signal_memory[-500:]
            )

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
# COLLECT STATE
# =========================================================

def collect_state():

    load_memory()

    price, feeds = (
        get_spot_feeds()
    )

    candles = get_history()

    market = get_kalshi()

    buy_sell = (
        get_buy_sell_pressure()
    )

    order_book = (
        get_order_book_pressure()
    )

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

        "prediction_strength":
            strength,

        "latency":
            {

                "btc_age_ms":
                    (
                        round(
                            max(
                                0.0,
                                time.time()
                                -
                                live_btc.get(
                                    "received_at",
                                    0.0
                                )
                            ) * 1000,
                            1
                        )
                        if live_btc.get(
                            "received_at"
                        )
                        else
                        None
                    ),

                "btc_stream":
                    bool(
                        live_btc.get(
                            "connected"
                        )
                    ),

                "kalshi_age_ms":
                    (
                        round(
                            max(
                                0.0,
                                time.time()
                                -
                                kalshi_last_update
                            ) * 1000,
                            1
                        )
                        if kalshi_last_update
                        else
                        None
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


# =========================================================
# DASHBOARD
# =========================================================

PAGE = r"""
<!doctype html>

<html>

<head>

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>BTC Strike AI</title>

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

</style>

</head>

<body>

<div class="wrap">

<h1>BTC Strike AI</h1>

<div class="small">
KXBTC15M • Binance Live • Signal Memory 🧠
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


const verdict =
document.getElementById(
"verdict"
);

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


setText(
"latencyDetail",
(
latency.btc_stream
?
"⚡ Binance live stream"
:
"↩ REST fallback"
)
+
" • Kalshi "
+
(
latency.kalshi_age_ms == null
?
"--"
:
Number(
latency.kalshi_age_ms
).toFixed(0)
+
" ms old"
)
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

    received = live_btc.get(
        "received_at",
        0.0
    )

    return jsonify({

        "price":
            live_btc.get(
                "price"
            ),

        "received_at":
            received,

        "age_ms":
            (
                round(
                    max(
                        0.0,
                        time.time()
                        -
                        received
                    ) * 1000,
                    1
                )
                if received
                else
                None
            ),

        "connected":
            bool(
                live_btc.get(
                    "connected"
                )
            ),

        "source":
            live_btc.get(
                "source"
            )
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
