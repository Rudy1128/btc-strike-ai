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
                timeout=10
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
            -
            time.time()
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

    total = up + down

    if total <= 0:

        return {
            "direction": "WAIT",
            "score": 0,
            "strength": "LOW",
            "up": up,
            "down": down
        }

    if up > down:

        direction = "UP"

    elif down > up:

        direction = "DOWN"

    else:

        direction = "WAIT"

    score = round(
        (
            max(
                up,
                down
            ) /
            total
        ) * 100
    )

    if score >= 80:
        strength = "VERY STRONG"

    elif score >= 70:
        strength = "STRONG"

    elif score >= 60:
        strength = "MODERATE"

    else:
        strength = "LOW"

    return {
        "direction": direction,
        "score": score,
        "strength": strength,
        "up": up,
        "down": down
    }


# =========================================================
# HISTORY
# =========================================================

def get_history():

    now = time.time()

    if (
        history_cache["candles"]
        and
        now - history_cache["time"]
        < 10
    ):

        return history_cache["candles"]

    sources = [

        (
            "Coinbase",
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            {
                "granularity": 60
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
                "limit": 200
            }
        ),

        (
            "Binance.US",
            "https://api.binance.us/api/v3/klines",
            {
                "symbol": "BTCUSD",
                "interval": "1m",
                "limit": 200
            }
        )
    ]

    for name, url, params in sources:

        data = get_json(
            url,
            params
        )

        candles = []

        try:

            if name == "Coinbase":

                for row in reversed(data):

                    candles.append({
                        "time": float(row[0]),
                        "low": float(row[1]),
                        "high": float(row[2]),
                        "open": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5])
                    })

            elif name == "Kraken":

                pair = next(
                    iter(
                        data["result"]
                    )
                )

                for row in data["result"][pair]:

                    candles.append({
                        "time": float(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[6])
                    })

            else:

                for row in data:

                    candles.append({
                        "time": float(row[0]) / 1000,
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5])
                    })

        except:

            candles = []

        if len(candles) >= 20:

            record_feed(
                name,
                candles[-1]["close"]
            )

            history_cache["time"] = now
            history_cache["candles"] = candles[-200:]

            return history_cache["candles"]

    return history_cache["candles"]


# =========================================================
# MOMENTUM
# =========================================================

def momentum(candles):

    result = {
        "m1": None,
        "m5": None,
        "m15": None,
        "ema9": None,
        "ema21": None,
        "rsi": None
    }

    if len(candles) < 5:
        return result

    closes = [
        number(
            c.get("close")
        )
        for c in candles
    ]

    closes = [
        x
        for x in closes
        if x is not None
    ]

    if len(closes) < 5:
        return result

    last = closes[-1]

    result["m1"] = (
        (
            last -
            closes[-2]
        )
        /
        closes[-2]
    ) * 100

    if len(closes) >= 6:

        result["m5"] = (
            (
                last -
                closes[-6]
            )
            /
            closes[-6]
        ) * 100

    if len(closes) >= 16:

        result["m15"] = (
            (
                last -
                closes[-16]
            )
            /
            closes[-16]
        ) * 100

    def ema(period):

        if len(closes) < period:
            return None

        value = statistics.mean(
            closes[:period]
        )

        multiplier = (
            2 /
            (period + 1)
        )

        for price in closes[period:]:

            value = (
                (
                    price - value
                )
                *
                multiplier
            ) + value

        return value

    result["ema9"] = ema(9)
    result["ema21"] = ema(21)

    if len(closes) >= 15:

        gains = []
        losses = []

        for i in range(
            len(closes) - 14,
            len(closes)
        ):

            change = (
                closes[i] -
                closes[i - 1]
            )

            if change >= 0:

                gains.append(
                    change
                )

                losses.append(0)

            else:

                gains.append(0)

                losses.append(
                    abs(change)
                )

        avg_gain = (
            sum(gains) /
            len(gains)
        )

        avg_loss = (
            sum(losses) /
            len(losses)
        )

        if avg_loss == 0:

            result["rsi"] = 100

        else:

            rs = (
                avg_gain /
                avg_loss
            )

            result["rsi"] = (
                100 -
                (
                    100 /
                    (1 + rs)
                )
            )

    return result


# =========================================================
# PRICE STRUCTURE
# =========================================================

def price_structure(candles):

    if len(candles) < 6:
        return "WAIT"

    recent = candles[-6:]

    highs = [
        number(
            c.get("high")
        )
        for c in recent
    ]

    lows = [
        number(
            c.get("low")
        )
        for c in recent
    ]

    highs = [
        x
        for x in highs
        if x is not None
    ]

    lows = [
        x
        for x in lows
        if x is not None
    ]

    if len(highs) < 6 or len(lows) < 6:
        return "WAIT"

    if (
        highs[-1] > highs[-3]
        and
        lows[-1] > lows[-3]
    ):

        return "HIGHER HIGHS / HIGHER LOWS"

    if (
        highs[-1] < highs[-3]
        and
        lows[-1] < lows[-3]
    ):

        return "LOWER HIGHS / LOWER LOWS"

    if highs[-1] > highs[-3]:

        return "HIGHER HIGH"

    if lows[-1] < lows[-3]:

        return "LOWER LOW"

    return "MIXED"


# =========================================================
# KALSHI HELPERS
# =========================================================

def normalize_market(market):

    if not isinstance(
        market,
        dict
    ):
        return None

    ticker = (
        market.get("ticker")
        or
        market.get("market_ticker")
    )

    close_time = (
        market.get("close_time")
        or
        market.get("close_time_ts")
        or
        market.get("expiration_time")
    )

    target = None

    for key in (
        "strike",
        "floor_strike",
        "cap_strike",
        "target"
    ):

        target = number(
            market.get(key)
        )

        if target is not None:
            break

    yes_bid = number(
        market.get("yes_bid")
    )

    yes_ask = number(
        market.get("yes_ask")
    )

    if yes_bid is not None:
        yes_bid = (
            yes_bid / 100
            if yes_bid > 1
            else yes_bid
        )

    if yes_ask is not None:
        yes_ask = (
            yes_ask / 100
            if yes_ask > 1
            else yes_ask
        )

    return {
        "ticker": ticker,
        "target": target,
        "close_time": close_time,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "raw": market
    }


def yes_mid(market):

    if not market:
        return None

    bid = number(
        market.get("yes_bid")
    )

    ask = number(
        market.get("yes_ask")
    )

    if (
        bid is not None
        and
        ask is not None
    ):

        return (
            bid + ask
        ) / 2

    if bid is not None:
        return bid

    if ask is not None:
        return ask

    return None


def get_kalshi():

    global kalshi_last_update
    global active_market

    if KALSHI_TICKER:

        for base in KALSHI_BASES:

            data = get_json(
                f"{base}/markets/{KALSHI_TICKER}"
            )

            if not isinstance(
                data,
                dict
            ):
                continue

            market_data = data.get(
                "market",
                data
            )

            market = normalize_market(
                market_data
            )

            if not market:
                continue

            close = parse_time(
                market.get(
                    "close_time"
                )
            )

            if (
                close is not None
                and
                close.timestamp()
                > time.time()
            ):

                kalshi_last_update = (
                    time.time()
                )

                active_market = market

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

        if not isinstance(
            data,
            dict
        ):
            continue

        markets = data.get(
            "markets",
            []
        )

        candidates = []

        for item in markets:

            market = normalize_market(
                item
            )

            if not market:
                continue

            close = parse_time(
                market.get(
                    "close_time"
                )
            )

            if (
                close is not None
                and
                close.timestamp()
                > time.time()
            ):

                candidates.append(
                    market
                )

        if candidates:

            candidates.sort(
                key=lambda x:
                    parse_time(
                        x.get(
                            "close_time"
                        )
                    ).timestamp()
            )

            kalshi_last_update = (
                time.time()
            )

            active_market = candidates[0]

            return candidates[0]

    return None


# =========================================================
# KALSHI ORDER BOOK
# =========================================================

def get_kalshi_book(market):

    if not market:
        return None

    ticker = market.get(
        "ticker"
    )

    if not ticker:
        return None

    for base in KALSHI_BASES:

        data = get_json(
            f"{base}/markets/{ticker}/orderbook"
        )

        if not isinstance(
            data,
            dict
        ):
            continue

        book = data.get(
            "orderbook",
            data
        )

        if isinstance(
            book,
            dict
        ):

            return book

    return None


# =========================================================
# SIGNAL ENGINE
# =========================================================

def signal_engine(
    price,
    market,
    momentum_data,
    structure,
    buy_sell,
    order_book
):

    reasons = []

    score = 0
    bullish = 0
    bearish = 0

    m1 = momentum_data.get(
        "m1"
    )

    m5 = momentum_data.get(
        "m5"
    )

    m15 = momentum_data.get(
        "m15"
    )

    target = (
        market.get("target")
        if market
        else None
    )

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

        if price > target:

            score += 25
            bullish += 1

            reasons.append(
                "BTC is above the Kalshi target"
            )

        elif price < target:

            score -= 25
            bearish += 1

            reasons.append(
                "BTC is below the Kalshi target"
            )

        else:

            reasons.append(
                "BTC is sitting at the target"
            )

    else:

        distance_pct = None

    for label, value, weight in (
        ("1m", m1, 10),
        ("5m", m5, 15),
        ("15m", m15, 20)
    ):

        if value is None:
            continue

        if value > 0:

            score += weight
            bullish += 1

            reasons.append(
                f"{label} momentum is positive"
            )

        elif value < 0:

            score -= weight
            bearish += 1

            reasons.append(
                f"{label} momentum is negative"
            )

    if structure.startswith(
        "HIGHER"
    ):

        score += 15
        bullish += 1

        reasons.append(
            "Price structure is bullish"
        )

    elif structure.startswith(
        "LOWER"
    ):

        score -= 15
        bearish += 1

        reasons.append(
            "Price structure is bearish"
        )

    else:

        reasons.append(
            "Price structure is mixed"
        )

    acceleration = "STABLE"

    if (
        m1 is not None
        and
        m5 is not None
    ):

        if (
            m1 > 0
            and
            m5 > 0
            and
            m1 > abs(m5) * 0.15
        ):

            acceleration = "ACCELERATING UP"

        elif (
            m1 < 0
            and
            m5 < 0
            and
            abs(m1) > abs(m5) * 0.15
        ):

            acceleration = "ACCELERATING DOWN"

    if acceleration == "ACCELERATING UP":

        score += 8
        bullish += 1

        reasons.append(
            "Short-term momentum is accelerating upward"
        )

    elif acceleration == "ACCELERATING DOWN":

        score -= 8
        bearish += 1

        reasons.append(
            "Short-term momentum is accelerating downward"
        )

    if buy_sell.get(
        "available"
    ):

        winner = buy_sell.get(
            "winner"
        )

        if winner == "BUYERS":

            score += 10
            bullish += 1

            reasons.append(
                "Trade flow favors buyers"
            )

        elif winner == "SELLERS":

            score -= 10
            bearish += 1

            reasons.append(
                "Trade flow favors sellers"
            )

    if order_book.get(
        "available"
    ):

        winner = order_book.get(
            "winner"
        )

        if winner == "BIDS":

            score += 8
            bullish += 1

            reasons.append(
                "Order book favors bids"
            )

        elif winner == "ASKS":

            score -= 8
            bearish += 1

            reasons.append(
                "Order book favors asks"
            )

    yes = yes_mid(
        market
    )

    if yes is not None:

        if yes >= 0.60:

            score += 10
            bullish += 1

            reasons.append(
                "Kalshi YES pricing favors UP"
            )

        elif yes <= 0.40:

            score -= 10
            bearish += 1

            reasons.append(
                "Kalshi YES pricing favors DOWN"
            )

    reversal = "LOW"

    if (
        m1 is not None
        and
        m5 is not None
    ):

        if (
            m1 < 0
            and
            m5 > 0
        ):

            reversal = "HIGH"

        elif (
            m1 > 0
            and
            m5 < 0
        ):

            reversal = "HIGH"

    if reversal == "HIGH":

        reasons.append(
            "Short-term momentum is fighting the broader move"
        )

    if bullish > bearish:

        raw_confidence = (
            50 +
            min(
                45,
                abs(score) * 0.8
            )
        )

        if score >= 25:

            verdict = "UP"

        else:

            verdict = "WAIT"

    elif bearish > bullish:

        raw_confidence = (
            50 +
            min(
                45,
                abs(score) * 0.8
            )
        )

        if score <= -25:

            verdict = "DOWN"

        else:

            verdict = "WAIT"

    else:

        raw_confidence = 50
        verdict = "WAIT"

    confidence = int(
        max(
            0,
            min(
                95,
                raw_confidence
            )
        )
    )

    if reversal == "HIGH":

        confidence = max(
            35,
            confidence - 12
        )

        if verdict != "WAIT":

            reasons.append(
                "Reversal risk reduces confidence"
            )

    close_time = parse_time(
        market.get(
            "close_time"
        )
        if market
        else None
    )

    remaining = (
        max(
            0,
            int(
                close_time.timestamp()
                -
                time.time()
            )
        )
        if close_time
        else None
    )

    if (
        remaining is not None
        and
        remaining <= 60
    ):

        if confidence < 75:

            verdict = "WAIT"

            reasons.append(
                "Final-minute brake: confidence is not strong enough"
            )

    if verdict == "UP":

        label = "🟢 UP"

    elif verdict == "DOWN":

        label = "🔴 DOWN"

    else:

        label = "🟡 WAIT"

    return {

        "verdict": verdict,

        "label": label,

        "confidence": confidence,

        "score": score,

        "bullish": bullish,

        "bearish": bearish,

        "m1": m1,

        "m5": m5,

        "m15": m15,

        "structure": structure,

        "acceleration": acceleration,

        "reversal": reversal,

        "distance_pct": distance_pct,

        "reasons": reasons[-8:]
    }


# =========================================================
# QUALITY
# =========================================================

def data_quality(
    price,
    market,
    candles,
    buy_sell,
    order_book
):

    score = 0

    if price is not None:
        score += 25

    if market:
        score += 25

    if len(candles) >= 30:
        score += 20

    elif len(candles) >= 15:
        score += 10

    if buy_sell.get(
        "available"
    ):
        score += 15

    if order_book.get(
        "available"
    ):
        score += 15

    if score >= 85:

        grade = "EXCELLENT"

    elif score >= 70:

        grade = "GOOD"

    elif score >= 50:

        grade = "FAIR"

    else:

        grade = "LOW"

    return {
        "score": score,
        "grade": grade
    }


# =========================================================
# SIGNAL MEMORY
# =========================================================

def memory_stats():

    load_memory()

    total = len(
        signal_memory
    )

    up = sum(
        1
        for item in signal_memory
        if item.get("result") == "UP"
    )

    down = sum(
        1
        for item in signal_memory
        if item.get("result") == "DOWN"
    )

    rate = None

    if total:

        rate = (
            max(
                up,
                down
            )
            /
            total
        ) * 100

    return {
        "records": total,
        "up": up,
        "down": down,
        "rate": rate
    }


def memory_match(
    signal,
    price,
    market
):

    load_memory()

    if not signal_memory:
        return None, 0

    current_direction = signal.get(
        "verdict"
    )

    if current_direction == "WAIT":
        return None, 0

    matches = []

    for item in signal_memory[-500:]:

        if item.get(
            "verdict"
        ) != current_direction:

            continue

        old_score = number(
            item.get("score")
        )

        if old_score is None:
            continue

        if abs(
            old_score -
            signal.get(
                "score",
                0
            )
        ) <= 20:

            matches.append(
                item
            )

    if not matches:
        return None, 0

    resolved = [
        item
        for item in matches
        if item.get("result")
        in ("UP", "DOWN")
    ]

    if not resolved:
        return None, len(matches)

    good = sum(
        1
        for item in resolved
        if item.get("result")
        ==
        current_direction
    )

    rate = (
        good /
        len(resolved)
    ) * 100

    return rate, len(matches)


# =========================================================
# STATE COLLECTION
# =========================================================

def collect_state():

    now = time.time()

    if (
        cache["state"] is not None
        and
        now - cache["time"]
        < CACHE_SECONDS
    ):

        return cache["state"]

    price, feeds = get_spot_feeds()

    market = get_kalshi()

    candles = get_history()

    momentum_data = momentum(
        candles
    )

    structure = price_structure(
        candles
    )

    buy_sell = get_buy_sell_pressure()

    order_book = get_order_book_pressure()

    quality = data_quality(
        price,
        market,
        candles,
        buy_sell,
        order_book
    )

    signal = signal_engine(
        price,
        market,
        momentum_data,
        structure,
        buy_sell,
        order_book
    )

    stats = memory_stats()

    memory_rate, memory_matches = memory_match(
        signal,
        price,
        market
    )

    strength = prediction_strength(
        signal,
        price,
        market,
        buy_sell,
        order_book,
        quality,
        memory_rate,
        memory_matches
    )

    close_time = parse_time(
        market.get(
            "close_time"
        )
        if market
        else None
    )

    market_close_ts = (
        close_time.timestamp()
        if close_time
        else None
    )

    state = {

        "updated": datetime.now(
            timezone.utc
        ).isoformat(),

        "btc": price,

        "feeds": feeds,

        "market": market,

        "market_close_ts":
            market_close_ts,

        "candles": len(
            candles
        ),

        "momentum":
            momentum_data,

        "signal":
            signal,

        "data_quality":
            quality,

        "buy_sell":
            buy_sell,

        "order_book":
            order_book,

        "prediction_strength":
            strength,

        "latency":
            {

                # True Binance stream latency:
                # exchange event time -> server receive time.
                # The old value was actually feed age,
                # which made normal live data look slow.

                "btc_age_ms":
                    (
                        round(
                            max(
                                0.0,
                                live_btc.get(
                                    "received_at",
                                    0.0
                                )
                                -
                                live_btc.get(
                                    "event_at",
                                    live_btc.get(
                                        "received_at",
                                        0.0
                                    )
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

                # Freshness of the latest Binance trade.
                "btc_freshness_ms":
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
                    stats["records"],

                "up":
                    stats["up"],

                "down":
                    stats["down"],

                "rate":
                    stats["rate"],

                "matches":
                    memory_matches
            }
    }

    cache["state"] = state
    cache["time"] = now

    return state


# =========================================================
# BACKGROUND STREAM START
# =========================================================

if websocket is not None:

    threading.Thread(
        target=_binance_stream_loop,
        daemon=True
    ).start()


# =========================================================
# DASHBOARD
# =========================================================

PAGE = """
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1"
>

<title>BTC Strike AI</title>

<style>

*{
box-sizing:border-box
}

body{
margin:0;
background:#071018;
color:#edf5f8;
font-family:Arial,sans-serif
}

.wrap{
max-width:1200px;
margin:auto;
padding:18px
}

h1{
margin:0;
font-size:32px
}

.small{
color:#91a3b0;
font-size:13px;
margin-top:5px
}

.grid{
display:grid;
grid-template-columns:repeat(3,1fr);
gap:12px;
margin-top:14px
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

<div
id="verdict"
class="verdict wait"
>

<div
id="label"
class="label"
>
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

<div
id="btc"
class="big"
>
--
</div>

<div
id="feeds"
class="small"
>
--
</div>

</div>

<div class="card">

<div class="small">
⚡ DATA LATENCY
</div>

<div
id="btcAge"
class="big"
>
--
</div>

<div
id="latencyDetail"
class="small"
>
Starting live stream...
</div>

</div>

<div class="card">

<div class="small">
KALSHI TARGET
</div>

<div
id="target"
class="big"
>
--
</div>

<div
id="ticker"
class="small"
>
--
</div>

</div>

<div class="card">

<div class="small">
BTC VS TARGET
</div>

<div
id="distance"
class="big"
>
--
</div>

<div
id="distancePct"
class="small"
>
--
</div>

</div>

<div class="card">

<div class="small">
COUNTDOWN
</div>

<div
id="countdown"
class="big"
>
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

<div
id="m1"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
5 MIN
</div>

<div
id="m5"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
15 MIN
</div>

<div
id="m15"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
STRUCTURE
</div>

<div
id="structure"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
MOMENTUM
</div>

<div
id="acceleration"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
REVERSAL RISK
</div>

<div
id="reversal"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
KALSHI YES
</div>

<div
id="yes"
class="value"
>
--
</div>

</div>

<div class="card">

<div class="small">
SIGNAL SCORE
</div>

<div
id="score"
class="value"
>
--
</div>

</div>

<div class="card wide">

<div class="small">
⚔️ BUYER / SELLER BATTLE
</div>

<div
id="battleWinner"
class="value"
>
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

<div
id="buyBar"
class="battleBuy"
style="width:50%"
>
</div>

</div>

<div
id="battleDelta"
class="small"
>
Delta: --
</div>

<div
id="battleTrades"
class="small"
>
Trades: --
</div>

</div>

<div class="card wide">

<div class="small">
📖 ORDER-BOOK PRESSURE
</div>

<div
id="bookWinner"
class="value"
>
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

<div class="battleBar">

<div
id="bidBar"
class="battleBuy"
style="width:50%"
>
</div>

</div>

<div
id="bookQty"
class="small"
>
--
</div>

</div>

<div class="card wide">

<div class="small">
🧠 PREDICTION STRENGTH
</div>

<div
id="prediction"
class="value"
>
--
</div>

<div
id="predictionScore"
class="small"
>
--
</div>

</div>

<div class="card wide">

<div class="small">
🧠 DATA QUALITY BRAIN
</div>

<div
id="quality"
class="value"
>
--
</div>

<div
id="qualityText"
class="small"
>
--
</div>

</div>

<div class="card wide">

<div class="small">
🧠 SIGNAL MEMORY
</div>

<div
id="memory"
class="value"
>
--
</div>

<div
id="memoryMatch"
class="small"
>
--
</div>

<div
id="memoryStats"
class="small"
>
--
</div>

</div>

<div class="card wide">

<div class="small">
📌 SIGNAL REASONS
</div>

<div
id="reasons"
class="small"
style="white-space:pre-line"
>
--
</div>

</div>

<div class="card wide">

<div class="small">
SYSTEM STATUS
</div>

<div
id="health"
class="small"
>
Starting...
</div>

</div>

</div>

</div>

<script>

let marketCloseMs = null;

function setText(id,value){

const element =
document.getElementById(id);

if(element){
element.textContent =
value;
}

}

function money(value){

if(
value == null ||
!Number.isFinite(
Number(value)
)
){
return "--";
}

return "$" +
Number(value).toLocaleString(
undefined,
{
minimumFractionDigits:2,
maximumFractionDigits:2
}
);

}

function percent(value){

if(
value == null ||
!Number.isFinite(
Number(value)
)
){
return "--";
}

const n =
Number(value);

return (
n >= 0
? "+"
: ""
) +
n.toFixed(3) +
"%";

}

function formatCountdown(){

if(
marketCloseMs == null
){
return "--";
}

const remaining =
Math.max(
0,
Math.floor(
(
marketCloseMs -
Date.now()
) / 1000
)
);

const minutes =
Math.floor(
remaining / 60
);

const seconds =
remaining % 60;

return (
String(minutes)
.padStart(2,"0")
+
":"
+
String(seconds)
.padStart(2,"0")
);

}

function tickCountdown(){

setText(
"countdown",
formatCountdown()
);

}

async function refreshLive(){

try{

const response =
await fetch(
"/api/live?x=" +
Date.now(),
{
cache:"no-store"
}
);

const live =
await response.json();

if(
live.price != null
){

setText(
"btc",
money(
live.price
)
);

}

if(
live.age_ms != null
){

}

}catch(error){

}

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

const prediction =
data.prediction_strength || {};

const latency =
data.latency || {};

const verdict =
document.getElementById(
"verdict"
);

verdict.className =
"verdict " +
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

if(
data.btc != null
){

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
"⚡ Binance stream " +
(
latency.btc_freshness_ms == null
?
"--"
:
Number(
latency.btc_freshness_ms
).toFixed(0)
+
" ms fresh"
)
:
"↩ REST fallback"
)
+
" • Kalshi " +
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
"🟢 Buyers " +
Number(
buySell.buy_pct
).toFixed(1)
+
"%"
);

setText(
"sellPct",
"🔴 Sellers " +
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
"Delta: " +
(
buySell.delta >= 0
?
"+$"
:
"-$"
) +
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
"Trades: " +
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
"🟢 Bids " +
Number(
orderBook.bid_pct
).toFixed(1)
+
"%"
);

setText(
"askPct",
"🔴 Asks " +
Number(
orderBook.ask_pct
).toFixed(1)
+
"%"
);

document.getElementById(
"bidBar"
).style.width =
Number(
orderBook.bid_pct
) +
"%";

setText(
"bookQty",
"Bid qty: " +
Number(
orderBook.bid_qty || 0
).toFixed(4)
+
" BTC • Ask qty: " +
Number(
orderBook.ask_qty || 0
).toFixed(4)
+
" BTC"
);

}

setText(
"prediction",
(
prediction.direction ||
"WAIT"
)
+
" • "
+
(
prediction.strength ||
"LOW"
)
);

setText(
"predictionScore",
"Score: " +
(
prediction.score || 0
)
+
" • Up factors: " +
(
prediction.up || 0
)
+
" • Down factors: " +
(
prediction.down || 0
)
);

setText(
"quality",
(
quality.score || 0
)
+
"/100 " +
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
memory.records || 0
)
+
" resolved setups stored"
);

if(
memory.matches
){

setText(
"memoryMatch",
memory.matches +
" similar setups • " +
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
"Stored outcomes: " +
(
memory.up || 0
)
+
" UP • " +
(
memory.down || 0
)
+
" DOWN"
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
        now - cache["time"]
        < CACHE_SECONDS
        and
        not cached_market_expired
    ):

        return jsonify(
            cache["state"]
        )

    return jsonify(
        collect_state()
    )


if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
