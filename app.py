import os, time, json, statistics
from datetime import datetime, timezone, timedelta
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 2
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


# ============================================================
# BASIC HELPERS
# ============================================================

def now_ts():
    return time.time()


def utc_now():
    return datetime.now(timezone.utc)


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=None):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def pct_change(a, b):
    if a is None or b is None:
        return None

    try:
        if float(b) == 0:
            return None
        return ((float(a) - float(b)) / float(b)) * 100.0
    except Exception:
        return None


def median(values):
    vals = [
        float(x)
        for x in values
        if x is not None
    ]

    if not vals:
        return None

    return statistics.median(vals)


def clamp(value, low, high):
    try:
        return max(low, min(high, value))
    except Exception:
        return low


def fmt_price(value):
    if value is None:
        return "—"

    try:
        return f"${float(value):,.2f}"
    except Exception:
        return "—"


def fmt_pct(value, digits=3):
    if value is None:
        return "—"

    try:
        return f"{float(value):+.{digits}f}%"
    except Exception:
        return "—"


def fmt_number(value, digits=2):
    if value is None:
        return "—"

    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return "—"


def get_json(url, params=None, timeout=TIMEOUT):
    started = time.time()

    try:
        r = session.get(
            url,
            params=params,
            timeout=timeout
        )

        elapsed = round(
            (time.time() - started) * 1000
        )

        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"

        data = r.json()

        return data, {
            "ok": True,
            "ms": elapsed,
            "url": url
        }

    except Exception as e:
        return None, str(e)


def record_feed(name, ok, detail=None):
    feed_health[name] = {
        "ok": bool(ok),
        "detail": detail,
        "updated": time.time()
    }


# ============================================================
# BINANCE PRICE FEEDS
# ============================================================

def get_binance_price():
    urls = [
        (
            "Binance",
            "https://api.binance.com/api/v3/ticker/price",
            {"symbol": "BTCUSDT"}
        ),
        (
            "Binance Data",
            "https://data-api.binance.vision/api/v3/ticker/price",
            {"symbol": "BTCUSDT"}
        ),
        (
            "Binance US",
            "https://api.binance.us/api/v3/ticker/price",
            {"symbol": "BTCUSD"}
        ),
    ]

    for name, url, params in urls:
        data, info = get_json(url, params)

        if isinstance(data, dict):
            price = safe_float(data.get("price"))

            if price:
                record_feed(name, True, info)
                return {
                    "price": price,
                    "source": name
                }

        record_feed(name, False, info)

    return {
        "price": None,
        "source": None
    }


# ============================================================
# SPOT PRICE COMPOSITE
# ============================================================

def get_spot_feeds():
    results = []

    feeds = [
        (
            "Binance",
            "https://api.binance.com/api/v3/ticker/price",
            {"symbol": "BTCUSDT"}
        ),
        (
            "Coinbase",
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            None
        ),
        (
            "Kraken",
            "https://api.kraken.com/0/public/Ticker",
            {"pair": "XBTUSD"}
        ),
        (
            "Bitstamp",
            "https://www.bitstamp.net/api/v2/ticker/btcusd/",
            None
        ),
    ]

    for name, url, params in feeds:
        data, info = get_json(url, params)

        price = None

        try:
            if name == "Binance":
                price = safe_float(
                    data.get("price")
                ) if isinstance(data, dict) else None

            elif name == "Coinbase":
                price = safe_float(
                    data["data"]["amount"]
                ) if isinstance(data, dict) else None

            elif name == "Kraken":
                result = data.get("result", {})
                if result:
                    first = next(iter(result.values()))
                    price = safe_float(first["c"][0])

            elif name == "Bitstamp":
                price = safe_float(
                    data.get("last")
                ) if isinstance(data, dict) else None

        except Exception:
            price = None

        if price:
            results.append({
                "name": name,
                "price": price
            })
            record_feed(name, True, info)
        else:
            record_feed(name, False, info)

    prices = [
        x["price"]
        for x in results
        if x.get("price") is not None
    ]

    composite = median(prices)

    return {
        "price": composite,
        "feeds": results
    }


# ============================================================
# HISTORICAL CANDLES
# ============================================================

def get_coinbase_candles():
    url = (
        "https://api.exchange.coinbase.com/"
        "products/BTC-USD/candles"
    )

    params = {
        "granularity": 60
    }

    data, info = get_json(url, params)

    if not isinstance(data, list):
        record_feed(
            "Coinbase History",
            False,
            info
        )
        return []

    candles = []

    for row in reversed(data):
        try:
            candles.append({
                "time": int(row[0]),
                "low": float(row[1]),
                "high": float(row[2]),
                "open": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5])
            })
        except Exception:
            pass

    record_feed(
        "Coinbase History",
        bool(candles),
        info
    )

    return candles


def get_kraken_candles():
    url = "https://api.kraken.com/0/public/OHLC"

    params = {
        "pair": "XBTUSD",
        "interval": 1
    }

    data, info = get_json(url, params)

    try:
        result = data.get("result", {})
        key = next(
            k for k in result.keys()
            if k != "last"
        )

        rows = result[key]

        candles = []

        for row in rows:
            candles.append({
                "time": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[6])
            })

        record_feed(
            "Kraken History",
            bool(candles),
            info
        )

        return candles

    except Exception:
        record_feed(
            "Kraken History",
            False,
            info
        )
        return []


def get_binance_candles():
    endpoints = [
        (
            "Binance History",
            "https://api.binance.com/api/v3/klines"
        ),
        (
            "Binance Data History",
            "https://data-api.binance.vision/api/v3/klines"
        ),
        (
            "Binance US History",
            "https://api.binance.us/api/v3/klines"
        )
    ]

    for name, url in endpoints:
        data, info = get_json(
            url,
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 120
            }
        )

        if not isinstance(data, list):
            continue

        candles = []

        for row in data:
            try:
                candles.append({
                    "time": int(row[0] / 1000),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5])
                })
            except Exception:
                pass

        if candles:
            record_feed(
                name,
                True,
                info
            )
            return candles

        record_feed(
            name,
            False,
            info
        )

    return []


def get_history():
    now = time.time()

    if (
        history_cache["candles"]
        and now - history_cache["time"] < 10
    ):
        return history_cache["candles"]

    sources = [
        get_coinbase_candles,
        get_kraken_candles,
        get_binance_candles
    ]

    best = []

    for source in sources:
        try:
            candles = source()

            if len(candles) > len(best):
                best = candles

            if len(candles) >= 30:
                break

        except Exception:
            pass

    history_cache["time"] = now
    history_cache["candles"] = best

    return best


# ============================================================
# MOMENTUM
# ============================================================

def calculate_return(candles, periods):
    if not candles:
        return None

    if len(candles) <= periods:
        return None

    current = safe_float(
        candles[-1].get("close")
    )

    previous = safe_float(
        candles[-1 - periods].get("close")
    )

    if current is None or previous is None:
        return None

    return pct_change(current, previous)


def momentum_data(candles):
    m1 = calculate_return(candles, 1)
    m5 = calculate_return(candles, 5)
    m15 = calculate_return(candles, 15)

    return {
        "m1": m1,
        "m5": m5,
        "m15": m15
    }


# ============================================================
# STRUCTURE
# ============================================================

def structure_data(candles):
    if not candles or len(candles) < 10:
        return {
            "label": "INSUFFICIENT DATA",
            "direction": "WAIT",
            "score": 0
        }

    recent = candles[-10:]

    highs = [
        safe_float(x.get("high"))
        for x in recent
    ]

    lows = [
        safe_float(x.get("low"))
        for x in recent
    ]

    highs = [
        x for x in highs
        if x is not None
    ]

    lows = [
        x for x in lows
        if x is not None
    ]

    if len(highs) < 6 or len(lows) < 6:
        return {
            "label": "INSUFFICIENT DATA",
            "direction": "WAIT",
            "score": 0
        }

    mid = len(highs) // 2

    first_high = max(highs[:mid])
    second_high = max(highs[mid:])

    first_low = min(lows[:mid])
    second_low = min(lows[mid:])

    higher_highs = second_high > first_high
    higher_lows = second_low > first_low

    lower_highs = second_high < first_high
    lower_lows = second_low < first_low

    if higher_highs and higher_lows:
        return {
            "label": "HIGHER HIGHS / HIGHER LOWS",
            "direction": "UP",
            "score": 2
        }

    if lower_highs and lower_lows:
        return {
            "label": "LOWER HIGHS / LOWER LOWS",
            "direction": "DOWN",
            "score": 2
        }

    if higher_highs or higher_lows:
        return {
            "label": "MIXED BULLISH",
            "direction": "UP",
            "score": 1
        }

    if lower_highs or lower_lows:
        return {
            "label": "MIXED BEARISH",
            "direction": "DOWN",
            "score": 1
        }

    return {
        "label": "SIDEWAYS",
        "direction": "WAIT",
        "score": 0
    }


# ============================================================
# RSI
# ============================================================

def calculate_rsi(candles, period=14):
    closes = [
        safe_float(x.get("close"))
        for x in candles
        if x.get("close") is not None
    ]

    if len(closes) <= period:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    if len(gains) < period:
        return None

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss

    return 100 - (100 / (1 + rs))


# ============================================================
# KALSHI HELPERS
# ============================================================

def kalshi_get(path, params=None):
    for base in KALSHI_BASES:
        url = f"{base}{path}"

        data, info = get_json(
            url,
            params
        )

        if data is not None:
            return data

    return None


def normalize_market(market):
    if not isinstance(market, dict):
        return None

    ticker = (
        market.get("ticker")
        or market.get("market_ticker")
        or ""
    )

    title = (
        market.get("title")
        or market.get("subtitle")
        or ""
    )

    yes_bid = safe_float(
        market.get("yes_bid")
    )

    yes_ask = safe_float(
        market.get("yes_ask")
    )

    no_bid = safe_float(
        market.get("no_bid")
    )

    no_ask = safe_float(
        market.get("no_ask")
    )

    last_price = safe_float(
        market.get("last_price")
    )

    close_time = (
        market.get("close_time")
        or market.get("expiration_time")
        or market.get("end_time")
    )

    strike = None

    possible_strike_keys = [
        "strike",
        "floor_strike",
        "cap_strike",
        "target",
        "target_price"
    ]

    for key in possible_strike_keys:
        value = safe_float(
            market.get(key)
        )

        if value is not None:
            strike = value
            break

    return {
        "ticker": ticker,
        "title": title,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "last_price": last_price,
        "close_time": close_time,
        "strike": strike,
        "raw": market
    }


def extract_market_list(data):
    if not isinstance(data, dict):
        return []

    markets = data.get("markets")

    if isinstance(markets, list):
        return markets

    market = data.get("market")

    if isinstance(market, dict):
        return [market]

    return []


def discover_kalshi_market():
    global KALSHI_TICKER

    if KALSHI_TICKER:
        data = kalshi_get(
            f"/markets/{KALSHI_TICKER}"
        )

        markets = extract_market_list(data)

        if markets:
            normalized = normalize_market(
                markets[0]
            )

            if normalized:
                return normalized

    data = kalshi_get(
        "/markets",
        {
            "series_ticker": KALSHI_SERIES,
            "status": "open",
            "limit": 100
        }
    )

    markets = extract_market_list(data)

    normalized = []

    for market in markets:
        item = normalize_market(market)

        if item:
            normalized.append(item)

    if not normalized:
        return None

    now = utc_now()

    active = []

    for market in normalized:
        close_time = market.get("close_time")

        if close_time:
            try:
                dt = datetime.fromisoformat(
                    str(close_time).replace(
                        "Z",
                        "+00:00"
                    )
                )

                if dt > now:
                    active.append(
                        (dt, market)
                    )
            except Exception:
                pass

    if active:
        active.sort(
            key=lambda x: x[0]
        )

        selected = active[0][1]

        KALSHI_TICKER = selected["ticker"]

        return selected

    selected = normalized[0]

    KALSHI_TICKER = selected["ticker"]

    return selected


def kalshi_data():
    market = discover_kalshi_market()

    if not market:
        record_feed(
            "Kalshi",
            False,
            "No active market found"
        )

        return {
            "market": None,
            "target": None,
            "yes_probability": None,
            "ticker": None
        }

    yes_probability = None

    if market["yes_ask"] is not None:
        yes_probability = market["yes_ask"]

    elif market["last_price"] is not None:
        yes_probability = market["last_price"]

    if yes_probability is not None:
        if yes_probability > 1:
            yes_probability = yes_probability / 100.0

    record_feed(
        "Kalshi",
        True,
        market["ticker"]
    )

    return {
        "market": market,
        "target": market.get("strike"),
        "yes_probability": yes_probability,
        "ticker": market.get("ticker")
    }


# ============================================================
# COUNTDOWN
# ============================================================

def countdown_seconds(close_time):
    if not close_time:
        return None

    try:
        dt = datetime.fromisoformat(
            str(close_time).replace(
                "Z",
                "+00:00"
            )
        )

        seconds = (
            dt - utc_now()
        ).total_seconds()

        return max(
            0,
            int(seconds)
        )

    except Exception:
        return None


def format_countdown(seconds):
    if seconds is None:
        return "—"

    seconds = max(
        0,
        int(seconds)
    )

    minutes = seconds // 60
    secs = seconds % 60

    return f"{minutes:02d}:{secs:02d}"


# ============================================================
# SIGNAL MEMORY
# ============================================================

def load_memory():
    try:
        if not os.path.exists(
            MEMORY_FILE
        ):
            return {
                "signals": []
            }

        with open(
            MEMORY_FILE,
            "r",
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return {
                "signals": []
            }

        if not isinstance(
            data.get("signals"),
            list
        ):
            data["signals"] = []

        return data

    except Exception:
        return {
            "signals": []
        }


def save_memory(data):
    try:
        with open(
            MEMORY_FILE,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                data,
                f,
                indent=2
            )
    except Exception:
        pass


def update_signal_memory(signal):
    memory = load_memory()

    signals = memory.get(
        "signals",
        []
    )

    current = {
        "time": time.time(),
        "verdict": signal.get("verdict"),
        "confidence": signal.get("confidence"),
        "bullish": signal.get("bullish_score"),
        "bearish": signal.get("bearish_score")
    }

    signals.append(current)

    signals = signals[-200:]

    memory["signals"] = signals

    save_memory(memory)

    return memory


def memory_analysis():
    memory = load_memory()

    signals = memory.get(
        "signals",
        []
    )

    if not signals:
        return {
            "samples": 0,
            "up": 0,
            "down": 0,
            "wait": 0
        }

    up = sum(
        1 for x in signals
        if x.get("verdict") == "UP"
    )

    down = sum(
        1 for x in signals
        if x.get("verdict") == "DOWN"
    )

    wait = sum(
        1 for x in signals
        if x.get("verdict") == "WAIT"
    )

    return {
        "samples": len(signals),
        "up": up,
        "down": down,
        "wait": wait
    }


# ============================================================
# BUYER / SELLER BATTLE
# ============================================================

def get_buy_sell_pressure():
    endpoints = [
        (
            "Binance Trades",
            "https://api.binance.com/api/v3/aggTrades",
            {"symbol": "BTCUSDT", "limit": 1000}
        ),
        (
            "Binance Trades Data",
            "https://data-api.binance.vision/api/v3/aggTrades",
            {"symbol": "BTCUSDT", "limit": 1000}
        ),
        (
            "Binance US Trades",
            "https://api.binance.us/api/v3/aggTrades",
            {"symbol": "BTCUSD", "limit": 1000}
        )
    ]

    for name, url, params in endpoints:
        data, info = get_json(
            url,
            params
        )

        if not isinstance(data, list):
            record_feed(
                name,
                False,
                info
            )
            continue

        buyers = 0.0
        sellers = 0.0
        trade_count = 0

        for trade in data:
            try:
                qty = safe_float(
                    trade.get("q"),
                    0
                )

                price = safe_float(
                    trade.get("p"),
                    0
                )

                notional = qty * price

                # Binance:
                # m=True means buyer is maker.
                # Therefore seller was the aggressive side.
                buyer_is_maker = bool(
                    trade.get("m")
                )

                if buyer_is_maker:
                    sellers += notional
                else:
                    buyers += notional

                trade_count += 1

            except Exception:
                continue

        total = buyers + sellers

        if total <= 0:
            continue

        buy_pct = (
            buyers / total
        ) * 100

        sell_pct = (
            sellers / total
        ) * 100

        delta = buyers - sellers

        if buyers > sellers:
            winner = "BUYERS"
            strength = buy_pct
        elif sellers > buyers:
            winner = "SELLERS"
            strength = sell_pct
        else:
            winner = "BALANCED"
            strength = 50.0

        record_feed(
            "Binance Trades",
            True,
            {
                "source": name,
                "trades": trade_count
            }
        )

        return {
            "buyers": buyers,
            "sellers": sellers,
            "buy_pct": buy_pct,
            "sell_pct": sell_pct,
            "delta": delta,
            "winner": winner,
            "strength": strength,
            "trade_count": trade_count,
            "source": name
        }

    return {
        "buyers": None,
        "sellers": None,
        "buy_pct": None,
        "sell_pct": None,
        "delta": None,
        "winner": "UNKNOWN",
        "strength": None,
        "trade_count": 0,
        "source": None
    }


# ============================================================
# ORDER BOOK PRESSURE
# ============================================================

def get_order_book_pressure():
    endpoints = [
        (
            "Binance Order Book",
            "https://api.binance.com/api/v3/depth",
            {"symbol": "BTCUSDT", "limit": 100}
        ),
        (
            "Binance Data Order Book",
            "https://data-api.binance.vision/api/v3/depth",
            {"symbol": "BTCUSDT", "limit": 100}
        ),
        (
            "Binance US Order Book",
            "https://api.binance.us/api/v3/depth",
            {"symbol": "BTCUSD", "limit": 100}
        )
    ]

    for name, url, params in endpoints:
        data, info = get_json(
            url,
            params
        )

        if not isinstance(data, dict):
            record_feed(
                name,
                False,
                info
            )
            continue

        bids = data.get("bids", [])
        asks = data.get("asks", [])

        bid_qty = 0.0
        ask_qty = 0.0

        for row in bids:
            try:
                bid_qty += (
                    float(row[1])
                )
            except Exception:
                pass

        for row in asks:
            try:
                ask_qty += (
                    float(row[1])
                )
            except Exception:
                pass

        total = bid_qty + ask_qty

        if total <= 0:
            continue

        bid_pct = (
            bid_qty / total
        ) * 100

        ask_pct = (
            ask_qty / total
        ) * 100

        if bid_qty > ask_qty:
            winner = "BIDS"
            pressure = bid_pct
        elif ask_qty > bid_qty:
            winner = "ASKS"
            pressure = ask_pct
        else:
            winner = "BALANCED"
            pressure = 50.0

        record_feed(
            "Order Book",
            True,
            {
                "source": name,
                "levels": len(bids)
            }
        )

        return {
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
            "bid_pct": bid_pct,
            "ask_pct": ask_pct,
            "winner": winner,
            "pressure": pressure,
            "levels": min(
                len(bids),
                len(asks)
            ),
            "source": name
        }

    return {
        "bid_qty": None,
        "ask_qty": None,
        "bid_pct": None,
        "ask_pct": None,
        "winner": "UNKNOWN",
        "pressure": None,
        "levels": 0,
        "source": None
    }


# ============================================================
# DATA QUALITY
# ============================================================

def calculate_quality(
    btc,
    feeds,
    candles,
    market
):
    score = 0
    reasons = []

    if btc is not None:
        score += 30
        reasons.append(
            "Live BTC price"
        )

    if len(feeds) >= 3:
        score += 20
        reasons.append(
            "Multiple spot feeds"
        )
    elif len(feeds) >= 1:
        score += 10
        reasons.append(
            "Single spot feed"
        )

    if len(candles) >= 30:
        score += 25
        reasons.append(
            "Historical candles"
        )
    elif len(candles) >= 10:
        score += 15
        reasons.append(
            "Limited candles"
        )

    if market:
        score += 25
        reasons.append(
            "Kalshi market"
        )

    score = clamp(
        score,
        0,
        100
    )

    if score >= 85:
        label = "HIGH"
    elif score >= 65:
        label = "MEDIUM"
    else:
        label = "LOW"

    return {
        "score": score,
        "label": label,
        "reasons": reasons
    }


# ============================================================
# SIGNAL ENGINE
# ============================================================

def build_signal(
    btc,
    target,
    momentum,
    structure,
    yes_probability,
    quality,
    buy_sell=None,
    order_book=None
):
    bullish = 0
    bearish = 0

    reasons_up = []
    reasons_down = []

    if btc is not None and target is not None:
        if btc > target:
            bullish += 2
            reasons_up.append(
                "BTC above target"
            )
        elif btc < target:
            bearish += 2
            reasons_down.append(
                "BTC below target"
            )

    for key, weight in [
        ("m1", 1),
        ("m5", 2),
        ("m15", 2)
    ]:
        value = momentum.get(key)

        if value is None:
            continue

        if value > 0:
            bullish += weight
            reasons_up.append(
                f"{key} positive"
            )
        elif value < 0:
            bearish += weight
            reasons_down.append(
                f"{key} negative"
            )

    if structure.get("direction") == "UP":
        bullish += structure.get(
            "score",
            0
        )
        reasons_up.append(
            structure.get("label")
        )

    elif structure.get("direction") == "DOWN":
        bearish += structure.get(
            "score",
            0
        )
        reasons_down.append(
            structure.get("label")
        )

    if buy_sell:
        winner = buy_sell.get(
            "winner"
        )

        strength = buy_sell.get(
            "strength"
        )

        if winner == "BUYERS":
            bullish += 1

            if strength and strength >= 55:
                bullish += 1

            reasons_up.append(
                "Buyer pressure"
            )

        elif winner == "SELLERS":
            bearish += 1

            if strength and strength >= 55:
                bearish += 1

            reasons_down.append(
                "Seller pressure"
            )

    if order_book:
        winner = order_book.get(
            "winner"
        )

        pressure = order_book.get(
            "pressure"
        )

        if winner == "BIDS":
            bullish += 1

            if pressure and pressure >= 55:
                bullish += 1

            reasons_up.append(
                "Bid pressure"
            )

        elif winner == "ASKS":
            bearish += 1

            if pressure and pressure >= 55:
                bearish += 1

            reasons_down.append(
                "Ask pressure"
            )

    if yes_probability is not None:
        if yes_probability >= 0.65:
            bullish += 1
            reasons_up.append(
                "Kalshi YES strength"
            )

        elif yes_probability <= 0.35:
            bearish += 1
            reasons_down.append(
                "Kalshi YES weakness"
            )

    total = bullish + bearish

    if total <= 0:
        return {
            "verdict": "WAIT",
            "confidence": 50,
            "label": "NO CLEAR EDGE",
            "bullish_score": bullish,
            "bearish_score": bearish,
            "reasons_up": reasons_up,
            "reasons_down": reasons_down
        }

    if bullish > bearish:
        confidence = 50 + (
            (bullish - bearish)
            / max(total, 1)
        ) * 45

        verdict = "UP"

    elif bearish > bullish:
        confidence = 50 + (
            (bearish - bullish)
            / max(total, 1)
        ) * 45

        verdict = "DOWN"

    else:
        confidence = 50
        verdict = "WAIT"

    confidence = int(
        clamp(
            round(confidence),
            50,
            95
        )
    )

    if quality < 60:
        verdict = "WAIT"
        confidence = min(
            confidence,
            60
        )

    if verdict == "UP":
        if confidence >= 85:
            label = "STRONG CONFIRMATION"
        elif confidence >= 70:
            label = "GOOD CONFIRMATION"
        else:
            label = "WEAK UP"

    elif verdict == "DOWN":
        if confidence >= 85:
            label = "STRONG CONFIRMATION"
        elif confidence >= 70:
            label = "GOOD CONFIRMATION"
        else:
            label = "WEAK DOWN"

    else:
        label = "WAIT / UNDECIDED"

    return {
        "verdict": verdict,
        "confidence": confidence,
        "label": label,
        "bullish_score": bullish,
        "bearish_score": bearish,
        "reasons_up": reasons_up,
        "reasons_down": reasons_down
    }


# ============================================================
# REVERSAL RISK
# ============================================================

def reversal_risk(momentum):
    m1 = momentum.get("m1")
    m5 = momentum.get("m5")
    m15 = momentum.get("m15")

    values = [
        x for x in [
            m1,
            m5,
            m15
        ]
        if x is not None
    ]

    if len(values) < 3:
        return {
            "label": "UNKNOWN",
            "score": 50
        }

    same_positive = all(
        x > 0
        for x in values
    )

    same_negative = all(
        x < 0
        for x in values
    )

    if same_positive or same_negative:
        return {
            "label": "LOW",
            "score": 15
        }

    signs = [
        1 if x > 0 else -1
        for x in values
    ]

    changes = sum(
        1
        for i in range(1, len(signs))
        if signs[i] != signs[i - 1]
    )

    if changes >= 2:
        return {
            "label": "HIGH",
            "score": 85
        }

    return {
        "label": "MEDIUM",
        "score": 50
    }


# ============================================================
# PREDICTION STRENGTH
# ============================================================

def prediction_strength(
    btc,
    target,
    momentum,
    structure,
    buy_sell,
    order_book,
    yes_probability,
    reversal,
    quality,
    memory
):
    """
    This is an ALIGNMENT SCORE, not a probability of winning.
    It measures how many independent pieces of evidence agree.
    """

    bullish = 0
    bearish = 0

    evidence = []

    # --------------------------------------------------------
    # BTC VS TARGET
    # --------------------------------------------------------

    if btc is not None and target is not None:
        if btc > target:
            bullish += 1
            evidence.append(
                "BTC above target"
            )

        elif btc < target:
            bearish += 1
            evidence.append(
                "BTC below target"
            )

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    for key in [
        "m1",
        "m5",
        "m15"
    ]:
        value = momentum.get(key)

        if value is None:
            continue

        if value > 0:
            bullish += 1

        elif value < 0:
            bearish += 1

    # --------------------------------------------------------
    # STRUCTURE
    # --------------------------------------------------------

    if structure.get("direction") == "UP":
        bullish += 1
        evidence.append(
            "Bullish structure"
        )

    elif structure.get("direction") == "DOWN":
        bearish += 1
        evidence.append(
            "Bearish structure"
        )

    # --------------------------------------------------------
    # BUYER / SELLER
    # --------------------------------------------------------

    if buy_sell:
        winner = buy_sell.get(
            "winner"
        )

        strength = buy_sell.get(
            "strength"
        )

        if winner == "BUYERS":
            bullish += 1

            if strength and strength >= 60:
                bullish += 1

            evidence.append(
                "Buyers winning"
            )

        elif winner == "SELLERS":
            bearish += 1

            if strength and strength >= 60:
                bearish += 1

            evidence.append(
                "Sellers winning"
            )

    # --------------------------------------------------------
    # ORDER BOOK
    # --------------------------------------------------------

    if order_book:
        winner = order_book.get(
            "winner"
        )

        pressure = order_book.get(
            "pressure"
        )

        if winner == "BIDS":
            bullish += 1

            if pressure and pressure >= 60:
                bullish += 1

            evidence.append(
                "Bid pressure"
            )

        elif winner == "ASKS":
            bearish += 1

            if pressure and pressure >= 60:
                bearish += 1

            evidence.append(
                "Ask pressure"
            )

    # --------------------------------------------------------
    # KALSHI YES
    # --------------------------------------------------------

    if yes_probability is not None:
        if yes_probability >= 0.65:
            bullish += 1
            evidence.append(
                "Kalshi YES support"
            )

        elif yes_probability <= 0.35:
            bearish += 1
            evidence.append(
                "Kalshi YES weakness"
            )

    # --------------------------------------------------------
    # MEMORY
    # --------------------------------------------------------

    if memory:
        samples = memory.get(
            "samples",
            0
        )

        up = memory.get(
            "up",
            0
        )

        down = memory.get(
            "down",
            0
        )

        if samples >= 3:
            if up > down:
                bullish += 1
            elif down > up:
                bearish += 1

    # --------------------------------------------------------
    # REVERSAL BRAKE
    # --------------------------------------------------------

    if reversal:
        if reversal.get("score", 0) >= 80:
            evidence.append(
                "High reversal risk"
            )

            return {
                "direction": "WAIT",
                "score": 0,
                "label": "REVERSAL RISK",
                "bullish_points": bullish,
                "bearish_points": bearish,
                "evidence": evidence
            }

    total = bullish + bearish

    if total == 0:
        return {
            "direction": "WAIT",
            "score": 0,
            "label": "NO EDGE",
            "bullish_points": bullish,
            "bearish_points": bearish,
            "evidence": evidence
        }

    if bullish > bearish:
        direction = "UP"
        winning = bullish
    elif bearish > bullish:
        direction = "DOWN"
        winning = bearish
    else:
        direction = "WAIT"
        winning = 0

    # Normalize to a 0-10 alignment scale.
    ratio = winning / max(total, 1)

    score = round(
        ratio * 10
    )

    if quality < 60:
        direction = "WAIT"
        label = "LOW DATA QUALITY"

    elif direction == "WAIT":
        label = "MIXED"

    elif score >= 9:
        label = "VERY STRONG"

    elif score >= 8:
        label = "STRONG"

    elif score >= 7:
        label = "GOOD"

    elif score >= 5:
        label = "MODERATE"

    else:
        label = "WEAK"

    return {
        "direction": direction,
        "score": score,
        "label": label,
        "bullish_points": bullish,
        "bearish_points": bearish,
        "evidence": evidence
    }


# ============================================================
# FULL STATE
# ============================================================

def collect_state():
    spot = get_spot_feeds()

    btc = spot.get(
        "price"
    )

    feeds = spot.get(
        "feeds",
        []
    )

    candles = get_history()

    momentum = momentum_data(
        candles
    )

    structure = structure_data(
        candles
    )

    rsi = calculate_rsi(
        candles
    )

    kalshi = kalshi_data()

    market = kalshi.get(
        "market"
    )

    target = kalshi.get(
        "target"
    )

    yes_probability = kalshi.get(
        "yes_probability"
    )

    countdown = countdown_seconds(
        market.get("close_time")
        if market
        else None
    )

    quality = calculate_quality(
        btc,
        feeds,
        candles,
        market
    )

    buy_sell = get_buy_sell_pressure()

    order_book = get_order_book_pressure()

    reversal = reversal_risk(
        momentum
    )

    signal = build_signal(
        btc=btc,
        target=target,
        momentum=momentum,
        structure=structure,
        yes_probability=yes_probability,
        quality=quality.get("score", 0),
        buy_sell=buy_sell,
        order_book=order_book
    )

    memory = update_signal_memory(
        signal
    )

    memory_stats = memory_analysis()

    strength = prediction_strength(
        btc=btc,
        target=target,
        momentum=momentum,
        structure=structure,
        buy_sell=buy_sell,
        order_book=order_book,
        yes_probability=yes_probability,
        reversal=reversal,
        quality=quality.get("score", 0),
        memory=memory_stats
    )

    return {
        "updated": datetime.now(
            timezone.utc
        ).isoformat(),

        "btc": btc,

        "btc_formatted": fmt_price(
            btc
        ),

        "target": target,

        "target_formatted": fmt_price(
            target
        ),

        "btc_vs_target": (
            btc - target
            if btc is not None
            and target is not None
            else None
        ),

        "btc_vs_target_pct": (
            pct_change(
                btc,
                target
            )
            if btc is not None
            and target is not None
            else None
        ),

        "feeds": feeds,

        "market": market,

        "kalshi": {
            "ticker": kalshi.get("ticker"),
            "yes_probability": yes_probability,
            "yes_percent": (
                yes_probability * 100
                if yes_probability is not None
                else None
            )
        },

        "countdown": countdown,

        "countdown_formatted":
            format_countdown(
                countdown
            ),

        "candles": candles[-120:],

        "momentum": momentum,

        "rsi": rsi,

        "structure": structure,

        "reversal": reversal,

        "signal": signal,

        "prediction_strength":
            strength,

        "buy_sell":
            buy_sell,

        "order_book":
            order_book,

        "data_quality":
            quality,

        "memory":
            memory_stats,

        "feed_health":
            feed_health
    }


# ============================================================
# CACHED STATE
# ============================================================

def get_state():
    now = time.time()

    if (
        cache["state"] is not None
        and now - cache["time"] < CACHE_SECONDS
    ):
        return cache["state"]

    state = collect_state()

    cache["state"] = state
    cache["time"] = now

    return state


# ============================================================
# DASHBOARD HTML
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>

<meta charset="UTF-8">

<meta
    name="viewport"
    content="width=device-width,
    initial-scale=1.0"
>

<title>BTC Strike AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background:
        radial-gradient(
            circle at top,
            #162033 0%,
            #080b12 42%,
            #05070b 100%
        );
    color: #f4f7fb;
    font-family:
        Arial,
        Helvetica,
        sans-serif;
}

.container {
    max-width: 1200px;
    margin: auto;
    padding: 18px;
}

.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 15px;
    margin-bottom: 18px;
}

.title {
    font-size: 27px;
    font-weight: 900;
    letter-spacing: .5px;
}

.subtitle {
    color: #8995a8;
    font-size: 13px;
    margin-top: 4px;
}

.status {
    padding: 8px 12px;
    border-radius: 999px;
    background: #111827;
    border: 1px solid #263247;
    color: #aab6c8;
    font-size: 12px;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(
            4,
            minmax(
                0,
                1fr
            )
        );
    gap: 12px;
}

.card {
    background:
        linear-gradient(
            145deg,
            rgba(20,28,42,.96),
            rgba(10,14,22,.96)
        );

    border:
        1px solid #263247;

    border-radius: 16px;

    padding: 16px;

    box-shadow:
        0 10px 30px
        rgba(0,0,0,.28);
}

.card.wide {
    grid-column:
        span 2;
}

.card.full {
    grid-column:
        1 / -1;
}

.label {
    color: #8e9aae;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
}

.value {
    margin-top: 8px;
    font-size: 24px;
    font-weight: 900;
}

.small {
    font-size: 13px;
    color: #9ca9bb;
    margin-top: 6px;
}

.signal {
    border-radius: 20px;
    padding: 28px;
    text-align: center;
    border: 2px solid #2b3547;
    background: #0c111a;
}

.signal.up {
    border-color: #19d36b;
    box-shadow:
        0 0 35px
        rgba(25,211,107,.12);
}

.signal.down {
    border-color: #ff405c;
    box-shadow:
        0 0 35px
        rgba(255,64,92,.12);
}

.signal.wait {
    border-color: #f0c44f;
    box-shadow:
        0 0 35px
        rgba(240,196,79,.08);
}

.verdict {
    font-size: 52px;
    font-weight: 1000;
    letter-spacing: 1px;
}

.signal.up .verdict {
    color: #22e878;
}

.signal.down .verdict {
    color: #ff4c67;
}

.signal.wait .verdict {
    color: #f4ce55;
}

.confidence {
    margin-top: 8px;
    font-size: 20px;
    font-weight: 800;
}

.badge {
    display: inline-block;
    margin-top: 10px;
    padding: 7px 12px;
    border-radius: 999px;
    background: #111827;
    border: 1px solid #2c394e;
    color: #c5cfdd;
    font-size: 12px;
    font-weight: 700;
}

.metric-row {
    display: grid;
    grid-template-columns:
        repeat(
            3,
            1fr
        );
    gap: 8px;
    margin-top: 12px;
}

.metric {
    background: #0a0f17;
    border: 1px solid #202b3c;
    border-radius: 11px;
    padding: 10px;
}

.metric .v {
    margin-top: 5px;
    font-size: 17px;
    font-weight: 800;
}

.up-text {
    color: #21df72;
}

.down-text {
    color: #ff5069;
}

.wait-text {
    color: #f4ce55;
}

.bar {
    width: 100%;
    height: 15px;
    margin-top: 12px;
    border-radius: 999px;
    overflow: hidden;
    background:
        linear-gradient(
            90deg,
            #ff405c 0%,
            #ff405c 50%,
            #20df72 50%,
            #20df72 100%
        );
    position: relative;
}

.bar-marker {
    position: absolute;
    top: 0;
    width: 4px;
    height: 100%;
    background: white;
    box-shadow:
        0 0 8px rgba(255,255,255,.8);
}

.battle-bar {
    display: flex;
    height: 17px;
    border-radius: 999px;
    overflow: hidden;
    margin-top: 12px;
    background: #171c25;
}

.buy-bar {
    background: #1fe071;
    transition: width .3s ease;
}

.sell-bar {
    background: #ff405c;
    transition: width .3s ease;
}

.battle-numbers {
    display: flex;
    justify-content: space-between;
    margin-top: 8px;
    font-size: 13px;
    font-weight: 800;
}

.strength-score {
    font-size: 42px;
    font-weight: 1000;
    margin-top: 5px;
}

table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 10px;
}

td, th {
    padding: 8px;
    border-bottom:
        1px solid #202b3b;
    text-align: left;
}

th {
    color: #7f8ca0;
    font-size: 11px;
    text-transform: uppercase;
}

td {
    font-size: 13px;
}

.footer {
    color: #647186;
    text-align: center;
    font-size: 11px;
    margin-top: 18px;
}

@media(max-width: 850px) {
    .grid {
        grid-template-columns:
            repeat(
                2,
                minmax(
                    0,
                    1fr
                )
            );
    }

    .card.wide {
        grid-column:
            span 2;
    }
}

@media(max-width: 600px) {
    .grid {
        grid-template-columns: 1fr;
    }

    .card.wide,
    .card.full {
        grid-column:
            1 / -1;
    }

    .verdict {
        font-size: 42px;
    }
}

</style>

</head>

<body>

<div class="container">

    <div class="header">

        <div>
            <div class="title">
                ₿ BTC STRIKE AI
            </div>

            <div class="subtitle">
                15-Minute Market Intelligence Dashboard
            </div>
        </div>

        <div
            id="status"
            class="status"
        >
            Connecting...
        </div>

    </div>


    <div
        id="signalCard"
        class="signal wait"
    >

        <div class="label">
            Current Verdict
        </div>

        <div
            id="verdict"
            class="verdict"
        >
            WAIT
        </div>

        <div
            id="confidence"
            class="confidence"
        >
            —
        </div>

        <div
            id="signalLabel"
            class="badge"
        >
            Loading
        </div>

    </div>


    <br>


    <div class="grid">

        <div class="card">

            <div class="label">
                BTC Reference
            </div>

            <div
                id="btc"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                Kalshi Target
            </div>

            <div
                id="target"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                BTC vs Target
            </div>

            <div
                id="distance"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                Countdown
            </div>

            <div
                id="countdown"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card wide">

            <div class="label">
                Multi-Timeframe Momentum
            </div>

            <div class="metric-row">

                <div class="metric">
                    <div class="label">
                        1 Minute
                    </div>

                    <div
                        id="m1"
                        class="v"
                    >
                        —
                    </div>
                </div>

                <div class="metric">
                    <div class="label">
                        5 Minutes
                    </div>

                    <div
                        id="m5"
                        class="v"
                    >
                        —
                    </div>
                </div>

                <div class="metric">
                    <div class="label">
                        15 Minutes
                    </div>

                    <div
                        id="m15"
                        class="v"
                    >
                        —
                    </div>
                </div>

            </div>

        </div>


        <div class="card wide">

            <div class="label">
                Price Structure
            </div>

            <div
                id="structure"
                class="value"
            >
                —
            </div>

            <div
                id="reversal"
                class="small"
            >
                Reversal Risk: —
            </div>

        </div>


        <div class="card wide">

            <div class="label">
                ⚔️ Buyer / Seller Battle
            </div>

            <div
                id="battleWinner"
                class="value"
            >
                —
            </div>

            <div
                id="battleBar"
                class="battle-bar"
            >
                <div
                    id="buyBar"
                    class="buy-bar"
                    style="width:50%"
                ></div>

                <div
                    id="sellBar"
                    class="sell-bar"
                    style="width:50%"
                ></div>
            </div>

            <div class="battle-numbers">

                <span
                    id="buyerPct"
                    class="up-text"
                >
                    Buyers —
                </span>

                <span
                    id="sellerPct"
                    class="down-text"
                >
                    Sellers —
                </span>

            </div>

            <div
                id="battleDelta"
                class="small"
            >
                Delta: —
            </div>

            <div
                id="tradeCount"
                class="small"
            >
                Trades: —
            </div>

        </div>


        <div class="card wide">

            <div class="label">
                📖 Order-Book Pressure
            </div>

            <div
                id="bookWinner"
                class="value"
            >
                —
            </div>

            <div class="battle-bar">

                <div
                    id="bidBar"
                    class="buy-bar"
                    style="width:50%"
                ></div>

                <div
                    id="askBar"
                    class="sell-bar"
                    style="width:50%"
                ></div>

            </div>

            <div class="battle-numbers">

                <span
                    id="bidPct"
                    class="up-text"
                >
                    Bids —
                </span>

                <span
                    id="askPct"
                    class="down-text"
                >
                    Asks —
                </span>

            </div>

            <div
                id="bookPressure"
                class="small"
            >
                Pressure: —
            </div>

        </div>


        <div class="card wide">

            <div class="label">
                🧠 Prediction Strength
            </div>

            <div
                id="strengthScore"
                class="strength-score"
            >
                —
            </div>

            <div
                id="strengthLabel"
                class="badge"
            >
                —
            </div>

            <div
                id="strengthDirection"
                class="small"
            >
                Direction: —
            </div>

            <div
                id="strengthPoints"
                class="small"
            >
                Bullish: — | Bearish: —
            </div>

        </div>


        <div class="card">

            <div class="label">
                Kalshi YES
            </div>

            <div
                id="yes"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                RSI
            </div>

            <div
                id="rsi"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                Data Quality
            </div>

            <div
                id="quality"
                class="value"
            >
                —
            </div>

            <div
                id="qualityLabel"
                class="small"
            >
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                Live Feeds
            </div>

            <div
                id="feeds"
                class="value"
            >
                —
            </div>

        </div>


        <div class="card full">

            <div class="label">
                Signal Breakdown
            </div>

            <table>

                <thead>
                    <tr>
                        <th>UP Evidence</th>
                        <th>DOWN Evidence</th>
                    </tr>
                </thead>

                <tbody>

                    <tr>
                        <td id="upReasons">
                            —
                        </td>

                        <td id="downReasons">
                            —
                        </td>
                    </tr>

                </tbody>

            </table>

        </div>

    </div>


    <div class="footer">
        BTC Strike AI • BRTI-style composite logic •
        Signal strength is evidence alignment, not a guaranteed outcome.
    </div>

</div>


<script>

function money(v) {

    if (
        v === null ||
        v === undefined ||
        isNaN(v)
    ) {
        return "—";
    }

    return "$" + Number(v).toLocaleString(
        undefined,
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    );
}


function pct(v, digits=3) {

    if (
        v === null ||
        v === undefined ||
        isNaN(v)
    ) {
        return "—";
    }

    const n = Number(v);

    return (
        (n >= 0 ? "+" : "") +
        n.toFixed(digits) +
        "%"
    );
}


function num(v, digits=2) {

    if (
        v === null ||
        v === undefined ||
        isNaN(v)
    ) {
        return "—";
    }

    return Number(v).toFixed(
        digits
    );
}


function colorize(el, value) {

    el.classList.remove(
        "up-text",
        "down-text",
        "wait-text"
    );

    if (
        value === null ||
        value === undefined ||
        isNaN(value)
    ) {
        return;
    }

    if (Number(value) > 0) {
        el.classList.add(
            "up-text"
        );
    }

    else if (
        Number(value) < 0
    ) {
        el.classList.add(
            "down-text"
        );
    }

    else {
        el.classList.add(
            "wait-text"
        );
    }
}


function setText(id, value) {

    const el = document.getElementById(
        id
    );

    if (el) {
        el.textContent = value;
    }
}


function render(data) {

    if (!data) {
        return;
    }

    const signal =
        data.signal || {};

    const verdict =
        signal.verdict || "WAIT";

    const card =
        document.getElementById(
            "signalCard"
        );

    card.classList.remove(
        "up",
        "down",
        "wait"
    );

    if (verdict === "UP") {
        card.classList.add(
            "up"
        );
    }

    else if (verdict === "DOWN") {
        card.classList.add(
            "down"
        );
    }

    else {
        card.classList.add(
            "wait"
        );
    }


    setText(
        "verdict",
        verdict === "UP"
            ? "🟢 UP"
            : verdict === "DOWN"
                ? "🔴 DOWN"
                : "🟡 WAIT"
    );


    setText(
        "confidence",
        "Confidence " +
        (signal.confidence ?? "—") +
        "%"
    );


    setText(
        "signalLabel",
        signal.label ||
        "WAIT / UNDECIDED"
    );


    setText(
        "btc",
        money(data.btc)
    );


    setText(
        "target",
        money(data.target)
    );


    const distance =
        data.btc_vs_target;

    const distancePct =
        data.btc_vs_target_pct;


    setText(
        "distance",
        distance === null ||
        distance === undefined
            ? "—"
            :
            (
                (distance >= 0
                    ? "+"
                    : "") +
                money(distance)
                .replace("$", "$")
            )
    );


    if (
        distancePct !== null &&
        distancePct !== undefined
    ) {

        setText(
            "distance",
            (
                distance >= 0
                    ? "+"
                    : ""
            ) +
            money(
                Math.abs(distance)
            ) +
            " (" +
            pct(distancePct, 3) +
            ")"
        );

    }


    setText(
        "countdown",
        data.countdown_formatted ||
        "—"
    );


    const momentum =
        data.momentum || {};


    setText(
        "m1",
        pct(momentum.m1)
    );

    setText(
        "m5",
        pct(momentum.m5)
    );

    setText(
        "m15",
        pct(momentum.m15)
    );


    colorize(
        document.getElementById("m1"),
        momentum.m1
    );

    colorize(
        document.getElementById("m5"),
        momentum.m5
    );

    colorize(
        document.getElementById("m15"),
        momentum.m15
    );


    const structure =
        data.structure || {};

    setText(
        "structure",
        structure.label ||
        "—"
    );


    const reversal =
        data.reversal || {};

    setText(
        "reversal",
        "Reversal Risk: " +
        (reversal.label || "—")
    );


    // ========================================================
    // BUYER / SELLER BATTLE
    // ========================================================

    const battle =
        data.buy_sell || {};

    setText(
        "battleWinner",
        battle.winner || "—"
    );

    setText(
        "buyerPct",
        "Buyers " +
        num(
            battle.buy_pct,
            1
        ) +
        "%"
    );

    setText(
        "sellerPct",
        "Sellers " +
        num(
            battle.sell_pct,
            1
        ) +
        "%"
    );

    setText(
        "battleDelta",
        "Delta: " +
        (
            battle.delta === null ||
            battle.delta === undefined
                ? "—"
                :
                money(
                    battle.delta
                )
        )
    );

    setText(
        "tradeCount",
        "Trades: " +
        (
            battle.trade_count ??
            "—"
        )
    );


    if (
        battle.buy_pct !== null &&
        battle.buy_pct !== undefined
    ) {

        document.getElementById(
            "buyBar"
        ).style.width =
            Number(
                battle.buy_pct
            ) + "%";

        document.getElementById(
            "sellBar"
        ).style.width =
            Number(
                battle.sell_pct
            ) + "%";
    }


    // ========================================================
    // ORDER BOOK
    // ========================================================

    const book =
        data.order_book || {};

    setText(
        "bookWinner",
        book.winner || "—"
    );

    setText(
        "bidPct",
        "Bids " +
        num(
            book.bid_pct,
            1
        ) +
        "%"
    );

    setText(
        "askPct",
        "Asks " +
        num(
            book.ask_pct,
            1
        ) +
        "%"
    );

    setText(
        "bookPressure",
        "Pressure: " +
        num(
            book.pressure,
            1
        ) +
        "%"
    );


    if (
        book.bid_pct !== null &&
        book.bid_pct !== undefined
    ) {

        document.getElementById(
            "bidBar"
        ).style.width =
            Number(
                book.bid_pct
            ) + "%";

        document.getElementById(
            "askBar"
        ).style.width =
            Number(
                book.ask_pct
            ) + "%";
    }


    // ========================================================
    // PREDICTION STRENGTH
    // ========================================================

    const strength =
        data.prediction_strength ||
        {};

    setText(
        "strengthScore",
        strength.score !== undefined
            ? strength.score + "/10"
            : "—"
    );

    setText(
        "strengthLabel",
        strength.label ||
        "—"
    );

    setText(
        "strengthDirection",
        "Direction: " +
        (
            strength.direction ||
            "WAIT"
        )
    );

    setText(
        "strengthPoints",
        "Bullish: " +
        (
            strength.bullish_points ??
            "—"
        ) +
        " | Bearish: " +
        (
            strength.bearish_points ??
            "—"
        )
    );


    colorize(
        document.getElementById(
            "strengthScore"
        ),
        strength.direction === "UP"
            ? 1
            : strength.direction === "DOWN"
                ? -1
                : 0
    );


    // ========================================================
    // KALSHI / RSI / QUALITY
    // ========================================================

    const kalshi =
        data.kalshi || {};

    setText(
        "yes",
        kalshi.yes_percent === null ||
        kalshi.yes_percent === undefined
            ? "—"
            :
            Number(
                kalshi.yes_percent
            ).toFixed(1) + "%"
    );


    setText(
        "rsi",
        data.rsi === null ||
        data.rsi === undefined
            ? "—"
            :
            Number(
                data.rsi
            ).toFixed(1)
    );


    const quality =
        data.data_quality || {};

    setText(
        "quality",
        (
            quality.score ??
            "—"
        ) + "/100"
    );

    setText(
        "qualityLabel",
        quality.label ||
        "—"
    );


    setText(
        "feeds",
        (
            data.feeds ||
            []
        ).length
    );


    setText(
        "upReasons",
        (
            signal.reasons_up ||
            []
        ).join(" • ") ||
        "None"
    );


    setText(
        "downReasons",
        (
            signal.reasons_down ||
            []
        ).join(" • ") ||
        "None"
    );


    setText(
        "status",
        "Live • " +
        new Date().toLocaleTimeString()
    );
}


async function refresh() {

    try {

        const response =
            await fetch(
                "/api/state",
                {
                    cache: "no-store"
                }
            );

        if (!response.ok) {
            throw new Error(
                "HTTP " +
                response.status
            );
        }

        const data =
            await response.json();

        render(data);

    }

    catch (error) {

        setText(
            "status",
            "Connection issue"
        );

        console.error(error);
    }
}


refresh();

setInterval(
    refresh,
    2000
);

</script>

</body>
</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def index():
    return render_template_string(
        HTML
    )


@app.route("/api/state")
def api_state():
    try:
        return jsonify(
            get_state()
        )

    except Exception as e:

        return jsonify({
            "error": str(e),
            "updated": datetime.now(
                timezone.utc
            ).isoformat()
        }), 200


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "BTC Strike AI",
        "time": datetime.now(
            timezone.utc
        ).isoformat()
    })


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":
    port = int(
        os.getenv(
            "PORT",
            "5000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
