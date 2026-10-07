import os
import time
import statistics
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 2
HISTORY_REFRESH = 15
MARKET_HISTORY_MAX = 120

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2"
)

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Strike-AI/7.0"
})

cache = {
    "time": 0,
    "state": None
}

history_cache = {
    "time": 0,
    "candles": []
}

market_history = []
feed_health = {}


# =========================================================
# HELPERS
# =========================================================

def num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_json(url, params=None):
    try:
        r = session.get(
            url,
            params=params,
            timeout=TIMEOUT
        )
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def parse_time(value):
    if not value:
        return None

    try:
        dt = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt

    except Exception:
        return None


def seconds_left(value):
    dt = parse_time(value)

    if not dt:
        return None

    return max(
        0,
        int(
            dt.timestamp() - time.time()
        )
    )


def mark_feed(name, value):

    now = time.time()

    item = feed_health.setdefault(
        name,
        {}
    )

    if value is not None:
        item["last_success"] = now
        item["last_value"] = value
        item["online"] = True
    else:
        item["online"] = False

    return value


# =========================================================
# BTC LIVE FEEDS
# =========================================================

def get_spot_feeds():

    feeds = {}

    # Binance
    data = get_json(
        "https://api.binance.com/api/v3/ticker/price",
        {"symbol": "BTCUSDT"}
    )

    if isinstance(data, dict):
        value = num(
            data.get("price")
        )
    else:
        value = None

    feeds["Binance"] = mark_feed(
        "Binance",
        value
    )

    # Coinbase
    data = get_json(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    )

    try:
        value = num(
            data["data"]["amount"]
        )
    except Exception:
        value = None

    feeds["Coinbase"] = mark_feed(
        "Coinbase",
        value
    )

    # Kraken
    data = get_json(
        "https://api.kraken.com/0/public/Ticker",
        {"pair": "XBTUSD"}
    )

    try:
        result = data["result"]
        pair = next(iter(result))

        value = num(
            result[pair]["c"][0]
        )

    except Exception:
        value = None

    feeds["Kraken"] = mark_feed(
        "Kraken",
        value
    )

    # Bitstamp
    data = get_json(
        "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    )

    if isinstance(data, dict):
        value = num(
            data.get("last")
        )
    else:
        value = None

    feeds["Bitstamp"] = mark_feed(
        "Bitstamp",
        value
    )

    valid = [
        value
        for value in feeds.values()
        if value is not None
        and value > 0
    ]

    if not valid:
        return None, feeds

    median = statistics.median(
        valid
    )

    filtered = [
        value
        for value in valid
        if abs(value - median)
        / median <= 0.0035
    ]

    reference = statistics.median(
        filtered or valid
    )

    return reference, feeds


# =========================================================
# BTC DATA QUALITY
# =========================================================

def calculate_feed_quality(feeds):

    now = time.time()

    live = 0
    fresh = 0

    values = []

    details = {}

    for name, value in feeds.items():

        item = feed_health.get(
            name,
            {}
        )

        last_success = item.get(
            "last_success"
        )

        age = (
            now - last_success
            if last_success
            else None
        )

        online = (
            value is not None
        )

        is_fresh = (
            online
            and
            age is not None
            and
            age <= 20
        )

        if online:
            live += 1
            values.append(value)

        if is_fresh:
            fresh += 1

        details[name] = {
            "online": online,
            "fresh": is_fresh,
            "age": (
                round(age, 1)
                if age is not None
                else None
            ),
            "price": (
                round(value, 2)
                if value is not None
                else None
            )
        }

    spread = None

    if len(values) >= 2:

        median = statistics.median(
            values
        )

        if median:
            spread = (
                max(values)
                - min(values)
            ) / median * 100

    score = 0

    # Number of working feeds
    if live == 4:
        score += 35
    elif live == 3:
        score += 30
    elif live == 2:
        score += 20
    elif live == 1:
        score += 8

    # Freshness
    if fresh == 4:
        score += 25
    elif fresh == 3:
        score += 22
    elif fresh == 2:
        score += 15
    elif fresh == 1:
        score += 5

    # Exchange agreement
    if spread is not None:

        if spread <= 0.03:
            score += 30

        elif spread <= 0.08:
            score += 25

        elif spread <= 0.20:
            score += 15

        elif spread <= 0.35:
            score += 5

    # Require at least two independent feeds
    if live >= 2:
        score += 10

    score = min(
        100,
        score
    )

    if score >= 85:
        grade = "HIGH"

    elif score >= 70:
        grade = "GOOD"

    elif score >= 50:
        grade = "FAIR"

    else:
        grade = "LOW"

    return {
        "score": score,
        "grade": grade,
        "live": live,
        "fresh": fresh,
        "spread": (
            round(spread, 4)
            if spread is not None
            else None
        ),
        "details": details
    }


# =========================================================
# CANDLE HISTORY
# =========================================================

def get_candle_source(
    url,
    params,
    mode
):

    data = get_json(
        url,
        params
    )

    candles = []

    if mode == "coinbase":

        rows = (
            data
            if isinstance(data, list)
            else []
        )

        for row in rows:

            try:
                candles.append(
                    (
                        float(row[0]),
                        num(row[4])
                    )
                )
            except Exception:
                pass

    elif mode == "kraken":

        try:
            result = data["result"]

            pair = next(
                key
                for key in result
                if key != "last"
            )

            for row in result[pair][-21:]:

                candles.append(
                    (
                        float(row[0]),
                        num(row[4])
                    )
                )

        except Exception:
            pass

    elif mode == "binance":

        rows = (
            data
            if isinstance(data, list)
            else []
        )

        for row in rows:

            try:

                candles.append(
                    (
                        float(row[0]) / 1000,
                        num(row[4])
                    )
                )

            except Exception:
                pass

    candles = [
        (ts, close)
        for ts, close in candles
        if close is not None
    ]

    candles.sort()

    return candles


def get_history():

    now = time.time()

    if (
        history_cache["candles"]
        and
        now - history_cache["time"]
        < HISTORY_REFRESH
    ):

        return history_cache["candles"]

    end = datetime.now(
        timezone.utc
    )

    start = (
        end
        - timedelta(minutes=21)
    )

    sources = [

        (
            "Coinbase",
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            {
                "granularity": 60,
                "start": start.isoformat(),
                "end": end.isoformat()
            },
            "coinbase"
        ),

        (
            "Kraken",
            "https://api.kraken.com/0/public/OHLC",
            {
                "pair": "XBTUSD",
                "interval": 1
            },
            "kraken"
        ),

        (
            "Binance",
            "https://api.binance.com/api/v3/klines",
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 21
            },
            "binance"
        )
    ]

    best = []

    for (
        name,
        url,
        params,
        mode
    ) in sources:

        candles = get_candle_source(
            url,
            params,
            mode
        )

        if len(candles) >= 16:

            best = candles
            break

    history_cache["candles"] = best
    history_cache["time"] = now

    return best


def momentum(
    candles,
    minutes
):

    if len(candles) < 2:
        return None

    current_ts, current = candles[-1]

    target_ts = (
        current_ts
        - minutes * 60
    )

    previous = None

    for timestamp, close in candles:

        if timestamp <= target_ts:
            previous = close
        else:
            break

    if previous in (
        None,
        0
    ):
        return None

    return (
        (current - previous)
        / previous
    ) * 100


def price_structure(candles):

    if len(candles) < 8:
        return "WAIT"

    values = [
        close
        for _, close in candles[-8:]
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
# KALSHI
# =========================================================

def probability(
    market,
    *keys
):

    for key in keys:

        value = num(
            market.get(key)
        )

        if value is not None:

            if value > 1:
                return value / 100

            return value

    return None


def normalize_market(
    market
):

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

    return {

        "ticker":
            market.get("ticker"),

        "title":
            (
                market.get("title")
                or
                market.get("subtitle")
                or
                ""
            ),

        "yes_bid":
            probability(
                market,
                "yes_bid_dollars",
                "yes_bid"
            ),

        "yes_ask":
            probability(
                market,
                "yes_ask_dollars",
                "yes_ask"
            ),

        "last":
            probability(
                market,
                "last_price_dollars",
                "last_price"
            ),

        "floor_strike":
            num(
                market.get(
                    "floor_strike"
                )
            ),

        "cap_strike":
            num(
                market.get(
                    "cap_strike"
                )
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
            ),

        "status":
            market.get(
                "status"
            ),

        "raw":
            market
    }


def get_target(
    market
):

    if not market:
        return None

    for key in (
        "floor_strike",
        "cap_strike"
    ):

        value = market.get(key)

        if (
            value is not None
            and
            value > 1000
        ):

            return value

    raw = market.get(
        "raw",
        {}
    )

    for key, value in raw.items():

        key_text = str(
            key
        ).lower()

        if (
            "strike" in key_text
            or
            "target" in key_text
        ):

            value = num(value)

            if (
                value is not None
                and
                value > 1000
            ):

                return value

    return None


def get_kalshi():

    manual = os.getenv(
        "KALSHI_TICKER",
        ""
    ).strip()

    if manual:

        data = get_json(
            f"{KALSHI_BASE}/markets/{manual}"
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

                market["target"] = (
                    get_target(
                        market
                    )
                )

                return market

    data = get_json(
        f"{KALSHI_BASE}/markets",
        {
            "series_ticker":
                "KXBTC15M",
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

    if not markets:

        data = get_json(
            f"{KALSHI_BASE}/markets",
            {
                "status":
                    "open",
                "limit":
                    200
            }
        )

        all_markets = (
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

        markets = [
            market
            for market in all_markets
            if str(
                market.get(
                    "ticker",
                    ""
                )
            ).startswith(
                "KXBTC15M"
            )
        ]

    now = time.time()

    candidates = []

    for raw in markets:

        ticker = str(
            raw.get(
                "ticker",
                ""
            )
        )

        if not ticker.startswith(
            "KXBTC15M"
        ):
            continue

        close = parse_time(
            raw.get(
                "close_time"
            )
            or
            raw.get(
                "expiration_time"
            )
        )

        if (
            close
            and
            close.timestamp() > now
        ):

            candidates.append(
                raw
            )

    if not candidates:
        return None

    def close_timestamp(
        market
    ):

        dt = parse_time(
            market.get(
                "close_time"
            )
            or
            market.get(
                "expiration_time"
            )
        )

        if dt:
            return dt.timestamp()

        return float("inf")

    candidates.sort(
        key=close_timestamp
    )

    market = normalize_market(
        candidates[0]
    )

    if market:

        market["target"] = (
            get_target(
                market
            )
        )

    return market


def yes_mid(
    market
):

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
            bid + ask
        ) / 2

    return market.get(
        "last"
    )


# =========================================================
# KALSHI HISTORY
# =========================================================

def record_market(
    market
):

    mid = yes_mid(
        market
    )

    if mid is None:
        return

    now = time.time()

    ticker = market.get(
        "ticker"
    )

    market_history.append(
        (
            now,
            ticker,
            mid
        )
    )

    cutoff = (
        now - 20 * 60
    )

    while (
        market_history
        and
        market_history[0][0]
        < cutoff
    ):

        market_history.pop(0)

    if (
        len(market_history)
        > MARKET_HISTORY_MAX
    ):

        del market_history[
            :-MARKET_HISTORY_MAX
        ]


def kalshi_change(
    ticker,
    seconds=60
):

    if (
        not ticker
        or
        not market_history
    ):

        return None

    now = time.time()

    current = None
    previous = None

    for (
        timestamp,
        saved_ticker,
        mid
    ) in reversed(
        market_history
    ):

        if saved_ticker != ticker:
            continue

        if current is None:

            current = mid
            continue

        if (
            timestamp
            <= now - seconds
        ):

            previous = mid
            break

    if (
        current is None
        or
        previous is None
    ):

        return None

    return (
        current - previous
    ) * 100


# =========================================================
# KALSHI QUALITY
# =========================================================

def calculate_kalshi_quality(
    market
):

    if not market:

        return {
            "score": 0,
            "grade": "OFFLINE"
        }

    score = 0

    if market.get(
        "ticker"
    ):
        score += 30

    if market.get(
        "target"
    ) is not None:
        score += 25

    if yes_mid(
        market
    ) is not None:
        score += 25

    if market.get(
        "close_time"
    ):
        score += 20

    if score >= 85:
        grade = "HIGH"

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
# OVERALL DATA QUALITY
# =========================================================

def overall_quality(
    btc_quality,
    kalshi_quality,
    history_points
):

    score = (
        btc_quality["score"]
        * 0.60
        +
        kalshi_quality["score"]
        * 0.25
    )

    if history_points >= 20:
        score += 15

    elif history_points >= 16:
        score += 10

    elif history_points >= 10:
        score += 5

    score = round(
        min(
            100,
            score
        )
    )

    if score >= 85:
        grade = "HIGH"

    elif score >= 70:
        grade = "GOOD"

    elif score >= 50:
        grade = "FAIR"

    else:
        grade = "LOW"

    return score, grade


# =========================================================
# SMART MOMENTUM BRAIN
# =========================================================

def acceleration_state(
    m1,
    m5,
    m15
):

    if None in (
        m1,
        m5,
        m15
    ):

        return "UNKNOWN"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 < -0.01
    ):

        return "ACCELERATING DOWN"

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 > 0.01
    ):

        return "ACCELERATING UP"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 > 0.02
    ):

        return (
            "SHORT-TERM REVERSAL UP"
        )

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 < -0.02
    ):

        return (
            "SHORT-TERM REVERSAL DOWN"
        )

    return "STABLE / MIXED"


def reversal_state(
    m1,
    m5,
    m15
):

    if None in (
        m1,
        m5,
        m15
    ):

        return "UNKNOWN"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 > 0.02
    ):

        return (
            "HIGH — BULLISH REVERSAL"
        )

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 < -0.02
    ):

        return (
            "HIGH — BEARISH REVERSAL"
        )

    if (
        m15 < -0.04
        and
        m5 < 0
        and
        m1 > 0
    ):

        return (
            "MEDIUM — POSSIBLE BULLISH TURN"
        )

    if (
        m15 > 0.04
        and
        m5 > 0
        and
        m1 < 0
    ):

        return (
            "MEDIUM — POSSIBLE BEARISH TURN"
        )

    return "LOW"


# =========================================================
# DECISION ENGINE
# =========================================================

def build_signal(
    price,
    market,
    candles,
    quality_score
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

    acceleration = (
        acceleration_state(
            m1,
            m5,
            m15
        )
    )

    reversal = (
        reversal_state(
            m1,
            m5,
            m15
        )
    )

    result = {

        "verdict":
            "WAIT",

        "label":
            "WAIT",

        "confidence":
            0,

        "score":
            0,

        "bullish":
            0,

        "bearish":
            0,

        "agreement":
            "NO DATA",

        "ready":
            False,

        "structure":
            structure,

        "acceleration":
            acceleration,

        "reversal":
            reversal,

        "pressure":
            None,

        "kalshi_change":
            None,

        "warning":
            None,

        "reasons":
            []
    }

    if price is None:

        result["label"] = (
            "WAIT — NO BTC DATA"
        )

        result["reasons"] = [
            "BTC reference price unavailable."
        ]

        return result

    if market is None:

        result["label"] = (
            "WAIT — NO KALSHI DATA"
        )

        result["reasons"] = [
            "KXBTC15M market unavailable."
        ]

        return result

    target = market.get(
        "target"
    )

    if target is None:

        result["label"] = (
            "WAIT — NO STRIKE"
        )

        result["reasons"] = [
            "Kalshi target unavailable."
        ]

        return result

    if None in (
        m1,
        m5,
        m15
    ):

        result["label"] = (
            "WAIT — BUILDING HISTORY"
        )

        result["confidence"] = 25

        result["reasons"] = [
            "Building complete 1m/5m/15m history."
        ]

        return result

    bullish = 0
    bearish = 0

    bullish_reasons = []
    bearish_reasons = []
    neutral_reasons = []

    # BTC VS STRIKE

    distance_pct = (
        (price - target)
        / target
    ) * 100

    if distance_pct > 0.03:

        bullish += 1

        bullish_reasons.append(
            "BTC is above the Kalshi target."
        )

    elif distance_pct < -0.03:

        bearish += 1

        bearish_reasons.append(
            "BTC is below the Kalshi target."
        )

    else:

        neutral_reasons.append(
            "BTC is very close to the Kalshi target."
        )

    # 1 MINUTE

    if m1 > 0.01:

        bullish += 1

        bullish_reasons.append(
            "1m momentum is positive."
        )

    elif m1 < -0.01:

        bearish += 1

        bearish_reasons.append(
            "1m momentum is negative."
        )

    else:

        neutral_reasons.append(
            "1m momentum is neutral."
        )

    # 5 MINUTE

    if m5 > 0.02:

        bullish += 1

        bullish_reasons.append(
            "5m momentum is positive."
        )

    elif m5 < -0.02:

        bearish += 1

        bearish_reasons.append(
            "5m momentum is negative."
        )

    else:

        neutral_reasons.append(
            "5m momentum is neutral."
        )

    # 15 MINUTE

    if m15 > 0.04:

        bullish += 1

        bullish_reasons.append(
            "15m momentum is positive."
        )

    elif m15 < -0.04:

        bearish += 1

        bearish_reasons.append(
            "15m momentum is negative."
        )

    else:

        neutral_reasons.append(
            "15m momentum is neutral."
        )

    # STRUCTURE

    if structure == (
        "HIGHER HIGHS / HIGHER LOWS"
    ):

        bullish += 1

        bullish_reasons.append(
            "Price structure is bullish."
        )

    elif structure == (
        "LOWER HIGHS / LOWER LOWS"
    ):

        bearish += 1

        bearish_reasons.append(
            "Price structure is bearish."
        )

    else:

        neutral_reasons.append(
            "Price structure is mixed."
        )

    # KALSHI YES

    mid = yes_mid(
        market
    )

    pressure = None

    if mid is not None:

        pressure = (
            mid - 0.50
        ) * 200

        if mid >= 0.60:

            bullish += 1

            bullish_reasons.append(
                "Kalshi YES pricing favors UP."
            )

        elif mid <= 0.40:

            bearish += 1

            bearish_reasons.append(
                "Kalshi YES pricing favors DOWN."
            )

        else:

            neutral_reasons.append(
                "Kalshi YES pricing is near neutral."
            )

    ticker = market.get(
        "ticker"
    )

    record_market(
        market
    )

    k_change = kalshi_change(
        ticker,
        60
    )

    # ACCELERATION

    if acceleration == (
        "ACCELERATING UP"
    ):

        bullish += 1

        bullish_reasons.append(
            "Short-term momentum is accelerating upward."
        )

    elif acceleration == (
        "ACCELERATING DOWN"
    ):

        bearish += 1

        bearish_reasons.append(
            "Short-term momentum is accelerating downward."
        )

    # TIME

    remaining = seconds_left(
        market.get(
            "close_time"
        )
    )

    final_minute = (
        remaining is not None
        and
        remaining <= 60
    )

    final_three = (
        remaining is not None
        and
        remaining <= 180
    )

    difference = abs(
        bullish - bearish
    )

    # REVERSAL BRAKE

    if reversal.startswith(
        "HIGH"
    ):

        result["verdict"] = (
            "WAIT"
        )

        result["label"] = (
            "WAIT — REVERSAL RISK"
        )

        result["confidence"] = 58

        result["warning"] = reversal

    elif (
        bullish >= 5
        and
        bullish - bearish >= 3
    ):

        result["verdict"] = (
            "UP"
        )

        result["label"] = (
            "UP — STRONG CONFIRMATION"
        )

        result["confidence"] = 90

    elif (
        bearish >= 5
        and
        bearish - bullish >= 3
    ):

        result["verdict"] = (
            "DOWN"
        )

        result["label"] = (
            "DOWN — STRONG CONFIRMATION"
        )

        result["confidence"] = 90

    elif (
        bullish >= 4
        and
        bullish > bearish
        and
        difference >= 2
    ):

        result["verdict"] = (
            "UP"
        )

        result["label"] = (
            "UP — CONFIRMED"
        )

        result["confidence"] = 72

    elif (
        bearish >= 4
        and
        bearish > bullish
        and
        difference >= 2
    ):

        result["verdict"] = (
            "DOWN"
        )

        result["label"] = (
            "DOWN — CONFIRMED"
        )

        result["confidence"] = 72

    else:

        result["verdict"] = (
            "WAIT"
        )

        result["label"] = (
            "WAIT — CONFLICT"
        )

        result["confidence"] = 50

        result["warning"] = (
            "Signals are not aligned enough."
        )

    # FINAL MINUTE BRAKE

    if (
        final_minute
        and
        difference < 4
        and
        result["verdict"] != "WAIT"
    ):

        result["verdict"] = (
            "WAIT"
        )

        result["label"] = (
            "WAIT — FINAL MINUTE"
        )

        result["confidence"] = 55

        result["warning"] = (
            "Final 60 seconds require stronger confirmation."
        )

    elif (
        final_three
        and
        difference < 3
    ):

        result["warning"] = (
            "Final 3 minutes — reversal risk is elevated."
        )

    # DATA QUALITY BRAKE

    if quality_score < 50:

        result["verdict"] = (
            "WAIT"
        )

        result["label"] = (
            "WAIT — LOW DATA QUALITY"
        )

        result["confidence"] = 35

        result["warning"] = (
            "Not enough reliable live data to trust the signal."
        )

    elif quality_score < 70:

        result["confidence"] = min(
            result["confidence"],
            65
        )

        if result["verdict"] != "WAIT":

            result["warning"] = (
                "Data quality is FAIR; confidence reduced."
            )

    # ACCELERATION BOOST

    if (
        result["verdict"] == "UP"
        and
        acceleration == "ACCELERATING UP"
    ):

        result["confidence"] += 3

    if (
        result["verdict"] == "DOWN"
        and
        acceleration == "ACCELERATING DOWN"
    ):

        result["confidence"] += 3

    result["confidence"] = min(
        95,
        result["confidence"]
    )

    result["bullish"] = bullish
    result["bearish"] = bearish

    result["score"] = (
        bullish - bearish
    )

    result["agreement"] = (
        f"{bullish} BULLISH / "
        f"{bearish} BEARISH"
    )

    result["ready"] = (
        result["verdict"] != "WAIT"
    )

    result["pressure"] = (
        round(
            pressure,
            2
        )
        if pressure is not None
        else None
    )

    result["kalshi_change"] = (
        round(
            k_change,
            2
        )
        if k_change is not None
        else None
    )

    # EXPLANATION

    if result["verdict"] == "UP":

        reasons = (
            bullish_reasons[:]
        )

    elif result["verdict"] == "DOWN":

        reasons = (
            bearish_reasons[:]
        )

    else:

        reasons = (
            bullish_reasons[:3]
            +
            bearish_reasons[:3]
        )

        if not reasons:

            reasons = (
                neutral_reasons[:3]
            )

    reasons.append(
        f"Momentum state: {acceleration}."
    )

    reasons.append(
        f"Reversal risk: {reversal}."
    )

    if (
        k_change is not None
        and
        abs(k_change) >= 2
    ):

        reasons.append(
            "Kalshi YES moved "
            f"{k_change:+.1f}¢ "
            "over the last minute."
        )

    result["reasons"] = (
        reasons[:8]
    )

    return result


# =========================================================
# STATE
# =========================================================

def collect_state():

    btc, feeds = (
        get_spot_feeds()
    )

    candles = (
        get_history()
    )

    market = (
        get_kalshi()
    )

    btc_quality = (
        calculate_feed_quality(
            feeds
        )
    )

    if market:

        market["target"] = (
            get_target(
                market
            )
        )

    kalshi_quality = (
        calculate_kalshi_quality(
            market
        )
    )

    quality_score, quality_grade = (
        overall_quality(
            btc_quality,
            kalshi_quality,
            len(candles)
        )
    )

    signal = build_signal(
        btc,
        market,
        candles,
        quality_score
    )

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

    target = (
        market.get(
            "target"
        )
        if market
        else None
    )

    distance = None
    distance_pct = None

    if (
        btc is not None
        and
        target is not None
    ):

        distance = (
            btc - target
        )

        distance_pct = (
            distance / target
        ) * 100

    return {

        "ok":
            btc is not None,

        "updated":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "btc":
            (
                round(
                    btc,
                    2
                )
                if btc is not None
                else None
            ),

        "feeds": {
            key:
                (
                    round(
                        value,
                        2
                    )
                    if value is not None
                    else None
                )
            for key, value
            in feeds.items()
        },

        "feed_count":
            btc_quality["live"],

        "feed_quality":
            btc_quality,

        "history_points":
            len(candles),

        "kalshi_quality":
            kalshi_quality,

        "data_quality": {

            "score":
                quality_score,

            "grade":
                quality_grade
        },

        "kalshi": {

            "ticker":
                (
                    market.get(
                        "ticker"
                    )
                    if market
                    else None
                ),

            "target":
                (
                    round(
                        target,
                        2
                    )
                    if target
                    else None
                ),

            "yes_bid":
                (
                    market.get(
                        "yes_bid"
                    )
                    if market
                    else None
                ),

            "yes_ask":
                (
                    market.get(
                        "yes_ask"
                    )
                    if market
                    else None
                ),

            "yes_mid":
                yes_mid(
                    market
                ),

            "last":
                (
                    market.get(
                        "last"
                    )
                    if market
                    else None
                ),

            "close_time":
                (
                    market.get(
                        "close_time"
                    )
                    if market
                    else None
                ),

            "countdown":
                (
                    seconds_left(
                        market.get(
                            "close_time"
                        )
                    )
                    if market
                    else None
                )
        },

        "distance": {

            "dollars":
                (
                    round(
                        distance,
                        2
                    )
                    if distance is not None
                    else None
                ),

            "percent":
                (
                    round(
                        distance_pct,
                        4
                    )
                    if distance_pct is not None
                    else None
                )
        },

        "momentum": {

            "m1":
                (
                    round(
                        m1,
                        4
                    )
                    if m1 is not None
                    else None
                ),

            "m5":
                (
                    round(
                        m5,
                        4
                    )
                    if m5 is not None
                    else None
                ),

            "m15":
                (
                    round(
                        m15,
                        4
                    )
                    if m15 is not None
                    else None
                )
        },

        "signal":
            signal
    }


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def home():

    return render_template_string(
        PAGE
    )


@app.route("/api/state")
def api_state():

    now = time.time()

    if (
        cache["state"] is not None
        and
        now - cache["time"]
        < CACHE_SECONDS
    ):

        return jsonify(
            cache["state"]
        )

    state = collect_state()

    cache["state"] = state
    cache["time"] = now

    return jsonify(
        state
    )


# =========================================================
# DASHBOARD
# =========================================================

PAGE = r"""
<!doctype html>

<html>

<head>

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
 background:#070a0f;
 color:#f4f7fb;
 font-family:Arial,sans-serif
}

.wrap{
 max-width:1100px;
 margin:auto;
 padding:18px
}

.header{
 display:flex;
 justify-content:space-between;
 align-items:center;
 margin-bottom:15px
}

h1{
 margin:0;
 font-size:25px
}

.sub{
 color:#8994a5;
 font-size:12px;
 margin-top:4px
}

.status{
 padding:8px 12px;
 border-radius:20px;
 background:#111822;
 font-size:12px
}

.dot{
 display:inline-block;
 width:8px;
 height:8px;
 border-radius:50%;
 background:#20d879;
 margin-right:6px
}

.verdict{
 padding:24px;
 text-align:center;
 border-radius:18px;
 background:#211b0b;
 border:1px solid #e8b84d;
 margin-bottom:14px
}

.verdict.up{
 background:#092018;
 border-color:#18d77a
}

.verdict.down{
 background:#250c11;
 border-color:#ff4d5d
}

.verdict.wait{
 background:#211b0b;
 border-color:#e8b84d
}

.verdict-label{
 font-size:39px;
 font-weight:900
}

.conf{
 margin-top:7px;
 color:#aeb8c8
}

.agreement{
 margin-top:6px;
 font-size:16px;
 font-weight:800
}

.warning{
 margin-top:7px;
 color:#ffcf5a;
 font-size:12px;
 font-weight:700
}

.grid{
 display:grid;
 grid-template-columns:repeat(4,1fr);
 gap:12px
}

.card{
 background:#10151e;
 border:1px solid #202a38;
 border-radius:14px;
 padding:15px
}

.card h3{
 margin:0 0 8px;
 color:#8994a5;
 font-size:12px;
 text-transform:uppercase
}

.big{
 font-size:25px;
 font-weight:800
}

.value{
 font-size:17px;
 font-weight:700
}

.green{
 color:#20d879
}

.red{
 color:#ff5262
}

.yellow{
 color:#e8b84d
}

.small{
 color:#7f8998;
 font-size:11px;
 margin-top:5px
}

.wide{
 grid-column:span 2
}

.quality-row{
 display:flex;
 align-items:center;
 gap:15px
}

.quality-score{
 font-size:28px;
 font-weight:900
}

.bar{
 height:7px;
 background:#222b37;
 border-radius:10px;
 overflow:hidden;
 margin-top:10px
}

.bar-fill{
 height:100%;
 width:0%;
 background:#20d879;
 transition:.3s
}

.reasons{
 margin:14px 0 0;
 padding-left:20px;
 color:#c8d0dc;
 font-size:13px;
 line-height:1.7
}

.note{
 color:#697585;
 font-size:10px;
 margin-top:6px
}

@media(max-width:800px){

 .grid{
  grid-template-columns:repeat(2,1fr)
 }

 .wide{
  grid-column:span 2
 }

}

@media(max-width:500px){

 .wrap{
  padding:10px
 }

 .verdict-label{
  font-size:28px
 }

 .big{
  font-size:21px
 }

}

</style>

</head>

<body>

<div class="wrap">


<div class="header">

<div>

<h1>₿ BTC STRIKE AI</h1>

<div class="sub">
15-minute adaptive confirmation engine
</div>

</div>

<div class="status">

<span class="dot"></span>

<span id="status">
CONNECTING
</span>

</div>

</div>


<div
 id="verdict"
 class="verdict wait"
>

<div
 id="verdictLabel"
 class="verdict-label"
>
🟡 WAIT
</div>

<div class="conf">

Confidence:

<b id="confidence">
0%
</b>

</div>

<div
 id="agreement"
 class="agreement"
>
--
</div>

<div
 id="warning"
 class="warning"
>
</div>

</div>


<div class="grid">


<div class="card">

<h3>BTC Reference</h3>

<div
 id="btc"
 class="big"
>
--
</div>

<div
 id="feedCount"
 class="small"
>
-- feeds
</div>

</div>


<div class="card">

<h3>Kalshi Target</h3>

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

<h3>BTC vs Target</h3>

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

<h3>Countdown</h3>

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

<h3>1 Minute</h3>

<div
 id="m1"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>5 Minutes</h3>

<div
 id="m5"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>15 Minutes</h3>

<div
 id="m15"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>Structure</h3>

<div
 id="structure"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>Momentum State</h3>

<div
 id="acceleration"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>Reversal Risk</h3>

<div
 id="reversal"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>Kalshi YES</h3>

<div
 id="yesMid"
 class="value"
>
--
</div>

<div
 id="kalshiChange"
 class="small"
>
--
</div>

</div>


<div class="card">

<h3>Signal Score</h3>

<div
 id="score"
 class="value"
>
--
</div>

</div>


<div class="card wide">

<h3>
DATA QUALITY BRAIN
</h3>

<div class="quality-row">

<div
 id="qualityScore"
 class="quality-score"
>
--
</div>

<div>

<div id="qualityGrade">
--
</div>

<div class="small">
Feeds + freshness + exchange agreement + Kalshi + history
</div>

</div>

</div>

<div class="bar">

<div
 id="qualityBar"
 class="bar-fill"
>
</div>

</div>

<div
 id="qualityDetails"
 class="small"
>
--
</div>

</div>


<div class="card wide">

<h3>
WHY THE ENGINE CHOSE THIS
</h3>

<ul
 id="reasons"
 class="reasons"
>

<li>
Waiting for live data...
</li>

</ul>

</div>


<div class="card wide">

<h3>
FEED HEALTH
</h3>

<div
 id="feeds"
 class="small"
>
--
</div>

<div class="note">
BTC Reference is an exchange composite and is not the official CF Benchmarks BRTI.
</div>

</div>


</div>

</div>


<script>

function money(v){

 if(v==null)
  return "--";

 return "$"
 +
 Number(v).toLocaleString(
  undefined,
  {
   minimumFractionDigits:2,
   maximumFractionDigits:2
  }
 );

}


function pct(v){

 if(v==null)
  return "--";

 return (
  v>=0 ? "+" : ""
 )
 +
 Number(v).toFixed(3)
 +
 "%";

}


function color(v){

 if(v==null)
  return "";

 if(v>0)
  return "green";

 if(v<0)
  return "red";

 return "yellow";

}


function setValue(
 id,
 text,
 cls
){

 const e =
  document.getElementById(
   id
  );

 e.textContent =
  text;

 e.className =
  "value "
  +
  (cls || "");

}


function clock(seconds){

 if(seconds==null)
  return "--";

 seconds =
  Math.max(
   0,
   Math.floor(seconds)
  );

 return (
  String(
   Math.floor(
    seconds / 60
   )
  ).padStart(2,"0")
  +
  ":"
  +
  String(
   seconds % 60
  ).padStart(2,"0")
 );

}


function refresh(){

 fetch(
  "/api/state?t="
  +
  Date.now(),
  {
   cache:"no-store"
  }
 )

 .then(
  r => {

   if(!r.ok)
    throw new Error(
     "API "
     +
     r.status
    );

   return r.json();

  }
 )

 .then(
  d => {

   document.getElementById(
    "status"
   ).textContent =
    d.ok
     ? "LIVE"
     : "DATA ERROR";


   document.getElementById(
    "btc"
   ).textContent =
    money(
     d.btc
    );


   document.getElementById(
    "target"
   ).textContent =
    money(
     d.kalshi.target
    );


   document.getElementById(
    "ticker"
   ).textContent =
    d.kalshi.ticker
    ||
    "--";


   document.getElementById(
    "feedCount"
   ).textContent =
    (
     d.feed_count
     ||
     0
    )
    +
    " feeds live";


   const distance =
    document.getElementById(
     "distance"
    );


   distance.textContent =
    money(
     d.distance.dollars
    );


   distance.className =
    "big "
    +
    color(
     d.distance.dollars
    );


   document.getElementById(
    "distancePct"
   ).textContent =
    pct(
     d.distance.percent
    );


   document.getElementById(
    "countdown"
   ).textContent =
    clock(
     d.kalshi.countdown
    );


   setValue(
    "m1",
    pct(
     d.momentum.m1
    ),
    color(
     d.momentum.m1
    )
   );


   setValue(
    "m5",
    pct(
     d.momentum.m5
    ),
    color(
     d.momentum.m5
    )
   );


   setValue(
    "m15",
    pct(
     d.momentum.m15
    ),
    color(
     d.momentum.m15
    )
   );


   setValue(
    "structure",
    d.signal.structure
    ||
    "--",
    ""
   );


   let accelClass =
    "yellow";


   if(
    d.signal.acceleration
    &&
    d.signal.acceleration.includes(
     "UP"
    )
   ){

    accelClass =
     "green";

   }


   if(
    d.signal.acceleration
    &&
    d.signal.acceleration.includes(
     "DOWN"
    )
   ){

    accelClass =
     "red";

   }


   setValue(
    "acceleration",
    d.signal.acceleration
    ||
    "--",
    accelClass
   );


   let reversalClass =
    "yellow";


   if(
    d.signal.reversal
    &&
    d.signal.reversal.includes(
     "BULLISH"
    )
   ){

    reversalClass =
     "green";

   }


   if(
    d.signal.reversal
    &&
    d.signal.reversal.includes(
     "BEARISH"
    )
   ){

    reversalClass =
     "red";

   }


   setValue(
    "reversal",
    d.signal.reversal
    ||
    "--",
    reversalClass
   );


   const mid =
    d.kalshi.yes_mid;


   setValue(
    "yesMid",

    mid==null
     ?
      "--"
     :
      (
       Number(mid)*100
      ).toFixed(1)
      +
      "%",

    mid==null
     ?
      ""
     :
      mid>=0.50
       ?
        "green"
       :
        "red"
   );


   document.getElementById(
    "kalshiChange"
   ).textContent =

    d.signal.kalshi_change==null
     ?
      "Building Kalshi history..."
     :
      "1m YES move: "
      +
      (
       d.signal.kalshi_change>=0
        ?
         "+"
        :
         ""
      )
      +
      Number(
       d.signal.kalshi_change
      ).toFixed(1)
      +
      "¢";


   setValue(
    "score",

    d.signal.score==null
     ?
      "--"
     :
      Number(
       d.signal.score
      ).toFixed(0),

    color(
     d.signal.score
    )
   );


   const verdict =
    d.signal.verdict
    ||
    "WAIT";


   const box =
    document.getElementById(
     "verdict"
    );


   box.className =
    "verdict "
    +
    (
     verdict==="UP"
      ?
       "up"
      :
     verdict==="DOWN"
      ?
       "down"
      :
       "wait"
    );


   document.getElementById(
    "verdictLabel"
   ).textContent =

    d.signal.label
    ||
    (
     verdict==="UP"
      ?
       "🟢 UP"
      :
     verdict==="DOWN"
      ?
       "🔴 DOWN"
      :
       "🟡 WAIT"
    );


   document.getElementById(
    "confidence"
   ).textContent =
    (
     d.signal.confidence
     ||
     0
    )
    +
    "%";


   document.getElementById(
    "agreement"
   ).textContent =
    d.signal.agreement
    ||
    "--";


   document.getElementById(
    "warning"
   ).textContent =
    d.signal.warning
    ||
    "";


   const reasons =
    document.getElementById(
     "reasons"
    );


   reasons.innerHTML =
    "";


   (
    d.signal.reasons
    ||
    [
     "Waiting for live data..."
    ]
   ).forEach(
    reason => {

     const li =
      document.createElement(
       "li"
      );

     li.textContent =
      reason;

     reasons.appendChild(
      li
     );

    }
   );


   /* DATA QUALITY */

   const quality =
    d.data_quality
    ||
    {};

   const qualityScore =
    quality.score
    ||
    0;


   document.getElementById(
    "qualityScore"
   ).textContent =
    qualityScore
    +
    "/100";


   document.getElementById(
    "qualityGrade"
   ).textContent =
    "QUALITY: "
    +
    (
     quality.grade
     ||
     "--"
    );


   document.getElementById(
    "qualityBar"
   ).style.width =
    qualityScore
    +
    "%";


   const fq =
    d.feed_quality
    ||
    {};


   document.getElementById(
    "qualityDetails"
   ).textContent =

    "BTC feeds: "
    +
    (
     fq.live
     ||
     0
    )
    +
    "/4 live • "
    +
    (
     fq.fresh
     ||
     0
    )
    +
    "/4 fresh • Exchange spread: "
    +
    (
     fq.spread==null
      ?
       "n/a"
      :
       Number(
        fq.spread
       ).toFixed(3)
       +
       "%"
    )
    +
    " • Kalshi: "
    +
    (
     d.kalshi_quality
     &&
     d.kalshi_quality.grade
      ?
       d.kalshi_quality.grade
      :
       "OFFLINE"
    )
    +
    " • History: "
    +
    (
     d.history_points
     ||
     0
    )
    +
    " candles";


   /* FEEDS */

   document.getElementById(
    "feeds"
   ).textContent =

    Object.entries(
     d.feeds
     ||
     {}
    )
    .map(
     ([name,value]) =>
      name
      +
      ": "
      +
      (
       value==null
        ?
         "OFFLINE"
        :
         money(value)
      )
    )
    .join(
     "  •  "
    )
    +
    "  •  History: "
    +
    (
     d.history_points
     ||
     0
    )
    +
    " candles";

  }
 )

 .catch(
  error => {

   document.getElementById(
    "status"
   ).textContent =
    "API ERROR";


   document.getElementById(
    "warning"
   ).textContent =
    error.message;

  }
 );

}


refresh();

setInterval(
 refresh,
 3000
);

</script>

</body>

</html>
"""


# =========================================================
# LOCAL RUN
# =========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        ),
        threaded=True
    )
