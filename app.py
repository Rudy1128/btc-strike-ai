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
    "https://api.elections.kalshi.com/trade-api/v2",
)

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Strike-AI/6.0"
})

cache = {
    "time": 0,
    "state": None,
}

history_cache = {
    "time": 0,
    "candles": [],
}

market_history = []


# =========================================================
# BASIC HELPERS
# =========================================================

def num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def get_json(url, params=None):
    try:
        response = session.get(
            url,
            params=params,
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        return response.json()
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
            dt = dt.replace(tzinfo=timezone.utc)

        return dt

    except Exception:
        return None


def seconds_left(value):
    dt = parse_time(value)

    if not dt:
        return None

    return max(
        0,
        int(dt.timestamp() - time.time())
    )


# =========================================================
# BTC LIVE PRICE
# =========================================================

def get_spot_feeds():

    feeds = {}

    # Binance
    data = get_json(
        "https://api.binance.com/api/v3/ticker/price",
        {"symbol": "BTCUSDT"},
    )

    if isinstance(data, dict):
        feeds["Binance"] = num(
            data.get("price")
        )
    else:
        feeds["Binance"] = None

    # Coinbase
    data = get_json(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    )

    try:
        feeds["Coinbase"] = num(
            data["data"]["amount"]
        )
    except Exception:
        feeds["Coinbase"] = None

    # Kraken
    data = get_json(
        "https://api.kraken.com/0/public/Ticker",
        {"pair": "XBTUSD"},
    )

    try:
        result = data["result"]
        pair = next(iter(result))

        feeds["Kraken"] = num(
            result[pair]["c"][0]
        )
    except Exception:
        feeds["Kraken"] = None

    # Bitstamp
    data = get_json(
        "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    )

    if isinstance(data, dict):
        feeds["Bitstamp"] = num(
            data.get("last")
        )
    else:
        feeds["Bitstamp"] = None

    valid = [
        value
        for value in feeds.values()
        if value is not None and value > 0
    ]

    if not valid:
        return None, feeds

    median = statistics.median(valid)

    filtered = [
        value
        for value in valid
        if abs(value - median) / median <= 0.0035
    ]

    reference = statistics.median(
        filtered or valid
    )

    return reference, feeds


# =========================================================
# 1-MINUTE HISTORY
# =========================================================

def coinbase_candles():

    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=21)

    data = get_json(
        "https://api.exchange.coinbase.com/products/BTC-USD/candles",
        {
            "granularity": 60,
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    )

    candles = []

    if isinstance(data, list):

        for row in data:

            try:
                timestamp = float(row[0])
                close = num(row[4])

                if close is not None:
                    candles.append(
                        (timestamp, close)
                    )

            except Exception:
                pass

    candles.sort()

    return candles


def kraken_candles():

    data = get_json(
        "https://api.kraken.com/0/public/OHLC",
        {
            "pair": "XBTUSD",
            "interval": 1,
        },
    )

    candles = []

    try:
        result = data["result"]

        pair = next(
            key
            for key in result
            if key != "last"
        )

        for row in result[pair][-21:]:

            timestamp = float(row[0])
            close = num(row[4])

            if close is not None:
                candles.append(
                    (timestamp, close)
                )

    except Exception:
        pass

    candles.sort()

    return candles


def binance_candles():

    data = get_json(
        "https://api.binance.com/api/v3/klines",
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "limit": 21,
        },
    )

    candles = []

    if isinstance(data, list):

        for row in data:

            try:
                timestamp = (
                    float(row[0]) / 1000
                )

                close = num(row[4])

                if close is not None:
                    candles.append(
                        (timestamp, close)
                    )

            except Exception:
                pass

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

    sources = [
        coinbase_candles,
        kraken_candles,
        binance_candles,
    ]

    best = []

    for source in sources:

        candles = source()

        if len(candles) >= 16:
            best = candles
            break

    history_cache["candles"] = best
    history_cache["time"] = now

    return best


def momentum(candles, minutes):

    if len(candles) < 2:
        return None

    current_time, current = candles[-1]

    target_time = (
        current_time - minutes * 60
    )

    previous = None

    for timestamp, close in candles:

        if timestamp <= target_time:
            previous = close
        else:
            break

    if previous in (None, 0):
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
        return "HIGHER HIGHS / HIGHER LOWS"

    if (
        max(second) < max(first)
        and
        min(second) < min(first)
    ):
        return "LOWER HIGHS / LOWER LOWS"

    return "MIXED"


# =========================================================
# KALSHI
# =========================================================

def probability(market, *keys):

    for key in keys:

        value = num(
            market.get(key)
        )

        if value is not None:

            if value > 1:
                return value / 100

            return value

    return None


def normalize_market(market):

    if (
        not isinstance(market, dict)
        or
        not market.get("ticker")
    ):
        return None

    return {
        "ticker": market.get("ticker"),

        "title": (
            market.get("title")
            or
            market.get("subtitle")
            or
            ""
        ),

        "yes_bid": probability(
            market,
            "yes_bid_dollars",
            "yes_bid",
        ),

        "yes_ask": probability(
            market,
            "yes_ask_dollars",
            "yes_ask",
        ),

        "last": probability(
            market,
            "last_price_dollars",
            "last_price",
        ),

        "floor_strike": num(
            market.get("floor_strike")
        ),

        "cap_strike": num(
            market.get("cap_strike")
        ),

        "close_time": (
            market.get("close_time")
            or
            market.get("expiration_time")
        ),

        "status": market.get("status"),

        "raw": market,
    }


def get_target(market):

    if not market:
        return None

    for key in (
        "floor_strike",
        "cap_strike",
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
        "",
    ).strip()

    if manual:

        data = get_json(
            f"{KALSHI_BASE}/markets/{manual}"
        )

        if isinstance(data, dict):

            market = normalize_market(
                data.get(
                    "market",
                    data,
                )
            )

            if market:

                market["target"] = (
                    get_target(market)
                )

                return market

    data = get_json(
        f"{KALSHI_BASE}/markets",
        {
            "series_ticker": "KXBTC15M",
            "status": "open",
            "limit": 100,
        },
    )

    markets = (
        data.get("markets", [])
        if isinstance(data, dict)
        else []
    )

    if not markets:

        data = get_json(
            f"{KALSHI_BASE}/markets",
            {
                "status": "open",
                "limit": 200,
            },
        )

        all_markets = (
            data.get("markets", [])
            if isinstance(data, dict)
            else []
        )

        markets = [
            market
            for market in all_markets
            if str(
                market.get(
                    "ticker",
                    "",
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
                "",
            )
        )

        if not ticker.startswith(
            "KXBTC15M"
        ):
            continue

        close = parse_time(
            raw.get("close_time")
            or
            raw.get("expiration_time")
        )

        if (
            close
            and
            close.timestamp() > now
        ):
            candidates.append(raw)

    if not candidates:
        return None

    def close_timestamp(market):

        dt = parse_time(
            market.get("close_time")
            or
            market.get("expiration_time")
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
            get_target(market)
        )

    return market


# =========================================================
# KALSHI HISTORY
# =========================================================

def yes_mid(market):

    if not market:
        return None

    bid = market.get("yes_bid")
    ask = market.get("yes_ask")

    if (
        bid is not None
        and
        ask is not None
    ):
        return (bid + ask) / 2

    return market.get("last")


def record_market(market):

    mid = yes_mid(market)

    if mid is None:
        return

    ticker = market.get("ticker")
    now = time.time()

    market_history.append(
        (
            now,
            ticker,
            mid,
        )
    )

    cutoff = now - 20 * 60

    while (
        market_history
        and
        market_history[0][0] < cutoff
    ):
        market_history.pop(0)

    if len(market_history) > MARKET_HISTORY_MAX:

        del market_history[
            :-MARKET_HISTORY_MAX
        ]


def kalshi_change(
    ticker,
    seconds=60,
):

    if not ticker:
        return None

    if not market_history:
        return None

    now = time.time()

    current = None
    previous = None

    for (
        timestamp,
        saved_ticker,
        mid,
    ) in reversed(market_history):

        if saved_ticker != ticker:
            continue

        if current is None:
            current = mid
            continue

        if timestamp <= now - seconds:

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
# SMART BRAIN
# =========================================================

def acceleration_state(
    m1,
    m5,
    m15,
):

    if None in (
        m1,
        m5,
        m15,
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
        return "SHORT-TERM REVERSAL UP"

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 < -0.02
    ):
        return "SHORT-TERM REVERSAL DOWN"

    return "STABLE / MIXED"


def reversal_state(
    m1,
    m5,
    m15,
):

    if None in (
        m1,
        m5,
        m15,
    ):
        return "UNKNOWN"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 > 0.02
    ):
        return "HIGH — BULLISH REVERSAL"

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 < -0.02
    ):
        return "HIGH — BEARISH REVERSAL"

    if (
        m15 < -0.04
        and
        m5 < 0
        and
        m1 > 0
    ):
        return "MEDIUM — POSSIBLE BULLISH TURN"

    if (
        m15 > 0.04
        and
        m5 > 0
        and
        m1 < 0
    ):
        return "MEDIUM — POSSIBLE BEARISH TURN"

    return "LOW"


def build_signal(
    price,
    market,
    candles,
):

    m1 = momentum(
        candles,
        1,
    )

    m5 = momentum(
        candles,
        5,
    )

    m15 = momentum(
        candles,
        15,
    )

    structure = price_structure(
        candles
    )

    acceleration = acceleration_state(
        m1,
        m5,
        m15,
    )

    reversal = reversal_state(
        m1,
        m5,
        m15,
    )

    result = {
        "verdict": "WAIT",
        "label": "WAIT",
        "confidence": 0,
        "score": 0,
        "bullish": 0,
        "bearish": 0,
        "agreement": "NO DATA",
        "ready": False,
        "structure": structure,
        "acceleration": acceleration,
        "reversal": reversal,
        "pressure": None,
        "kalshi_change": None,
        "warning": None,
        "reasons": [],
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
        m15,
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

    # -----------------------------------------------------
    # BTC VS TARGET
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 1M
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 5M
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 15M
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # STRUCTURE
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # KALSHI YES
    # -----------------------------------------------------

    mid = yes_mid(market)

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

    # -----------------------------------------------------
    # RECORD KALSHI MOVEMENT
    # -----------------------------------------------------

    ticker = market.get(
        "ticker"
    )

    record_market(
        market
    )

    k_change = kalshi_change(
        ticker,
        60,
    )

    # -----------------------------------------------------
    # ACCELERATION
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # TIME
    # -----------------------------------------------------

    remaining = seconds_left(
        market.get("close_time")
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

    # -----------------------------------------------------
    # REVERSAL SAFETY
    # -----------------------------------------------------

    if reversal.startswith("HIGH"):

        result["verdict"] = "WAIT"

        result["label"] = (
            "WAIT — REVERSAL RISK"
        )

        result["confidence"] = 58

        result["warning"] = reversal

    else:

        difference = abs(
            bullish - bearish
        )

        if (
            bullish >= 5
            and
            bullish - bearish >= 3
        ):

            result["verdict"] = "UP"

            result["label"] = (
                "UP — STRONG CONFIRMATION"
            )

            result["confidence"] = 90

        elif (
            bearish >= 5
            and
            bearish - bullish >= 3
        ):

            result["verdict"] = "DOWN"

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

            result["verdict"] = "UP"

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

            result["verdict"] = "DOWN"

            result["label"] = (
                "DOWN — CONFIRMED"
            )

            result["confidence"] = 72

        else:

            result["verdict"] = "WAIT"

            result["label"] = (
                "WAIT — CONFLICT"
            )

            result["confidence"] = 50

            result["warning"] = (
                "Signals are not aligned enough."
            )

        # Extra caution in final minute.
        if (
            final_minute
            and
            difference < 4
            and
            result["verdict"] != "WAIT"
        ):

            result["verdict"] = "WAIT"

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

    # -----------------------------------------------------
    # CONFIDENCE BOOST FOR ACCELERATION
    # -----------------------------------------------------

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
            2,
        )
        if pressure is not None
        else None
    )

    result["kalshi_change"] = (
        round(
            k_change,
            2,
        )
        if k_change is not None
        else None
    )

    # -----------------------------------------------------
    # EXPLANATION
    # -----------------------------------------------------

    if result["verdict"] == "UP":

        reasons = bullish_reasons[:]

    elif result["verdict"] == "DOWN":

        reasons = bearish_reasons[:]

    else:

        reasons = (
            bullish_reasons[:3]
            +
            bearish_reasons[:3]
        )

        if not reasons:
            reasons = neutral_reasons[:3]

    reasons.append(
        f"Momentum state: {acceleration}."
    )

    reasons.append(
        f"Reversal risk: {reversal}."
    )

    if k_change is not None:

        if abs(k_change) >= 2:

            reasons.append(
                "Kalshi YES moved "
                f"{k_change:+.1f}¢ "
                "over the last minute."
            )

    if final_minute:

        reasons.append(
            "Final 60 seconds: extra confirmation required."
        )

    elif final_three:

        reasons.append(
            "Final 3 minutes: time pressure is elevated."
        )

    result["reasons"] = reasons[:8]

    return result


# =========================================================
# STATE
# =========================================================

def collect_state():

    btc, feeds = get_spot_feeds()

    candles = get_history()

    market = get_kalshi()

    signal = build_signal(
        btc,
        market,
        candles,
    )

    m1 = momentum(
        candles,
        1,
    )

    m5 = momentum(
        candles,
        5,
    )

    m15 = momentum(
        candles,
        15,
    )

    target = (
        market.get("target")
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
                round(btc, 2)
                if btc is not None
                else None
            ),

        "feeds": {
            key: (
                round(value, 2)
                if value is not None
                else None
            )
            for key, value
            in feeds.items()
        },

        "feed_count":
            sum(
                value is not None
                for value
                in feeds.values()
            ),

        "history_points":
            len(candles),

        "kalshi": {

            "ticker":
                (
                    market.get("ticker")
                    if market
                    else None
                ),

            "target":
                (
                    round(target, 2)
                    if target
                    else None
                ),

            "yes_bid":
                (
                    market.get("yes_bid")
                    if market
                    else None
                ),

            "yes_ask":
                (
                    market.get("yes_ask")
                    if market
                    else None
                ),

            "yes_mid":
                yes_mid(market),

            "last":
                (
                    market.get("last")
                    if market
                    else None
                ),

            "close_time":
                (
                    market.get("close_time")
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
                ),
        },

        "distance": {

            "dollars":
                (
                    round(
                        distance,
                        2,
                    )
                    if distance is not None
                    else None
                ),

            "percent":
                (
                    round(
                        distance_pct,
                        4,
                    )
                    if distance_pct is not None
                    else None
                ),
        },

        "momentum": {

            "m1":
                (
                    round(
                        m1,
                        4,
                    )
                    if m1 is not None
                    else None
                ),

            "m5":
                (
                    round(
                        m5,
                        4,
                    )
                    if m5 is not None
                    else None
                ),

            "m15":
                (
                    round(
                        m15,
                        4,
                    )
                    if m15 is not None
                    else None
                ),
        },

        "signal":
            signal,
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

    return jsonify(state)


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
 box-sizing:border-box;
}

body{
 margin:0;
 background:#070a0f;
 color:#f4f7fb;
 font-family:Arial,sans-serif;
}

.wrap{
 max-width:1100px;
 margin:auto;
 padding:18px;
}

.header{
 display:flex;
 justify-content:space-between;
 align-items:center;
 margin-bottom:15px;
}

h1{
 margin:0;
 font-size:25px;
}

.sub{
 margin-top:4px;
 color:#8994a5;
 font-size:12px;
}

.status{
 padding:8px 12px;
 border-radius:20px;
 background:#111822;
 font-size:12px;
}

.dot{
 display:inline-block;
 width:8px;
 height:8px;
 border-radius:50%;
 background:#20d879;
 margin-right:6px;
}

.verdict{
 margin-bottom:14px;
 padding:24px;
 text-align:center;
 border-radius:18px;
 background:#111822;
 border:1px solid #202a38;
}

.verdict.up{
 background:#092018;
 border-color:#18d77a;
}

.verdict.down{
 background:#250c11;
 border-color:#ff4d5d;
}

.verdict.wait{
 background:#211b0b;
 border-color:#e8b84d;
}

.verdict-label{
 font-size:39px;
 font-weight:900;
}

.conf{
 margin-top:7px;
 color:#aeb8c8;
}

.agreement{
 margin-top:6px;
 font-size:16px;
 font-weight:800;
}

.warning{
 margin-top:7px;
 color:#ffcf5a;
 font-size:12px;
 font-weight:700;
}

.grid{
 display:grid;
 grid-template-columns:repeat(4,1fr);
 gap:12px;
}

.card{
 padding:15px;
 border-radius:14px;
 background:#10151e;
 border:1px solid #202a38;
}

.card h3{
 margin:0 0 8px;
 color:#8994a5;
 font-size:12px;
 text-transform:uppercase;
}

.big{
 font-size:25px;
 font-weight:800;
}

.value{
 font-size:17px;
 font-weight:700;
}

.green{
 color:#20d879;
}

.red{
 color:#ff5262;
}

.yellow{
 color:#e8b84d;
}

.small{
 margin-top:5px;
 color:#7f8998;
 font-size:11px;
}

.wide{
 grid-column:span 2;
}

.reasons{
 margin:14px 0 0;
 padding-left:20px;
 color:#c8d0dc;
 font-size:13px;
 line-height:1.7;
}

.note{
 margin-top:6px;
 color:#697585;
 font-size:10px;
}

@media(max-width:800px){

 .grid{
  grid-template-columns:repeat(2,1fr);
 }

 .wide{
  grid-column:span 2;
 }

}

@media(max-width:500px){

 .wrap{
  padding:10px;
 }

 .verdict-label{
  font-size:28px;
 }

 .big{
  font-size:21px;
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
<b id="confidence">0%</b>

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
Why The Engine Chose This
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
Feed Health
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

 const element =
  document.getElementById(id);

 element.textContent =
  text;

 element.className =
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
   Math.floor(seconds / 60)
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
  response => {

   if(!response.ok)
    throw new Error(
     "API "
     +
     response.status
    );

   return response.json();

  }
 )

 .then(
  data => {

   document.getElementById(
    "status"
   ).textContent =
    data.ok
     ? "LIVE"
     : "DATA ERROR";


   document.getElementById(
    "btc"
   ).textContent =
    money(data.btc);


   document.getElementById(
    "target"
   ).textContent =
    money(
     data.kalshi.target
    );


   document.getElementById(
    "ticker"
   ).textContent =
    data.kalshi.ticker
    || "--";


   document.getElementById(
    "feedCount"
   ).textContent =
    (
     data.feed_count
     || 0
    )
    +
    " feeds live";


   const distance =
    document.getElementById(
     "distance"
    );

   distance.textContent =
    money(
     data.distance.dollars
    );

   distance.className =
    "big "
    +
    color(
     data.distance.dollars
    );


   document.getElementById(
    "distancePct"
   ).textContent =
    pct(
     data.distance.percent
    );


   document.getElementById(
    "countdown"
   ).textContent =
    clock(
     data.kalshi.countdown
    );


   setValue(
    "m1",
    pct(
     data.momentum.m1
    ),
    color(
     data.momentum.m1
    )
   );


   setValue(
    "m5",
    pct(
     data.momentum.m5
    ),
    color(
     data.momentum.m5
    )
   );


   setValue(
    "m15",
    pct(
     data.momentum.m15
    ),
    color(
     data.momentum.m15
    )
   );


   setValue(
    "structure",
    data.signal.structure
    || "--",
    ""
   );


   let accelerationClass =
    "yellow";

   if(
    data.signal.acceleration
    &&
    data.signal.acceleration.includes(
     "UP"
    )
   ){
    accelerationClass =
     "green";
   }

   if(
    data.signal.acceleration
    &&
    data.signal.acceleration.includes(
     "DOWN"
    )
   ){
    accelerationClass =
     "red";
   }


   setValue(
    "acceleration",
    data.signal.acceleration
    || "--",
    accelerationClass
   );


   let reversalClass =
    "yellow";

   if(
    data.signal.reversal
    &&
    data.signal.reversal.includes(
     "BULLISH"
    )
   ){
    reversalClass =
     "green";
   }

   if(
    data.signal.reversal
    &&
    data.signal.reversal.includes(
     "BEARISH"
    )
   ){
    reversalClass =
     "red";
   }


   setValue(
    "reversal",
    data.signal.reversal
    || "--",
    reversalClass
   );


   const mid =
    data.kalshi.yes_mid;


   setValue(
    "yesMid",

    mid==null
     ? "--"
     :
     (
      Number(mid)*100
     ).toFixed(1)
     +
     "%",

    mid==null
     ? ""
     :
     mid>=0.50
      ? "green"
      : "red"
   );


   document.getElementById(
    "kalshiChange"
   ).textContent =

    data.signal.kalshi_change==null
     ?
     "Building Kalshi history..."
     :
     "1m YES move: "
     +
     (
      data.signal.kalshi_change>=0
       ? "+"
       : ""
     )
     +
     Number(
      data.signal.kalshi_change
     ).toFixed(1)
     +
     "¢";


   setValue(
    "score",

    data.signal.score==null
     ? "--"
     :
     Number(
      data.signal.score
     ).toFixed(0),

    color(
     data.signal.score
    )
   );


   const verdict =
    data.signal.verdict
    || "WAIT";


   const box =
    document.getElementById(
     "verdict"
    );


   box.className =
    "verdict "
    +
    (
     verdict==="UP"
      ? "up"
      :
     verdict==="DOWN"
      ? "down"
      :
      "wait"
    );


   document.getElementById(
    "verdictLabel"
   ).textContent =
    data.signal.label
    ||
    (
     verdict==="UP"
      ? "🟢 UP"
      :
     verdict==="DOWN"
      ? "🔴 DOWN"
      :
      "🟡 WAIT"
    );


   document.getElementById(
    "confidence"
   ).textContent =
    (
     data.signal.confidence
     || 0
    )
    +
    "%";


   document.getElementById(
    "agreement"
   ).textContent =
    data.signal.agreement
    || "--";


   document.getElementById(
    "warning"
   ).textContent =
    data.signal.warning
    || "";


   const reasons =
    document.getElementById(
     "reasons"
    );

   reasons.innerHTML = "";


   (
    data.signal.reasons
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


   document.getElementById(
    "feeds"
   ).textContent =

    Object.entries(
     data.feeds || {}
    )
    .map(
     ([name,value]) =>
      name
      +
      ": "
      +
      (
       value==null
        ? "OFFLINE"
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
    data.history_points
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


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        ),
        threaded=True,
    )
