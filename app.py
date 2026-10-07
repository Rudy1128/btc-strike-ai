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

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
)

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Strike-AI/4.0"
})

cache = {
    "time": 0,
    "state": None,
}

history_cache = {
    "time": 0,
    "candles": [],
}


# =========================================================
# HELPERS
# =========================================================

def f(value):
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
            str(value).replace(
                "Z",
                "+00:00",
            )
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
            dt.timestamp()
            - time.time()
        ),
    )


# =========================================================
# BTC SPOT
# =========================================================

def spot_feeds():

    feeds = {}

    # Binance
    data = get_json(
        "https://api.binance.com/api/v3/ticker/price",
        {"symbol": "BTCUSDT"},
    )

    if isinstance(data, dict):

        feeds["Binance"] = f(
            data.get("price")
        )

    # Coinbase
    data = get_json(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    )

    try:

        feeds["Coinbase"] = f(
            data["data"]["amount"]
        )

    except (
        TypeError,
        KeyError,
        IndexError,
    ):

        feeds["Coinbase"] = None

    # Kraken
    data = get_json(
        "https://api.kraken.com/0/public/Ticker",
        {"pair": "XBTUSD"},
    )

    try:

        result = data["result"]

        pair = next(
            iter(result)
        )

        feeds["Kraken"] = f(
            result[pair]["c"][0]
        )

    except (
        TypeError,
        KeyError,
        StopIteration,
        IndexError,
    ):

        feeds["Kraken"] = None

    # Bitstamp
    data = get_json(
        "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    )

    if isinstance(data, dict):

        feeds["Bitstamp"] = f(
            data.get("last")
        )

    else:

        feeds["Bitstamp"] = None

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
        if abs(value - median) / median
        <= 0.0035
    ]

    return (
        statistics.median(
            filtered or valid
        ),
        feeds,
    )


# =========================================================
# HISTORICAL CANDLES
# =========================================================

def coinbase_candles():

    end = datetime.now(
        timezone.utc
    )

    start = (
        end - timedelta(minutes=21)
    )

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

                timestamp = float(
                    row[0]
                )

                close = f(
                    row[4]
                )

                if close:

                    candles.append(
                        (
                            timestamp,
                            close,
                        )
                    )

            except (
                TypeError,
                IndexError,
            ):

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

            timestamp = float(
                row[0]
            )

            close = f(
                row[4]
            )

            if close:

                candles.append(
                    (
                        timestamp,
                        close,
                    )
                )

    except (
        TypeError,
        KeyError,
        StopIteration,
        IndexError,
    ):

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
                    float(row[0])
                    / 1000
                )

                close = f(
                    row[4]
                )

                if close:

                    candles.append(
                        (
                            timestamp,
                            close,
                        )
                    )

            except (
                TypeError,
                IndexError,
            ):

                pass

    candles.sort()

    return candles


def get_history():

    now = time.time()

    if (
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


def momentum(
    candles,
    minutes,
):

    if len(candles) < 2:

        return None

    current_time, current = (
        candles[-1]
    )

    target_time = (
        current_time
        - minutes * 60
    )

    previous = None

    for timestamp, close in candles:

        if timestamp <= target_time:

            previous = close

        else:

            break

    if previous in (
        None,
        0,
    ):

        return None

    return (
        (current - previous)
        / previous
    ) * 100


def get_structure(candles):

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
# KALSHI
# =========================================================

def probability(
    market,
    *keys,
):

    for key in keys:

        value = f(
            market.get(key)
        )

        if value is not None:

            if value > 1:

                return value / 100

            return value

    return None


def normalize_market(market):

    if (
        not isinstance(
            market,
            dict,
        )
        or not market.get("ticker")
    ):

        return None

    return {

        "ticker":
            market.get("ticker"),

        "title":
            (
                market.get("title")
                or market.get("subtitle")
                or ""
            ),

        "yes_bid":
            probability(
                market,
                "yes_bid_dollars",
                "yes_bid",
            ),

        "yes_ask":
            probability(
                market,
                "yes_ask_dollars",
                "yes_ask",
            ),

        "last":
            probability(
                market,
                "last_price_dollars",
                "last_price",
            ),

        "floor_strike":
            f(
                market.get(
                    "floor_strike"
                )
            ),

        "cap_strike":
            f(
                market.get(
                    "cap_strike"
                )
            ),

        "close_time":
            (
                market.get(
                    "close_time"
                )
                or market.get(
                    "expiration_time"
                )
            ),

        "status":
            market.get(
                "status"
            ),

        "raw":
            market,
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
            and value > 1000
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

            number = f(value)

            if (
                number is not None
                and number > 1000
            ):

                return number

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

        if isinstance(
            data,
            dict,
        ):

            market = normalize_market(
                data.get(
                    "market",
                    data,
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
                100,
        },
    )

    markets = (
        data.get(
            "markets",
            []
        )
        if isinstance(
            data,
            dict,
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
                    200,
            },
        )

        all_markets = (
            data.get(
                "markets",
                []
            )
            if isinstance(
                data,
                dict,
            )
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
            raw.get(
                "close_time"
            )
            or raw.get(
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
                raw
            )

    if not candidates:

        return None

    def close_timestamp(
        market,
    ):

        dt = parse_time(
            market.get(
                "close_time"
            )
            or market.get(
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


# =========================================================
# CONFIRMATION ENGINE
# =========================================================

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

    structure = get_structure(
        candles
    )

    bullish = 0
    bearish = 0

    bullish_items = []
    bearish_items = []
    neutral_items = []

    # -----------------------------------------------------
    # DATA GATES
    # -----------------------------------------------------

    if price is None:

        return {
            "verdict":
                "WAIT",

            "label":
                "WAIT — NO BTC DATA",

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

            "pressure":
                None,

            "reasons": [
                "BTC reference price unavailable."
            ],
        }

    if market is None:

        return {
            "verdict":
                "WAIT",

            "label":
                "WAIT — NO KALSHI DATA",

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

            "pressure":
                None,

            "reasons": [
                "KXBTC15M market unavailable."
            ],
        }

    target = market.get(
        "target"
    )

    if target is None:

        return {
            "verdict":
                "WAIT",

            "label":
                "WAIT — NO STRIKE",

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

            "pressure":
                None,

            "reasons": [
                "Kalshi target unavailable."
            ],
        }

    # -----------------------------------------------------
    # HISTORY GATE
    # -----------------------------------------------------

    if (
        m1 is None
        or m5 is None
        or m15 is None
    ):

        return {
            "verdict":
                "WAIT",

            "label":
                "WAIT — BUILDING HISTORY",

            "confidence":
                25,

            "score":
                0,

            "bullish":
                0,

            "bearish":
                0,

            "agreement":
                "BUILDING",

            "ready":
                False,

            "structure":
                structure,

            "pressure":
                None,

            "reasons": [
                "Building complete 1m/5m/15m candle history."
            ],
        }

    # -----------------------------------------------------
    # 1. BTC VS STRIKE
    # -----------------------------------------------------

    distance_pct = (
        (price - target)
        / target
    ) * 100

    if distance_pct > 0.03:

        bullish += 1

        bullish_items.append(
            "BTC is above the Kalshi target."
        )

    elif distance_pct < -0.03:

        bearish += 1

        bearish_items.append(
            "BTC is below the Kalshi target."
        )

    else:

        neutral_items.append(
            "BTC is very close to the Kalshi target."
        )

    # -----------------------------------------------------
    # 2. 1 MINUTE
    # -----------------------------------------------------

    if m1 > 0.01:

        bullish += 1

        bullish_items.append(
            "1m momentum is positive."
        )

    elif m1 < -0.01:

        bearish += 1

        bearish_items.append(
            "1m momentum is negative."
        )

    else:

        neutral_items.append(
            "1m momentum is neutral."
        )

    # -----------------------------------------------------
    # 3. 5 MINUTE
    # -----------------------------------------------------

    if m5 > 0.02:

        bullish += 1

        bullish_items.append(
            "5m momentum is positive."
        )

    elif m5 < -0.02:

        bearish += 1

        bearish_items.append(
            "5m momentum is negative."
        )

    else:

        neutral_items.append(
            "5m momentum is neutral."
        )

    # -----------------------------------------------------
    # 4. 15 MINUTE
    # -----------------------------------------------------

    if m15 > 0.04:

        bullish += 1

        bullish_items.append(
            "15m momentum is positive."
        )

    elif m15 < -0.04:

        bearish += 1

        bearish_items.append(
            "15m momentum is negative."
        )

    else:

        neutral_items.append(
            "15m momentum is neutral."
        )

    # -----------------------------------------------------
    # 5. STRUCTURE
    # -----------------------------------------------------

    if (
        structure
        == "HIGHER HIGHS / "
           "HIGHER LOWS"
    ):

        bullish += 1

        bullish_items.append(
            "Price structure is bullish."
        )

    elif (
        structure
        == "LOWER HIGHS / "
           "LOWER LOWS"
    ):

        bearish += 1

        bearish_items.append(
            "Price structure is bearish."
        )

    else:

        neutral_items.append(
            "Price structure is mixed."
        )

    # -----------------------------------------------------
    # 6. KALSHI YES
    # -----------------------------------------------------

    yes_bid = market.get(
        "yes_bid"
    )

    yes_ask = market.get(
        "yes_ask"
    )

    if (
        yes_bid is not None
        and yes_ask is not None
    ):

        yes_mid = (
            yes_bid
            + yes_ask
        ) / 2

    else:

        yes_mid = market.get(
            "last"
        )

    pressure = None

    if yes_mid is not None:

        pressure = (
            yes_mid
            - 0.50
        ) * 200

        if yes_mid >= 0.60:

            bullish += 1

            bullish_items.append(
                "Kalshi YES pricing favors UP."
            )

        elif yes_mid <= 0.40:

            bearish += 1

            bearish_items.append(
                "Kalshi YES pricing favors DOWN."
            )

        else:

            neutral_items.append(
                "Kalshi YES pricing is not strongly directional."
            )

    # -----------------------------------------------------
    # CONFIRMATION
    # -----------------------------------------------------

    total = (
        bullish
        + bearish
    )

    if total == 0:

        return {
            "verdict":
                "WAIT",

            "label":
                "WAIT — NEUTRAL",

            "confidence":
                35,

            "score":
                0,

            "bullish":
                bullish,

            "bearish":
                bearish,

            "agreement":
                "NEUTRAL",

            "ready":
                False,

            "structure":
                structure,

            "pressure":
                pressure,

            "reasons": [
                "No directional signals are strong enough."
            ],
        }

    difference = (
        abs(
            bullish
            - bearish
        )
    )

    # Strong confirmation:
    # At least 5 directional signals
    # and at least 3-signal advantage.

    if (
        bullish >= 5
        and
        bullish - bearish >= 3
    ):

        verdict = "UP"

        label = (
            "UP — STRONG CONFIRMATION"
        )

        confidence = 90 + min(
            5,
            bullish - bearish
        )

        agreement = (
            f"{bullish} BULLISH / "
            f"{bearish} BEARISH"
        )

    elif (
        bearish >= 5
        and
        bearish - bullish >= 3
    ):

        verdict = "DOWN"

        label = (
            "DOWN — STRONG CONFIRMATION"
        )

        confidence = 90 + min(
            5,
            bearish - bullish
        )

        agreement = (
            f"{bullish} BULLISH / "
            f"{bearish} BEARISH"
        )

    # Confirmed but not overwhelming.

    elif (
        bullish >= 4
        and
        bullish > bearish
        and
        difference >= 2
    ):

        verdict = "UP"

        label = (
            "UP — CONFIRMED"
        )

        confidence = 72

        agreement = (
            f"{bullish} BULLISH / "
            f"{bearish} BEARISH"
        )

    elif (
        bearish >= 4
        and
        bearish > bullish
        and
        difference >= 2
    ):

        verdict = "DOWN"

        label = (
            "DOWN — CONFIRMED"
        )

        confidence = 72

        agreement = (
            f"{bullish} BULLISH / "
            f"{bearish} BEARISH"
        )

    else:

        verdict = "WAIT"

        label = (
            "WAIT — CONFLICT"
        )

        confidence = 50

        agreement = (
            f"{bullish} BULLISH / "
            f"{bearish} BEARISH"
        )

    reasons = []

    if verdict == "UP":

        reasons.extend(
            bullish_items
        )

    elif verdict == "DOWN":

        reasons.extend(
            bearish_items
        )

    else:

        reasons.extend(
            bullish_items[:3]
        )

        reasons.extend(
            bearish_items[:3]
        )

        if not reasons:

            reasons.extend(
                neutral_items[:3]
            )

    return {

        "verdict":
            verdict,

        "label":
            label,

        "confidence":
            min(
                confidence,
                95,
            ),

        "score":
            bullish - bearish,

        "bullish":
            bullish,

        "bearish":
            bearish,

        "agreement":
            agreement,

        "ready":
            verdict != "WAIT",

        "structure":
            structure,

        "pressure":
            round(
                pressure,
                2,
            )
            if pressure is not None
            else None,

        "reasons":
            reasons[:6],
    }


# =========================================================
# STATE
# =========================================================

def collect_state():

    price, feeds = (
        spot_feeds()
    )

    candles = (
        get_history()
    )

    market = (
        get_kalshi()
    )

    signal = build_signal(
        price,
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
        market.get(
            "target"
        )
        if market
        else None
    )

    distance = None
    distance_pct = None

    if (
        price is not None
        and target is not None
    ):

        distance = (
            price
            - target
        )

        distance_pct = (
            distance
            / target
        ) * 100

    yes_bid = (
        market.get(
            "yes_bid"
        )
        if market
        else None
    )

    yes_ask = (
        market.get(
            "yes_ask"
        )
        if market
        else None
    )

    yes_mid = (

        (
            yes_bid
            + yes_ask
        ) / 2

        if (
            yes_bid is not None
            and yes_ask is not None
        )

        else (
            market.get(
                "last"
            )
            if market
            else None
        )
    )

    return {

        "ok":
            price is not None,

        "updated":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "btc":
            round(
                price,
                2,
            )
            if price is not None
            else None,

        "feeds": {
            key:
                round(
                    value,
                    2,
                )
                if value is not None
                else None

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
                market.get(
                    "ticker"
                )
                if market
                else None,

            "target":
                round(
                    target,
                    2,
                )
                if target
                else None,

            "yes_bid":
                yes_bid,

            "yes_ask":
                yes_ask,

            "yes_mid":
                yes_mid,

            "last":
                market.get(
                    "last"
                )
                if market
                else None,

            "close_time":
                market.get(
                    "close_time"
                )
                if market
                else None,

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
                round(
                    distance,
                    2,
                )
                if distance is not None
                else None,

            "percent":
                round(
                    distance_pct,
                    4,
                )
                if distance_pct is not None
                else None,
        },

        "momentum": {

            "m1":
                round(
                    m1,
                    4,
                )
                if m1 is not None
                else None,

            "m5":
                round(
                    m5,
                    4,
                )
                if m5 is not None
                else None,

            "m15":
                round(
                    m15,
                    4,
                )
                if m15 is not None
                else None,
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
 font-size:25px;
 margin:0;
}

.sub{
 color:#8994a5;
 font-size:12px;
 margin-top:4px;
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
 border-radius:18px;
 padding:25px;
 text-align:center;
 background:#111822;
 border:1px solid #202a38;
 margin-bottom:14px;
}

.verdict.up{
 border-color:#18d77a;
 background:#092018;
}

.verdict.down{
 border-color:#ff4d5d;
 background:#250c11;
}

.verdict.wait{
 border-color:#e8b84d;
 background:#211b0b;
}

.verdict-label{
 font-size:40px;
 font-weight:900;
}

.conf{
 margin-top:7px;
 color:#aeb8c8;
}

.grid{
 display:grid;
 grid-template-columns:repeat(4,1fr);
 gap:12px;
}

.card{
 background:#10151e;
 border:1px solid #202a38;
 border-radius:14px;
 padding:15px;
}

.card h3{
 font-size:12px;
 color:#8994a5;
 margin:0 0 8px;
 text-transform:uppercase;
}

.big{
 font-size:25px;
 font-weight:800;
}

.value{
 font-size:18px;
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
 font-size:11px;
 color:#7f8998;
 margin-top:5px;
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

.agreement{
 font-size:17px;
 font-weight:800;
 margin-top:3px;
}

.note{
 font-size:10px;
 color:#697585;
 margin-top:6px;
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
  font-size:31px;
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
15-minute BTC confirmation engine
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
 class="agreement"
 id="agreement"
>
--
</div>

</div>


<div class="grid">


<div class="card">

<h3>BTC Reference</h3>

<div
 class="big"
 id="btc"
>
--
</div>

<div
 class="small"
 id="feedCount"
>
-- feeds
</div>

</div>


<div class="card">

<h3>Kalshi Target</h3>

<div
 class="big"
 id="target"
>
--
</div>

<div
 class="small"
 id="ticker"
>
--
</div>

</div>


<div class="card">

<h3>BTC vs Target</h3>

<div
 class="big"
 id="distance"
>
--
</div>

<div
 class="small"
 id="distancePct"
>
--
</div>

</div>


<div class="card">

<h3>Countdown</h3>

<div
 class="big"
 id="countdown"
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
 class="value"
 id="m1"
>
--
</div>

</div>


<div class="card">

<h3>5 Minutes</h3>

<div
 class="value"
 id="m5"
>
--
</div>

</div>


<div class="card">

<h3>15 Minutes</h3>

<div
 class="value"
 id="m15"
>
--
</div>

</div>


<div class="card">

<h3>Structure</h3>

<div
 class="value"
 id="structure"
>
--
</div>

</div>


<div class="card">

<h3>Kalshi YES Bid</h3>

<div
 class="value"
 id="yesBid"
>
--
</div>

</div>


<div class="card">

<h3>Kalshi YES Ask</h3>

<div
 class="value"
 id="yesAsk"
>
--
</div>

</div>


<div class="card">

<h3>YES Mid / Lean</h3>

<div
 class="value"
 id="yesMid"
>
--
</div>

<div class="small">
50% = neutral
</div>

</div>


<div class="card">

<h3>Signal Score</h3>

<div
 class="value"
 id="score"
>
--
</div>

</div>


<div class="card wide">

<h3>
Why the engine chose this
</h3>

<ul
 class="reasons"
 id="reasons"
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
 class="small"
 id="feeds"
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


function cents(v){

 if(v==null)
  return "--";

 return (
  Number(v)*100
 ).toFixed(1)
 +
 "¢";

}


function cls(v){

 if(v==null)
  return "";

 if(v>0)
  return "green";

 if(v<0)
  return "red";

 return "yellow";

}


function setVal(
 id,
 text,
 color
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
  (color || "");

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
  + Date.now(),
  {
   cache:"no-store"
  }
 )

 .then(
  r => r.json()
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
    money(d.btc);


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
    || "--";


   document.getElementById(
    "feedCount"
   ).textContent =
    (
     d.feed_count
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
     d.distance.dollars
    );


   distance.className =
    "big "
    +
    cls(
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


   setVal(
    "m1",
    pct(
     d.momentum.m1
    ),
    cls(
     d.momentum.m1
    )
   );


   setVal(
    "m5",
    pct(
     d.momentum.m5
    ),
    cls(
     d.momentum.m5
    )
   );


   setVal(
    "m15",
    pct(
     d.momentum.m15
    ),
    cls(
     d.momentum.m15
    )
   );


   setVal(
    "structure",
    d.signal.structure
    || "--",
    ""
   );


   document.getElementById(
    "yesBid"
   ).textContent =
    cents(
     d.kalshi.yes_bid
    );


   document.getElementById(
    "yesAsk"
   ).textContent =
    cents(
     d.kalshi.yes_ask
    );


   const mid =
    d.kalshi.yes_mid;


   setVal(
    "yesMid",

    mid == null
     ? "--"
     :
     (
      Number(mid)
      * 100
     ).toFixed(1)
     + "%",

    mid == null
     ? ""
     :
     (
      mid >= 0.50
       ? "green"
       : "red"
     )
   );


   setVal(
    "score",

    d.signal.score == null
     ? "--"
     :
     Number(
      d.signal.score
     ).toFixed(0),

    cls(
     d.signal.score
    )
   );


   const verdict =
    d.signal.verdict
    || "WAIT";


   const box =
    document.getElementById(
     "verdict"
    );


   box.className =
    "verdict "
    +
    (
     verdict === "UP"
      ? "up"
      :
     verdict === "DOWN"
      ? "down"
      :
       "wait"
    );


   document.getElementById(
    "verdictLabel"
   ).textContent =
    d.signal.label
    ||
    (
     verdict === "UP"
      ? "🟢 UP"
      :
     verdict === "DOWN"
      ? "🔴 DOWN"
      :
       "🟡 WAIT"
    );


   document.getElementById(
    "confidence"
   ).textContent =
    (
     d.signal.confidence
     || 0
    )
    +
    "%";


   document.getElementById(
    "agreement"
   ).textContent =
    d.signal.agreement
    || "--";


   const list =
    document.getElementById(
     "reasons"
    );


   list.innerHTML =
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

     list.appendChild(
      li
     );

    }
   );


   document.getElementById(
    "feeds"
   ).textContent =

    Object.entries(
     d.feeds || {}
    )
    .map(
     ([name,value]) =>
      name
      +
      ": "
      +
      (
       value == null
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
    d.history_points
    +
    " candles";

  }
 )

 .catch(
  () => {

   document.getElementById(
    "status"
   ).textContent =
    "CONNECTION ERROR";

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
