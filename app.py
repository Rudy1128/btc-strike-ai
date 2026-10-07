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
    "User-Agent": "BTC-Strike-AI/3.0"
})

cache = {
    "time": 0,
    "state": None,
}

history_cache = {
    "time": 0,
    "candles": [],
}


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


# =========================================================
# BTC SPOT FEEDS
# =========================================================

def spot_feeds():

    feeds = {}

    data = get_json(
        "https://api.binance.com/api/v3/ticker/price",
        {"symbol": "BTCUSDT"},
    )

    if isinstance(data, dict):
        feeds["Binance"] = f(
            data.get("price")
        )

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

    data = get_json(
        "https://api.kraken.com/0/public/Ticker",
        {"pair": "XBTUSD"},
    )

    try:
        result = data["result"]
        pair = next(iter(result))

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

    data = get_json(
        "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    )

    feeds["Bitstamp"] = (
        f(data.get("last"))
        if isinstance(data, dict)
        else None
    )

    valid = [
        value
        for value in feeds.values()
        if value and value > 0
    ]

    if not valid:
        return None, feeds

    median = statistics.median(valid)

    filtered = [
        value
        for value in valid
        if abs(value - median) / median <= 0.0035
    ]

    return (
        statistics.median(
            filtered or valid
        ),
        feeds,
    )


# =========================================================
# HISTORICAL 1-MINUTE CANDLES
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
                timestamp = float(row[0])
                close = f(row[4])

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

            timestamp = float(row[0])
            close = f(row[4])

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

                close = f(row[4])

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

    # Coinbase first because Binance
    # may be unavailable from some
    # hosting regions.

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

    current_timestamp, current = (
        candles[-1]
    )

    target_timestamp = (
        current_timestamp
        - minutes * 60
    )

    previous = None

    for timestamp, close in candles:

        if timestamp <= target_timestamp:

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


def structure(candles):

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
# TIME
# =========================================================

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
# KALSHI
# =========================================================

def price_probability(
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
            price_probability(
                market,
                "yes_bid_dollars",
                "yes_bid",
            ),

        "yes_ask":
            price_probability(
                market,
                "yes_ask_dollars",
                "yes_ask",
            ),

        "last":
            price_probability(
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
            value
            and value > 1000
        ):
            return value

    raw = market.get(
        "raw",
        {},
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
                number
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

    # Fallback search.
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
            close.timestamp() > now
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
# DECISION ENGINE
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

    struct = structure(
        candles
    )

    if price is None:

        return {
            "verdict":
                "WAIT",

            "confidence":
                0,

            "score":
                0,

            "ready":
                False,

            "structure":
                struct,

            "pressure":
                None,

            "reasons": [
                "BTC price unavailable."
            ],
        }

    if market is None:

        return {
            "verdict":
                "WAIT",

            "confidence":
                0,

            "score":
                0,

            "ready":
                False,

            "structure":
                struct,

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

            "confidence":
                0,

            "score":
                0,

            "ready":
                False,

            "structure":
                struct,

            "pressure":
                None,

            "reasons": [
                "Kalshi target unavailable."
            ],
        }

    # All three timeframes are required
    # before a directional call.

    if (
        m1 is None
        or
        m5 is None
        or
        m15 is None
    ):

        return {
            "verdict":
                "WAIT",

            "confidence":
                25,

            "score":
                0,

            "ready":
                False,

            "structure":
                struct,

            "pressure":
                None,

            "reasons": [
                "Building 1m/5m/15m candle history."
            ],
        }

    score = 0.0
    reasons = []

    distance_pct = (
        (price - target)
        / target
    ) * 100

    if distance_pct > 0.03:

        score += 2

        reasons.append(
            "BTC is above the Kalshi target."
        )

    elif distance_pct < -0.03:

        score -= 2

        reasons.append(
            "BTC is below the Kalshi target."
        )

    else:

        reasons.append(
            "BTC is very close to the Kalshi target."
        )

    # 1-minute

    if m1 > 0.01:

        score += 0.75

        reasons.append(
            "1m momentum is positive."
        )

    elif m1 < -0.01:

        score -= 0.75

        reasons.append(
            "1m momentum is negative."
        )

    # 5-minute

    if m5 > 0.02:

        score += 1.25

        reasons.append(
            "5m momentum is positive."
        )

    elif m5 < -0.02:

        score -= 1.25

        reasons.append(
            "5m momentum is negative."
        )

    # 15-minute

    if m15 > 0.04:

        score += 1.5

        reasons.append(
            "15m momentum is positive."
        )

    elif m15 < -0.04:

        score -= 1.5

        reasons.append(
            "15m momentum is negative."
        )

    # Structure

    if (
        struct
        == "HIGHER HIGHS / "
           "HIGHER LOWS"
    ):

        score += 1.25

        reasons.append(
            "Price structure is bullish."
        )

    elif (
        struct
        == "LOWER HIGHS / "
           "LOWER LOWS"
    ):

        score -= 1.25

        reasons.append(
            "Price structure is bearish."
        )

    # Kalshi YES

    yes_bid = market.get(
        "yes_bid"
    )

    yes_ask = market.get(
        "yes_ask"
    )

    if (
        yes_bid is not None
        and
        yes_ask is not None
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

            score += 1.25

            reasons.append(
                "Kalshi YES pricing favors the upside."
            )

        elif yes_mid <= 0.40:

            score -= 1.25

            reasons.append(
                "Kalshi YES pricing favors the downside."
            )

    # Final verdict.

    if score >= 4:

        verdict = "UP"

    elif score <= -4:

        verdict = "DOWN"

    else:

        verdict = "WAIT"

    confidence = min(
        92,
        max(
            35,
            int(
                50
                + abs(score)
                * 7
            ),
        ),
    )

    if verdict == "WAIT":

        confidence = min(
            confidence,
            60,
        )

    return {

        "verdict":
            verdict,

        "confidence":
            confidence,

        "score":
            round(
                score,
                2,
            ),

        "ready":
            verdict != "WAIT",

        "structure":
            struct,

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

    sig = build_signal(
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
        and
        target is not None
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
            and
            yes_ask is not None
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

        "history_source":
            (
                "loaded"
                if len(candles) >= 16
                else "unavailable"
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
            sig,
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
 font-size:46px;
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
  font-size:38px;
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
15-minute BTC decision engine
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
Historical candles use exchange data; BTC Reference is not official BRTI.
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
 + Number(v).toFixed(3)
 + "%";

}


function cents(v){

 if(v==null)
  return "--";

 return (
  Number(v)*100
 ).toFixed(1)
 + "¢";

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

 const element =
  document.getElementById(
   id
  );

 element.textContent =
  text;

 element.className =
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
  response =>
   response.json()
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
    money(
     data.btc
    );


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
    + " feeds live";


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
    cls(
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


   setVal(
    "m1",
    pct(
     data.momentum.m1
    ),
    cls(
     data.momentum.m1
    )
   );


   setVal(
    "m5",
    pct(
     data.momentum.m5
    ),
    cls(
     data.momentum.m5
    )
   );


   setVal(
    "m15",
    pct(
     data.momentum.m15
    ),
    cls(
     data.momentum.m15
    )
   );


   setVal(
    "structure",
    data.signal.structure
     || "--",
    ""
   );


   document.getElementById(
    "yesBid"
   ).textContent =
    cents(
     data.kalshi.yes_bid
    );


   document.getElementById(
    "yesAsk"
   ).textContent =
    cents(
     data.kalshi.yes_ask
    );


   const mid =
    data.kalshi.yes_mid;


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

    data.signal.score == null
     ? "--"
     :
     Number(
      data.signal.score
     ).toFixed(2),

    cls(
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

    verdict === "UP"
     ? "🟢 UP"
     :
    verdict === "DOWN"
     ? "🔴 DOWN"
     :
      "🟡 WAIT";


   document.getElementById(
    "confidence"
   ).textContent =
    (
     data.signal.confidence
     || 0
    )
    + "%";


   const list =
    document.getElementById(
     "reasons"
    );


   list.innerHTML =
    "";


   (
    data.signal.reasons
    ||
    [
     "Waiting for stronger alignment..."
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
     data.feeds || {}
    )
    .map(
     ([name,value]) =>
      name
      + ": "
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
    data.history_source
    +
    " ("
    +
    data.history_points
    +
    " candles)";

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
