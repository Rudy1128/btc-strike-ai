import os
import time
import statistics
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 2.0
KLINES_REFRESH = 12.0

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
)

BINANCE_SPOT = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
COINBASE_SPOT = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
KRAKEN_SPOT = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
BITSTAMP_SPOT = "https://www.bitstamp.net/api/v2/ticker/btcusd/"

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Strike-AI/2.0"
})

cache = {
    "timestamp": 0.0,
    "state": None,
}

history_cache = {
    "timestamp": 0.0,
    "prices": [],
}


def safe_float(value):
    try:
        if value is None or value == "":
            return None

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
# BTC PRICE FEEDS
# =========================================================

def get_binance():

    data = get_json(
        BINANCE_SPOT
    )

    if isinstance(data, dict):
        return safe_float(
            data.get("price")
        )

    return None


def get_coinbase():

    data = get_json(
        COINBASE_SPOT
    )

    try:

        return safe_float(
            data["data"]["amount"]
        )

    except (
        TypeError,
        KeyError,
        IndexError,
    ):

        return None


def get_kraken():

    data = get_json(
        KRAKEN_SPOT
    )

    try:

        result = data["result"]

        pair = next(
            iter(result)
        )

        return safe_float(
            result[pair]["c"][0]
        )

    except (
        TypeError,
        KeyError,
        StopIteration,
        IndexError,
    ):

        return None


def get_bitstamp():

    data = get_json(
        BITSTAMP_SPOT
    )

    if isinstance(data, dict):

        return safe_float(
            data.get("last")
        )

    return None


def get_reference_price():

    feeds = {
        "Binance": get_binance(),
        "Coinbase": get_coinbase(),
        "Kraken": get_kraken(),
        "Bitstamp": get_bitstamp(),
    }

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

    if not filtered:
        filtered = valid

    return (
        statistics.median(filtered),
        feeds,
    )


# =========================================================
# HISTORICAL 1-MINUTE BTC DATA
# =========================================================

def update_historical_prices():

    now = time.time()

    if (
        now - history_cache["timestamp"]
        < KLINES_REFRESH
    ):

        return history_cache["prices"]

    data = get_json(
        BINANCE_KLINES,
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "limit": 20,
        },
    )

    prices = []

    if isinstance(data, list):

        for row in data:

            try:

                timestamp = (
                    float(row[0]) / 1000.0
                )

                close = safe_float(
                    row[4]
                )

                if close:

                    prices.append(
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

    if prices:

        history_cache["prices"] = prices
        history_cache["timestamp"] = now

    return history_cache["prices"]


def momentum_from_history(
    prices,
    seconds,
):

    if not prices:
        return None

    current_time, current = prices[-1]

    target_time = (
        current_time - seconds
    )

    previous = None

    for timestamp, price in prices:

        if timestamp <= target_time:

            previous = price

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
    ) * 100.0


def structure_from_history(
    prices,
):

    if len(prices) < 8:

        return "WAIT"

    values = [
        price
        for _, price in prices[-8:]
    ]

    first = values[:4]
    second = values[4:]

    high1 = max(first)
    low1 = min(first)

    high2 = max(second)
    low2 = min(second)

    if (
        high2 > high1
        and low2 > low1
    ):

        return (
            "HIGHER HIGHS / "
            "HIGHER LOWS"
        )

    if (
        high2 < high1
        and low2 < low1
    ):

        return (
            "LOWER HIGHS / "
            "LOWER LOWS"
        )

    return "MIXED"


# =========================================================
# TIME HELPERS
# =========================================================

def parse_time(value):

    if not value:
        return None

    try:

        text = str(
            value
        ).replace(
            "Z",
            "+00:00",
        )

        dt = datetime.fromisoformat(
            text
        )

        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=timezone.utc
            )

        return dt

    except Exception:

        return None


def seconds_until(value):

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
# KALSHI MARKET
# =========================================================

def normalize_market(
    market,
):

    if not isinstance(
        market,
        dict,
    ):

        return None

    ticker = market.get(
        "ticker"
    )

    if not ticker:
        return None

    def price_value(
        *keys,
    ):

        for key in keys:

            value = safe_float(
                market.get(key)
            )

            if value is not None:

                if value > 1:

                    return value / 100.0

                return value

        return None

    return {

        "ticker": ticker,

        "title": (
            market.get("title")
            or market.get("subtitle")
            or ticker
        ),

        "subtitle":
            market.get("subtitle"),

        "yes_bid":
            price_value(
                "yes_bid_dollars",
                "yes_bid",
            ),

        "yes_ask":
            price_value(
                "yes_ask_dollars",
                "yes_ask",
            ),

        "no_bid":
            price_value(
                "no_bid_dollars",
                "no_bid",
            ),

        "no_ask":
            price_value(
                "no_ask_dollars",
                "no_ask",
            ),

        "last":
            price_value(
                "last_price_dollars",
                "last_price",
            ),

        "floor_strike":
            safe_float(
                market.get(
                    "floor_strike"
                )
            ),

        "cap_strike":
            safe_float(
                market.get(
                    "cap_strike"
                )
            ),

        "functional_strike":
            market.get(
                "functional_strike"
            ),

        "close_time": (
            market.get(
                "close_time"
            )
            or market.get(
                "expiration_time"
            )
        ),

        "open_time":
            market.get(
                "open_time"
            ),

        "status":
            market.get(
                "status"
            ),

        "volume":
            safe_float(
                market.get(
                    "volume_fp",
                    market.get(
                        "volume_24h_fp"
                    ),
                )
            ),

        "raw": market,
    }


def extract_target(
    market,
):

    if not market:
        return None

    # The BTC 15-minute market target
    # is normally available through the
    # strike fields.

    for value in (
        market.get(
            "floor_strike"
        ),
        market.get(
            "cap_strike"
        ),
    ):

        if (
            value is not None
            and value > 1000
        ):

            return value

    raw = market.get(
        "raw",
        {}
    )

    custom = raw.get(
        "custom_strike"
    )

    if isinstance(
        custom,
        dict,
    ):

        for value in custom.values():

            number = safe_float(
                value
            )

            if (
                number
                and number > 1000
            ):

                return number

    functional = market.get(
        "functional_strike"
    )

    if isinstance(
        functional,
        (int, float),
    ):

        if functional > 1000:

            return float(
                functional
            )

    return None


def fetch_market_by_ticker(
    ticker,
):

    data = get_json(
        f"{KALSHI_BASE}/markets/{ticker}"
    )

    if not isinstance(
        data,
        dict,
    ):

        return None

    market = data.get(
        "market",
        data,
    )

    return normalize_market(
        market
    )


def get_kalshi():

    manual = os.getenv(
        "KALSHI_TICKER",
        "",
    ).strip()

    # -----------------------------------------------------
    # Manual ticker if supplied
    # -----------------------------------------------------

    if manual:

        market = fetch_market_by_ticker(
            manual
        )

        if market:

            market["target"] = (
                extract_target(
                    market
                )
            )

            return market

    # -----------------------------------------------------
    # DIRECT KXBTC15M QUERY
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # FALLBACK
    # -----------------------------------------------------

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

    candidates = []

    now = time.time()

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
            and close.timestamp()
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
            extract_target(
                market
            )
        )

    return market


# =========================================================
# KALSHI YES PRICING
# =========================================================

def yes_midpoint(
    market,
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
        and ask is not None
    ):

        return (
            bid + ask
        ) / 2.0

    return market.get(
        "last"
    )


def yes_book_pressure(
    market,
):

    midpoint = yes_midpoint(
        market
    )

    if midpoint is None:
        return None

    # This is YES-price lean.
    # It is NOT true depth-based CVD/order-flow.

    return (
        midpoint - 0.50
    ) * 200.0


# =========================================================
# DECISION ENGINE
# =========================================================

def build_signal(
    price,
    market,
    history,
):

    reasons = []

    score = 0.0

    m1 = momentum_from_history(
        history,
        60,
    )

    m5 = momentum_from_history(
        history,
        300,
    )

    m15 = momentum_from_history(
        history,
        900,
    )

    structure = (
        structure_from_history(
            history
        )
    )

    target = (
        market.get("target")
        if market
        else None
    )

    yes_mid = yes_midpoint(
        market
    )

    seconds_left = (
        seconds_until(
            market.get(
                "close_time"
            )
        )
        if market
        else None
    )

    # -----------------------------------------------------
    # HARD DATA GATES
    # -----------------------------------------------------

    if price is None:

        return {
            "verdict":
                "WAIT",

            "confidence":
                0,

            "score":
                0,

            "reasons": [
                "BTC reference price is unavailable."
            ],

            "structure":
                structure,

            "pressure":
                None,

            "ready":
                False,
        }

    if market is None:

        return {
            "verdict":
                "WAIT",

            "confidence":
                0,

            "score":
                0,

            "reasons": [
                "Live KXBTC15M market is unavailable."
            ],

            "structure":
                structure,

            "pressure":
                None,

            "ready":
                False,
        }

    if target is None:

        return {
            "verdict":
                "WAIT",

            "confidence":
                0,

            "score":
                0,

            "reasons": [
                "Kalshi target is unavailable."
            ],

            "structure":
                structure,

            "pressure":
                None,

            "ready":
                False,
        }

    # -----------------------------------------------------
    # BTC VS TARGET
    # -----------------------------------------------------

    distance_pct = (
        (price - target)
        / target
    ) * 100.0

    if distance_pct > 0.03:

        score += 2.0

        reasons.append(
            "BTC is meaningfully above the Kalshi target."
        )

    elif distance_pct < -0.03:

        score -= 2.0

        reasons.append(
            "BTC is meaningfully below the Kalshi target."
        )

    else:

        reasons.append(
            "BTC is very close to the Kalshi target."
        )

    # -----------------------------------------------------
    # 1-MINUTE
    # -----------------------------------------------------

    if m1 is not None:

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

    # -----------------------------------------------------
    # 5-MINUTE
    # -----------------------------------------------------

    if m5 is not None:

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

    # -----------------------------------------------------
    # 15-MINUTE
    # -----------------------------------------------------

    if m15 is not None:

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

    # -----------------------------------------------------
    # PRICE STRUCTURE
    # -----------------------------------------------------

    if (
        structure
        == "HIGHER HIGHS / HIGHER LOWS"
    ):

        score += 1.25

        reasons.append(
            "Price structure is bullish."
        )

    elif (
        structure
        == "LOWER HIGHS / LOWER LOWS"
    ):

        score -= 1.25

        reasons.append(
            "Price structure is bearish."
        )

    # -----------------------------------------------------
    # KALSHI YES MARKET
    # -----------------------------------------------------

    if yes_mid is not None:

        if yes_mid >= 0.60:

            score += 1.25

            reasons.append(
                "Kalshi YES price favors the upside."
            )

        elif yes_mid <= 0.40:

            score -= 1.25

            reasons.append(
                "Kalshi YES price favors the downside."
            )

    pressure = yes_book_pressure(
        market
    )

    # -----------------------------------------------------
    # HISTORY GATE
    # -----------------------------------------------------

    available_confirmations = sum(
        value is not None
        for value in (
            m1,
            m5,
            m15,
        )
    )

    if available_confirmations < 2:

        return {
            "verdict":
                "WAIT",

            "confidence":
                25,

            "score":
                round(
                    score,
                    2,
                ),

            "reasons": [
                "Building enough 1m/5m/15m history before making a call."
            ],

            "structure":
                structure,

            "pressure":
                round(
                    pressure,
                    2,
                )
                if pressure is not None
                else None,

            "ready":
                False,
        }

    # -----------------------------------------------------
    # FINAL 60 SECOND WARNING
    # -----------------------------------------------------

    if (
        seconds_left is not None
        and seconds_left <= 60
    ):

        reasons.append(
            "Final 60-second settlement window: official RTI average is not known yet."
        )

    # -----------------------------------------------------
    # VERDICT
    # -----------------------------------------------------

    if score >= 4.0:

        verdict = "UP"

    elif score <= -4.0:

        verdict = "DOWN"

    else:

        verdict = "WAIT"

    confidence = min(
        92,
        max(
            35,
            int(
                50
                + abs(score) * 7
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

        "reasons":
            reasons[:6],

        "structure":
            structure,

        "pressure":
            round(
                pressure,
                2,
            )
            if pressure is not None
            else None,

        "ready":
            verdict != "WAIT",
    }


# =========================================================
# STATE
# =========================================================

def collect_state():

    price, feeds = (
        get_reference_price()
    )

    history = (
        update_historical_prices()
    )

    if price is not None:

        now = time.time()

        if (
            not history
            or now - history[-1][0]
            >= 2
        ):

            history = list(
                history
            )

            history.append(
                (
                    now,
                    price,
                )
            )

            history = history[-30:]

    market = get_kalshi()

    m1 = momentum_from_history(
        history,
        60,
    )

    m5 = momentum_from_history(
        history,
        300,
    )

    m15 = momentum_from_history(
        history,
        900,
    )

    signal = build_signal(
        price,
        market,
        history,
    )

    target = (
        market.get("target")
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
            price - target
        )

        distance_pct = (
            distance
            / target
        ) * 100.0

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
            name:
                round(
                    value,
                    2,
                )
                if value is not None
                else None

            for name, value
            in feeds.items()
        },

        "feed_count":
            sum(
                1
                for value
                in feeds.values()
                if value is not None
            ),

        "kalshi": {

            "ticker":
                market.get(
                    "ticker"
                )
                if market
                else None,

            "title":
                market.get(
                    "title"
                )
                if market
                else None,

            "target":
                round(
                    target,
                    2,
                )
                if target is not None
                else None,

            "yes_bid":
                market.get(
                    "yes_bid"
                )
                if market
                else None,

            "yes_ask":
                market.get(
                    "yes_ask"
                )
                if market
                else None,

            "no_bid":
                market.get(
                    "no_bid"
                )
                if market
                else None,

            "no_ask":
                market.get(
                    "no_ask"
                )
                if market
                else None,

            "last":
                market.get(
                    "last"
                )
                if market
                else None,

            "yes_mid":
                yes_midpoint(
                    market
                ),

            "close_time":
                market.get(
                    "close_time"
                )
                if market
                else None,

            "countdown":
                seconds_until(
                    market.get(
                        "close_time"
                    )
                )
                if market
                else None,

            "status":
                market.get(
                    "status"
                )
                if market
                else None,

            "volume":
                market.get(
                    "volume"
                )
                if market
                else None,
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

        "history_points":
            len(history),
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
        now - cache["timestamp"]
        < CACHE_SECONDS
    ):

        return jsonify(
            cache["state"]
        )

    state = collect_state()

    cache["state"] = state
    cache["timestamp"] = now

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
 gap:12px;
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

 .grid{
  grid-template-columns:1fr 1fr;
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
BTC Reference is an exchange composite. It is not the official CF Benchmarks BRTI.
</div>

</div>


</div>

</div>


<script>

function money(v){

 if(v==null)
  return "--";

 return "$" +
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

 const e =
  document.getElementById(
   id
  );

 e.textContent =
  text;

 e.className =
  "value " +
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
     d.feed_count || 0
    )
    + " feeds live";


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
      mid >= 0.5
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
     ).toFixed(2),

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
     d.signal.confidence
     || 0
    )
    + "%";


   const ul =
    document.getElementById(
     "reasons"
    );


   ul.innerHTML =
    "";


   (
    d.signal.reasons
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

     ul.appendChild(
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
    );

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


# =========================================================
# LOCAL DEVELOPMENT
# =========================================================

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
        threaded=True,
    )
