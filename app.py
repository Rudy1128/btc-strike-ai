import os
import json
import time
import threading
import statistics
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

try:
    import websocket
except Exception:
    websocket = None

app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 2
KALSHI_SERIES = "KXBTC15M"
KALSHI_TICKER = os.getenv("KALSHI_TICKER", "").strip()

KALSHI_BASES = [
    os.getenv(
        "KALSHI_BASE_URL",
        "https://external-api.kalshi.com/trade-api/v2"
    ).rstrip("/"),
    "https://api.elections.kalshi.com/trade-api/v2",
]

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Strike-AI/12.0"
})

cache = {
    "time": 0.0,
    "state": None
}

history_cache = {
    "time": 0.0,
    "candles": []
}

live = {
    "price": None,
    "received": 0.0,
    "connected": False,
    "source": "REST"
}

lock = threading.Lock()
streams_started = False


# =========================================================
# BASIC HELPERS
# =========================================================

def num(value):
    try:
        return float(value)
    except Exception:
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
    except Exception:
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

    except Exception:
        return None


def set_live(price, source):
    if price is None or price <= 0:
        return

    with lock:
        live.update(
            price=float(price),
            received=time.time(),
            connected=True,
            source=source
        )


# =========================================================
# LIVE COINBASE BTC STREAM
# =========================================================

def coinbase_loop():

    if websocket is None:
        return

    url = "wss://advanced-trade-ws.coinbase.com"

    while True:

        try:

            def on_open(ws):

                ws.send(
                    json.dumps({
                        "type": "subscribe",
                        "product_ids": ["BTC-USD"],
                        "channel": "ticker"
                    })
                )

                ws.send(
                    json.dumps({
                        "type": "subscribe",
                        "product_ids": ["BTC-USD"],
                        "channel": "heartbeats"
                    })
                )


            def on_message(ws, raw):

                try:

                    data = json.loads(raw)

                    price = None

                    def walk(obj):

                        nonlocal price

                        if price is not None:
                            return

                        if isinstance(obj, dict):

                            for key in (
                                "price",
                                "price_usd",
                                "last_trade_price"
                            ):

                                value = num(
                                    obj.get(key)
                                )

                                if (
                                    value is not None
                                    and
                                    value > 0
                                ):

                                    price = value
                                    return

                            for value in obj.values():
                                walk(value)

                        elif isinstance(obj, list):

                            for value in obj:
                                walk(value)

                    walk(data)

                    if price:
                        set_live(
                            price,
                            "Coinbase Live"
                        )

                except Exception:
                    pass


            def on_close(ws, code, message):

                with lock:
                    live["connected"] = False


            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                skip_utf8_validation=True
            )

        except Exception:

            with lock:
                live["connected"] = False

        time.sleep(3)


# =========================================================
# LIVE BINANCE FALLBACK STREAM
# =========================================================

def binance_loop():

    if websocket is None:
        return

    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"

    while True:

        try:

            def on_message(ws, raw):

                try:

                    data = json.loads(raw)

                    price = num(
                        data.get("p")
                    )

                    if (
                        price
                        and
                        not (
                            live["source"] == "Coinbase Live"
                            and
                            time.time() - live["received"] < 3
                        )
                    ):

                        set_live(
                            price,
                            "Binance Live"
                        )

                except Exception:
                    pass


            ws = websocket.WebSocketApp(
                url,
                on_message=on_message
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                skip_utf8_validation=True
            )

        except Exception:
            pass

        time.sleep(4)


# =========================================================
# START LIVE STREAMS
# =========================================================

def start_streams():

    global streams_started

    if websocket is None:
        return

    if streams_started:
        return

    streams_started = True

    threading.Thread(
        target=coinbase_loop,
        daemon=True,
        name="coinbase-btc"
    ).start()

    threading.Thread(
        target=binance_loop,
        daemon=True,
        name="binance-btc"
    ).start()


# =========================================================
# BTC SPOT PRICE
# =========================================================

def spot_price():

    with lock:

        if (
            live["price"]
            and
            time.time() - live["received"] < 4
        ):

            return (
                live["price"],
                live["source"]
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
            "Coinbase",
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            None
        ),

        (
            "Kraken",
            "https://api.kraken.com/0/public/Ticker",
            {
                "pair": "XBTUSD"
            }
        )

    ]

    values = []

    for name, url, params in sources:

        data = get_json(
            url,
            params
        )

        price = None

        if name.startswith("Binance"):

            if isinstance(data, dict):
                price = num(
                    data.get("price")
                )

        elif name == "Coinbase":

            try:
                price = num(
                    data["data"]["amount"]
                )
            except Exception:
                pass

        else:

            try:

                key = next(
                    key
                    for key in data["result"]
                    if key != "last"
                )

                price = num(
                    data["result"][key]["c"][0]
                )

            except Exception:
                pass

        if price and price > 0:
            values.append(price)

    if values:

        return (
            statistics.median(values),
            "Composite REST"
        )

    return (
        None,
        "Unavailable"
    )


# =========================================================
# ONE-MINUTE BTC HISTORY
# =========================================================

def candles():

    now = time.time()

    if (
        now - history_cache["time"] < 15
        and
        history_cache["candles"]
    ):

        return history_cache["candles"]

    data = get_json(
        "https://api.binance.com/api/v3/klines",
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "limit": 21
        }
    )

    output = []

    if isinstance(data, list):

        for row in data:

            try:

                output.append(
                    (
                        float(row[0]) / 1000.0,
                        float(row[4])
                    )
                )

            except Exception:
                pass

    if len(output) < 16:

        data = get_json(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            {
                "granularity": 60
            }
        )

        if isinstance(data, list):

            output = []

            for row in data:

                try:

                    output.append(
                        (
                            float(row[0]),
                            float(row[4])
                        )
                    )

                except Exception:
                    pass

            output.sort()
            output = output[-21:]

    history_cache.update(
        time=now,
        candles=output
    )

    return output


# =========================================================
# MOMENTUM
# =========================================================

def momentum(candles_data, minutes):

    if len(candles_data) < 2:
        return None

    current = candles_data[-1][1]

    target_time = (
        candles_data[-1][0]
        -
        minutes * 60
    )

    previous = next(
        (
            price
            for timestamp, price
            in reversed(candles_data[:-1])
            if timestamp <= target_time
        ),
        None
    )

    if previous is None:
        return None

    return (
        (current - previous)
        /
        previous
        *
        100
    )


# =========================================================
# PRICE STRUCTURE
# =========================================================

def structure(candles_data):

    if len(candles_data) < 8:
        return "WAIT"

    first = [
        price
        for _, price
        in candles_data[-8:-4]
    ]

    second = [
        price
        for _, price
        in candles_data[-4:]
    ]

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
# KALSHI NORMALIZATION
# =========================================================

def normalize_market(market):

    if (
        not isinstance(market, dict)
        or
        not market.get("ticker")
    ):

        return None


    def probability(*keys):

        for key in keys:

            value = num(
                market.get(key)
            )

            if value is not None:

                if value > 1:
                    return value / 100

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

        value = num(
            market.get(key)
        )

        if value is not None:

            target = value
            break


    return {
        "ticker": market.get("ticker"),

        "target": target,

        "yes_bid": probability(
            "yes_bid_dollars",
            "yes_bid"
        ),

        "yes_ask": probability(
            "yes_ask_dollars",
            "yes_ask"
        ),

        "last": probability(
            "last_price_dollars",
            "last_price"
        ),

        "close_time": (
            market.get("close_time")
            or
            market.get("expiration_time")
        )
    }


# =========================================================
# KALSHI MARKET
# =========================================================

def get_kalshi():

    if KALSHI_TICKER:

        for base in KALSHI_BASES:

            data = get_json(
                f"{base}/markets/{KALSHI_TICKER}"
            )

            market = normalize_market(
                data.get(
                    "market",
                    data
                )
                if isinstance(data, dict)
                else None
            )

            if market:
                return market


    for base in KALSHI_BASES:

        data = get_json(
            f"{base}/markets",
            {
                "series_ticker": KALSHI_SERIES,
                "status": "open",
                "limit": 100
            }
        )

        markets = (
            data.get("markets", [])
            if isinstance(data, dict)
            else []
        )

        valid = []

        for market in markets:

            close = parse_time(
                market.get("close_time")
                or
                market.get("expiration_time")
            )

            ticker = str(
                market.get(
                    "ticker",
                    ""
                )
            ).upper()

            if (
                ticker.startswith(
                    KALSHI_SERIES
                )
                and
                close
                and
                close.timestamp() > time.time()
            ):

                valid.append(
                    (
                        close.timestamp(),
                        market
                    )
                )

        if valid:

            valid.sort(
                key=lambda x: x[0]
            )

            return normalize_market(
                valid[0][1]
            )

    return None


# =========================================================
# BUYER / SELLER PRESSURE
# =========================================================

def get_pressure():

    data = get_json(
        "https://api.binance.com/api/v3/aggTrades",
        {
            "symbol": "BTCUSDT",
            "limit": 1000
        }
    )

    if not isinstance(data, list):

        return {
            "winner": "UNAVAILABLE",
            "buy_pct": None,
            "sell_pct": None,
            "delta": None,
            "trades": 0
        }

    buyers = 0.0
    sellers = 0.0

    for trade in data:

        quantity = num(
            trade.get("q")
        )

        price = num(
            trade.get("p")
        )

        if not quantity or not price:
            continue

        value = (
            quantity
            *
            price
        )

        if trade.get("m"):
            sellers += value
        else:
            buyers += value

    total = (
        buyers
        +
        sellers
    )

    buy_pct = (
        buyers
        /
        total
        *
        100
        if total
        else None
    )

    sell_pct = (
        sellers
        /
        total
        *
        100
        if total
        else None
    )

    if buyers > sellers:
        winner = "BUYERS"
    elif sellers > buyers:
        winner = "SELLERS"
    else:
        winner = "BALANCED"

    return {
        "winner": winner,
        "buy_pct": buy_pct,
        "sell_pct": sell_pct,
        "delta": buyers - sellers,
        "trades": len(data)
    }


# =========================================================
# ORDER BOOK
# =========================================================

def get_orderbook():

    data = get_json(
        "https://api.binance.com/api/v3/depth",
        {
            "symbol": "BTCUSDT",
            "limit": 100
        }
    )

    if not isinstance(data, dict):

        return {
            "winner": "UNAVAILABLE",
            "bid_pct": None,
            "ask_pct": None
        }

    bids = sum(
        (
            num(row[1]) or 0
        )
        for row in data.get(
            "bids",
            []
        )
    )

    asks = sum(
        (
            num(row[1]) or 0
        )
        for row in data.get(
            "asks",
            []
        )
    )

    total = (
        bids
        +
        asks
    )

    if bids > asks:
        winner = "BIDS"
    elif asks > bids:
        winner = "ASKS"
    else:
        winner = "BALANCED"

    return {
        "winner": winner,

        "bid_pct": (
            bids
            /
            total
            *
            100
            if total
            else None
        ),

        "ask_pct": (
            asks
            /
            total
            *
            100
            if total
            else None
        )
    }


# =========================================================
# SIGNAL ENGINE
# =========================================================

def build_signal(
    price,
    market,
    candles_data,
    pressure
):

    m1 = momentum(
        candles_data,
        1
    )

    m5 = momentum(
        candles_data,
        5
    )

    m15 = momentum(
        candles_data,
        15
    )

    price_structure = structure(
        candles_data
    )

    if (
        price is None
        or
        not market
        or
        market.get("target") is None
        or
        len(candles_data) < 16
    ):

        return {
            "verdict": "WAIT",
            "label": "🟡 WAIT",
            "confidence": 0,
            "score": 0,
            "bullish": 0,
            "bearish": 0,
            "m1": m1,
            "m5": m5,
            "m15": m15,
            "structure": price_structure,
            "reversal": "LOW",
            "reasons": [
                "Waiting for live BTC history and Kalshi target."
            ]
        }


    bullish = 0
    bearish = 0

    reasons = []

    target = market["target"]


    # -----------------------------------------------------
    # PRICE VS KALSHI TARGET
    # -----------------------------------------------------

    if price > target:

        bullish += 2

        reasons.append(
            "BTC is above the Kalshi target."
        )

    else:

        bearish += 2

        reasons.append(
            "BTC is below the Kalshi target."
        )


    # -----------------------------------------------------
    # MULTI-TIMEFRAME MOMENTUM
    # -----------------------------------------------------

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


    # -----------------------------------------------------
    # PRICE STRUCTURE
    # -----------------------------------------------------

    if price_structure.startswith(
        "HIGHER"
    ):

        bullish += 2

        reasons.append(
            "Price structure is bullish."
        )

    elif price_structure.startswith(
        "LOWER"
    ):

        bearish += 2

        reasons.append(
            "Price structure is bearish."
        )


    # -----------------------------------------------------
    # KALSHI YES
    # -----------------------------------------------------

    yes = None

    if (
        market.get("yes_bid") is not None
        and
        market.get("yes_ask") is not None
    ):

        yes = (
            market["yes_bid"]
            +
            market["yes_ask"]
        ) / 2

    elif market.get("last") is not None:

        yes = market["last"]


    if yes is not None:

        if yes >= 0.60:

            bullish += 1

            reasons.append(
                "Kalshi YES is favoring UP."
            )

        elif yes <= 0.40:

            bearish += 1

            reasons.append(
                "Kalshi YES is favoring DOWN."
            )


    # -----------------------------------------------------
    # TRADE PRESSURE
    # -----------------------------------------------------

    if pressure.get(
        "winner"
    ) == "BUYERS":

        bullish += 1

        reasons.append(
            "Recent Binance trade flow favors buyers."
        )

    elif pressure.get(
        "winner"
    ) == "SELLERS":

        bearish += 1

        reasons.append(
            "Recent Binance trade flow favors sellers."
        )


    # -----------------------------------------------------
    # REVERSAL DETECTION
    # -----------------------------------------------------

    reversal = "LOW"

    if (
        (m15 or 0) < 0
        and
        (m5 or 0) < 0
        and
        (m1 or 0) > 0
    ):

        reversal = "HIGH"

    elif (
        (m15 or 0) > 0
        and
        (m5 or 0) > 0
        and
        (m1 or 0) < 0
    ):

        reversal = "HIGH"


    if reversal == "HIGH":

        reasons.append(
            "Short-term momentum is conflicting with the larger trend."
        )


    # -----------------------------------------------------
    # FINAL DECISION
    # -----------------------------------------------------

    total = (
        bullish
        +
        bearish
    )

    difference = abs(
        bullish
        -
        bearish
    )

    verdict = "WAIT"
    label = "🟡 WAIT"

    if (
        reversal != "HIGH"
        and
        total >= 6
        and
        difference >= 3
    ):

        if bullish > bearish:

            verdict = "UP"

            label = (
                "🟢 UP — STRONG SETUP"
            )

        else:

            verdict = "DOWN"

            label = (
                "🔴 DOWN — STRONG SETUP"
            )


    if verdict != "WAIT":

        confidence = min(
            96,
            50 + difference * 7
        )

    else:

        confidence = min(
            49,
            35 + difference * 3
        )


    return {
        "verdict": verdict,
        "label": label,
        "confidence": confidence,
        "score": total,
        "bullish": bullish,
        "bearish": bearish,
        "m1": m1,
        "m5": m5,
        "m15": m15,
        "structure": price_structure,
        "reversal": reversal,
        "reasons": reasons
    }


# =========================================================
# DATA COLLECTION
# =========================================================

def collect_state():

    price, source = spot_price()

    market = get_kalshi()

    candle_data = candles()

    pressure = get_pressure()

    book = get_orderbook()

    signal = build_signal(
        price,
        market,
        candle_data,
        pressure
    )


    # -----------------------------------------------------
    # DATA QUALITY
    # -----------------------------------------------------

    quality_score = 0

    if price is not None:
        quality_score += 35

    if (
        market
        and
        market.get("target") is not None
    ):

        quality_score += 35

    if len(candle_data) >= 16:
        quality_score += 30


    if quality_score >= 85:
        quality_grade = "HIGH"

    elif quality_score >= 70:
        quality_grade = "GOOD"

    else:
        quality_grade = "FAIR"


    # -----------------------------------------------------
    # COUNTDOWN
    # -----------------------------------------------------

    countdown = None

    if (
        market
        and
        market.get("close_time")
    ):

        close = parse_time(
            market["close_time"]
        )

        if close:

            countdown = max(
                0,
                int(
                    close.timestamp()
                    -
                    time.time()
                )
            )


    # -----------------------------------------------------
    # LIVE LATENCY
    # -----------------------------------------------------

    with lock:

        if live["received"]:

            age = (
                time.time()
                -
                live["received"]
            ) * 1000

        else:

            age = None

        stream_connected = bool(
            live["connected"]
        )


    return {
        "btc": price,

        "source": source,

        "market": market,

        "countdown": countdown,

        "candles": len(
            candle_data
        ),

        "signal": signal,

        "buy_sell": pressure,

        "order_book": book,

        "quality": {
            "score": quality_score,
            "grade": quality_grade
        },

        "latency": {
            "btc_age_ms": age,
            "btc_stream": stream_connected
        }
    }


# =========================================================
# DASHBOARD
# =========================================================

PAGE = r'''
<!doctype html>

<html>

<head>

<meta name="viewport"
      content="width=device-width,initial-scale=1">

<title>
BTC Strike AI
</title>

<style>

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background:
        #070b12;

    color:
        #e9eef7;

    font-family:
        Arial,
        sans-serif;
}

main {

    max-width:
        1100px;

    margin:
        auto;

    padding:
        18px;
}

.top {

    display:
        flex;

    justify-content:
        space-between;

    gap:
        12px;

    align-items:
        center;
}

.title {

    font-size:
        28px;

    font-weight:
        800;
}

.sub {

    color:
        #8d9aae;
}

.verdict {

    margin-top:
        16px;

    border-radius:
        16px;

    padding:
        20px;

    text-align:
        center;

    border:
        1px solid #263142;
}

.verdict.up {

    background:
        #082e1c;

    border-color:
        #18b66a;
}

.verdict.down {

    background:
        #3a1015;

    border-color:
        #ef4d5b;
}

.verdict.wait {

    background:
        #3a2d08;

    border-color:
        #e5b63f;
}

.verdictLabel {

    font-size:
        30px;

    font-weight:
        900;
}

.grid {

    display:
        grid;

    grid-template-columns:
        repeat(4, 1fr);

    gap:
        12px;

    margin-top:
        14px;
}

.card {

    background:
        #101722;

    border:
        1px solid #263142;

    border-radius:
        14px;

    padding:
        16px;

    min-height:
        105px;
}

.wide {

    grid-column:
        span 2;
}

.label {

    font-size:
        12px;

    color:
        #8996a8;

    font-weight:
        700;

    letter-spacing:
        .08em;
}

.big {

    font-size:
        28px;

    font-weight:
        800;

    margin-top:
        9px;
}

.value {

    font-size:
        18px;

    font-weight:
        700;

    margin-top:
        10px;
}

.small {

    font-size:
        13px;

    color:
        #9aa7b9;

    margin-top:
        7px;
}

.bar {

    height:
        16px;

    background:
        #252e3b;

    border-radius:
        10px;

    overflow:
        hidden;

    margin-top:
        10px;
}

.bar > div {

    height:
        100%;

    background:
        #18b66a;
}

.row {

    display:
        flex;

    justify-content:
        space-between;

    gap:
        10px;
}

.reason {

    white-space:
        pre-line;
}

.footer {

    margin-top:
        18px;

    text-align:
        center;

    color:
        #667286;

    font-size:
        12px;
}

@media(max-width:800px) {

    .grid {

        grid-template-columns:
            repeat(2, 1fr);
    }

    .wide {

        grid-column:
            span 2;
    }
}

@media(max-width:500px) {

    .grid {

        grid-template-columns:
            1fr;
    }

    .wide {

        grid-column:
            span 1;
    }
}

</style>

</head>

<body>

<main>

<div class="top">

<div>

<div class="title">
⚡ BTC STRIKE AI
</div>

<div class="sub">
KXBTC15M • Live BTC Feed • Signal Engine
</div>

</div>

<div id="status"
     class="sub">
Connecting...
</div>

</div>


<div id="verdict"
     class="verdict wait">

<div id="label"
     class="verdictLabel">
🟡 WAIT
</div>

<div id="confidence"
     class="small">
0% confidence
</div>

<div id="agreement"
     class="small">
--
</div>

</div>


<div class="grid">


<div class="card">

<div class="label">
LIVE BTC
</div>

<div id="btc"
     class="big">
--
</div>

<div id="btcAge"
     class="small">
--
</div>

</div>


<div class="card">

<div class="label">
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

<div class="label">
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

<div class="label">
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

<div class="label">
1 MIN
</div>

<div id="m1"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
5 MIN
</div>

<div id="m5"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
15 MIN
</div>

<div id="m15"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
STRUCTURE
</div>

<div id="structure"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
MOMENTUM
</div>

<div id="accel"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
REVERSAL RISK
</div>

<div id="reversal"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
KALSHI YES
</div>

<div id="yes"
     class="value">
--
</div>

</div>


<div class="card">

<div class="label">
SIGNAL SCORE
</div>

<div id="score"
     class="value">
--
</div>

</div>


<div class="card wide">

<div class="label">
⚔️ BUYER / SELLER BATTLE
</div>

<div id="battle"
     class="value">
--
</div>

<div class="row">

<span id="buy">
🟢 Buyers --
</span>

<span id="sell">
🔴 Sellers --
</span>

</div>

<div class="bar">

<div id="buybar"
     style="width:50%">
</div>

</div>

<div id="delta"
     class="small">
Delta: --
</div>

</div>


<div class="card wide">

<div class="label">
📖 ORDER BOOK PRESSURE
</div>

<div id="book"
     class="value">
--
</div>

<div id="bookdetail"
     class="small">
--
</div>

</div>


<div class="card wide">

<div class="label">
🧠 PREDICTION STRENGTH
</div>

<div id="pred"
     class="big">
--
</div>

<div id="predetail"
     class="small">
Alignment score — not a probability.
</div>

</div>


<div class="card wide">

<div class="label">
DATA QUALITY BRAIN
</div>

<div id="quality"
     class="big">
--
</div>

<div id="qdetail"
     class="small">
--
</div>

</div>


<div class="card wide">

<div class="label">
WHY THE ENGINE CHOSE THIS
</div>

<div id="reasons"
     class="small reason">
Waiting...
</div>

</div>


</div>


<div class="footer">

BRTI-style composite only — not official CF Benchmarks BRTI.
Trade-flow and order-book readings are exchange-specific proxies.

</div>

</main>


<script>

const $ = id =>
    document.getElementById(id);


function money(value) {

    if (value == null)
        return "--";

    return "$" +
        Number(value).toLocaleString(
            undefined,
            {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            }
        );
}


function percent(value) {

    if (value == null)
        return "--";

    return (
        value >= 0
        ? "+"
        : ""
    )
    +
    Number(value).toFixed(3)
    +
    "%";
}


function clock(seconds) {

    if (seconds == null)
        return "--";

    return String(
        Math.floor(seconds / 60)
    ).padStart(2, "0")
    +
    ":"
    +
    String(
        seconds % 60
    ).padStart(2, "0");
}


async function refresh() {

    try {

        const response =
            await fetch(
                "/api/state?x=" +
                Date.now(),
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();


        const signal =
            data.signal || {};

        const market =
            data.market || {};

        const pressure =
            data.buy_sell || {};

        const book =
            data.order_book || {};

        const quality =
            data.quality || {};

        const latency =
            data.latency || {};


        const verdict =
            $("verdict");


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


        $("label").textContent =
            signal.label ||
            "🟡 WAIT";


        $("confidence").textContent =
            (
                signal.confidence ||
                0
            )
            +
            "% confidence";


        $("agreement").textContent =
            (
                signal.bullish ||
                0
            )
            +
            " BULLISH / "
            +
            (
                signal.bearish ||
                0
            )
            +
            " BEARISH";


        $("btc").textContent =
            money(
                data.btc
            );


        $("btcAge").textContent =
            latency.btc_age_ms == null
            ?
            "--"
            :
            Math.round(
                latency.btc_age_ms
            )
            +
            " ms • "
            +
            (
                latency.btc_stream
                ?
                "LIVE"
                :
                "REST"
            );


        $("target").textContent =
            money(
                market.target
            );


        $("ticker").textContent =
            market.ticker ||
            "--";


        if (
            data.btc
            &&
            market.target
        ) {

            const difference =
                data.btc -
                market.target;


            $("distance").textContent =
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
                );


            $("distancePct").textContent =
                percent(
                    difference
                    /
                    market.target
                    *
                    100
                );

        }


        $("countdown").textContent =
            clock(
                data.countdown
            );


        $("m1").textContent =
            percent(
                signal.m1
            );


        $("m5").textContent =
            percent(
                signal.m5
            );


        $("m15").textContent =
            percent(
                signal.m15
            );


        $("structure").textContent =
            signal.structure ||
            "--";


        if (
            signal.m15 > 0
            &&
            signal.m5 > 0
            &&
            signal.m1 > 0
        ) {

            $("accel").textContent =
                "ACCELERATING UP";

        }

        else if (
            signal.m15 < 0
            &&
            signal.m5 < 0
            &&
            signal.m1 < 0
        ) {

            $("accel").textContent =
                "ACCELERATING DOWN";

        }

        else {

            $("accel").textContent =
                "MIXED";
        }


        $("reversal").textContent =
            signal.reversal ||
            "--";


        let yes = null;


        if (
            market.yes_bid != null
            &&
            market.yes_ask != null
        ) {

            yes =
                (
                    market.yes_bid
                    +
                    market.yes_ask
                )
                /
                2;

        }

        else if (
            market.last != null
        ) {

            yes =
                market.last;
        }


        $("yes").textContent =
            yes == null
            ?
            "--"
            :
            (
                yes * 100
            ).toFixed(1)
            +
            "%";


        $("score").textContent =
            signal.score ??
            "--";


        $("battle").textContent =
            pressure.winner ||
            "--";


        $("buy").textContent =
            "🟢 Buyers " +
            (
                pressure.buy_pct == null
                ?
                "--"
                :
                pressure.buy_pct.toFixed(1)
                +
                "%"
            );


        $("sell").textContent =
            "🔴 Sellers " +
            (
                pressure.sell_pct == null
                ?
                "--"
                :
                pressure.sell_pct.toFixed(1)
                +
                "%"
            );


        $("buybar").style.width =
            (
                pressure.buy_pct ||
                50
            )
            +
            "%";


        $("delta").textContent =
            "Delta: " +
            (
                pressure.delta == null
                ?
                "--"
                :
                (
                    pressure.delta >= 0
                    ?
                    "+$"
                    :
                    "-$"
                )
                +
                Math.abs(
                    pressure.delta
                ).toLocaleString(
                    undefined,
                    {
                        maximumFractionDigits: 0
                    }
                )
            );


        $("book").textContent =
            book.winner ||
            "--";


        $("bookdetail").textContent =
            book.bid_pct == null
            ?
            "--"
            :
            "Bids " +
            book.bid_pct.toFixed(1)
            +
            "% • Asks " +
            book.ask_pct.toFixed(1)
            +
            "%";


        const prediction =
            Math.min(
                100,
                Math.round(
                    (
                        Math.max(
                            signal.bullish || 0,
                            signal.bearish || 0
                        )
                        /
                        Math.max(
                            1,
                            signal.score || 1
                        )
                    )
                    *
                    100
                )
            );


        $("pred").textContent =
            prediction +
            " / 100";


        $("predetail").textContent =
            (
                signal.bullish ||
                0
            )
            +
            " bullish confirmations • "
            +
            (
                signal.bearish ||
                0
            )
            +
            " bearish confirmations";


        $("quality").textContent =
            (
                quality.score ||
                0
            )
            +
            "/100 "
            +
            (
                quality.grade ||
                ""
            );


        $("qdetail").textContent =
            (
                data.candles ||
                0
            )
            +
            " one-minute history candles • "
            +
            (
                data.source ||
                "unknown"
            );


        $("reasons").textContent =
            (
                signal.reasons ||
                []
            )
            .map(
                reason =>
                    "• " +
                    reason
            )
            .join("\n")
            ||
            "Waiting...";


        $("status").textContent =
            "Auto-refresh • " +
            new Date().toLocaleTimeString();

    }

    catch (error) {

        $("status").textContent =
            "Reconnecting...";

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
'''


# =========================================================
# ROUTES
# =========================================================

@app.get("/")
def index():

    return render_template_string(
        PAGE
    )


@app.get("/api/state")
def api_state():

    now = time.time()

    if (
        cache["state"] is not None
        and
        now - cache["time"]
        <
        CACHE_SECONDS
    ):

        return jsonify(
            cache["state"]
        )

    state = collect_state()

    cache.update(
        time=now,
        state=state
    )

    return jsonify(
        state
    )


@app.get("/api/live")
def api_live():

    with lock:

        if live["received"]:

            age = (
                time.time()
                -
                live["received"]
            ) * 1000

        else:

            age = None

        return jsonify({
            "price": live["price"],
            "age_ms": age,
            "connected": live["connected"],
            "source": live["source"]
        })


# =========================================================
# START LIVE STREAMS
# =========================================================

start_streams()


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
