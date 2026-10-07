import os
import re
import json
import time
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://external-api.kalshi.com/trade-api/v2"
)

BINANCE_REST = "https://api.binance.com"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

REQUEST_TIMEOUT = 10

# ============================================================
# GLOBAL STATE
# ============================================================

state = {
    "btc": None,
    "market": None,
    "strike": None,
    "seconds_left": None,
    "market_close": None,

    "btc_vs_strike": None,
    "distance_pct": None,

    "kalshi_probability": None,
    "kalshi_yes_bid": None,
    "kalshi_yes_ask": None,

    "trend_1m": "WAIT",
    "trend_5m": "WAIT",
    "trend_15m": "WAIT",

    "momentum_1m": 0,
    "momentum_5m": 0,
    "momentum_15m": 0,

    "rsi_1m": None,
    "rsi_5m": None,

    "ema9": None,
    "ema21": None,

    "delta": 0,
    "cvd": 0,

    "large_buy": 0,
    "large_sell": 0,

    "bid": None,
    "ask": None,
    "spread": None,
    "microprice": None,
    "book_imbalance": 0,

    "score": 0,
    "confidence": 0,
    "verdict": "WAITING",

    "phase": "WAITING",
    "reason": "Waiting for market data",

    "fed_status": "STARTING",
    "kalshi_status": "STARTING",

    "last_update": None,

    "wins": 0,
    "losses": 0,
    "accuracy": 50.0,

    "error": None,
}

lock = threading.Lock()

engine_started = False
engine_lock = threading.Lock()

# Used to prevent processing the same Binance trade twice.
last_trade_id = None

# Running order-flow totals.
flow_lock = threading.Lock()


# ============================================================
# HELPERS
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def safe_float(value):
    try:
        if value is None:
            return None

        if isinstance(value, bool):
            return None

        return float(value)
    except Exception:
        return None


def safe_int(value):
    try:
        return int(value)
    except Exception:
        return None


def fmt_money(value):
    if value is None:
        return "—"
    return f"${value:,.2f}"


def set_state(**kwargs):
    with lock:
        state.update(kwargs)
        state["last_update"] = now_iso()


def get_state():
    with lock:
        return dict(state)


# ============================================================
# RSI
# ============================================================

def calculate_rsi(closes, period=14):
    if not closes or len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]

        if change >= 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    recent_gains = gains[-period:]
    recent_losses = losses[-period:]

    avg_gain = sum(recent_gains) / period
    avg_loss = sum(recent_losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


# ============================================================
# EMA
# ============================================================

def calculate_ema(values, period):
    if not values or len(values) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(values[:period]) / period

    for price in values[period:]:
        ema = (price - ema) * multiplier + ema

    return ema


# ============================================================
# BINANCE CANDLES
# ============================================================

def get_binance_klines(interval, limit=100):
    url = f"{BINANCE_REST}/api/v3/klines"

    params = {
        "symbol": "BTCUSDT",
        "interval": interval,
        "limit": limit,
    }

    response = requests.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    data = response.json()

    closes = []

    for candle in data:
        try:
            closes.append(float(candle[4]))
        except Exception:
            pass

    return closes


def update_technicals():
    try:
        closes_1m = get_binance_klines("1m", 100)
        closes_5m = get_binance_klines("5m", 100)
        closes_15m = get_binance_klines("15m", 100)

        if not closes_1m:
            return

        btc = closes_1m[-1]

        # 1-minute trend
        ema9_1m = calculate_ema(closes_1m, 9)
        ema21_1m = calculate_ema(closes_1m, 21)

        if ema9_1m is not None and ema21_1m is not None:
            if ema9_1m > ema21_1m:
                trend_1m = "UP"
            elif ema9_1m < ema21_1m:
                trend_1m = "DOWN"
            else:
                trend_1m = "WAIT"
        else:
            trend_1m = "WAIT"

        # 5-minute trend
        ema9_5m = calculate_ema(closes_5m, 9)
        ema21_5m = calculate_ema(closes_5m, 21)

        if ema9_5m is not None and ema21_5m is not None:
            if ema9_5m > ema21_5m:
                trend_5m = "UP"
            elif ema9_5m < ema21_5m:
                trend_5m = "DOWN"
            else:
                trend_5m = "WAIT"
        else:
            trend_5m = "WAIT"

        # 15-minute trend
        ema9_15m = calculate_ema(closes_15m, 9)
        ema21_15m = calculate_ema(closes_15m, 21)

        if ema9_15m is not None and ema21_15m is not None:
            if ema9_15m > ema21_15m:
                trend_15m = "UP"
            elif ema9_15m < ema21_15m:
                trend_15m = "DOWN"
            else:
                trend_15m = "WAIT"
        else:
            trend_15m = "WAIT"

        # Momentum
        def momentum(closes, lookback):
            if len(closes) <= lookback:
                return 0

            old = closes[-lookback - 1]

            if old == 0:
                return 0

            return ((closes[-1] - old) / old) * 100

        m1 = momentum(closes_1m, 1)
        m5 = momentum(closes_5m, 1)
        m15 = momentum(closes_15m, 1)

        rsi1 = calculate_rsi(closes_1m)
        rsi5 = calculate_rsi(closes_5m)

        set_state(
            btc=btc,
            trend_1m=trend_1m,
            trend_5m=trend_5m,
            trend_15m=trend_15m,
            momentum_1m=m1,
            momentum_5m=m5,
            momentum_15m=m15,
            rsi_1m=rsi1,
            rsi_5m=rsi5,
            ema9=ema9_1m,
            ema21=ema21_1m,
            fed_status="CONNECTED",
            error=None,
        )

    except Exception as exc:
        set_state(
            fed_status="ERROR",
            error=f"Binance technicals: {str(exc)}",
        )


# ============================================================
# BINANCE BEST BID / ASK
# ============================================================

def update_book():
    while True:
        try:
            url = f"{BINANCE_REST}/api/v3/ticker/bookTicker"

            response = requests.get(
                url,
                params={"symbol": "BTCUSDT"},
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            data = response.json()

            bid = safe_float(data.get("bidPrice"))
            ask = safe_float(data.get("askPrice"))

            if bid is not None and ask is not None:

                spread = ask - bid

                microprice = (bid + ask) / 2

                set_state(
                    bid=bid,
                    ask=ask,
                    spread=spread,
                    microprice=microprice,
                )

        except Exception as exc:
            set_state(
                error=f"Book feed: {str(exc)}"
            )

        time.sleep(1)


# ============================================================
# BINANCE TRADE WEBSOCKET
# ============================================================

def handle_trade(message):
    global last_trade_id

    try:
        data = json.loads(message)

        trade_id = data.get("a")

        if trade_id == last_trade_id:
            return

        last_trade_id = trade_id

        price = safe_float(data.get("p"))
        quantity = safe_float(data.get("q"))

        if price is None or quantity is None:
            return

        value = price * quantity

        # Binance "m" means buyer is market maker.
        # m=True generally means aggressive sell.
        is_sell = bool(data.get("m"))

        with flow_lock:
            if is_sell:
                state["delta"] -= value
            else:
                state["delta"] += value

            state["cvd"] += (-value if is_sell else value)

            if is_sell:
                if value >= 250000:
                    state["large_sell"] += value
            else:
                if value >= 250000:
                    state["large_buy"] += value

        set_state(
            btc=price,
            fed_status="CONNECTED",
            error=None,
        )

    except Exception as exc:
        set_state(
            error=f"Trade feed: {str(exc)}"
        )


def binance_websocket_loop():
    while True:

        def on_message(ws, message):
            handle_trade(message)

        def on_error(ws, error):
            set_state(
                fed_status="RECONNECTING",
                error=f"Binance websocket: {error}",
            )

        def on_close(ws, close_status_code, close_msg):
            set_state(
                fed_status="RECONNECTING"
            )

        def on_open(ws):
            set_state(
                fed_status="CONNECTED",
                error=None,
            )

        try:
            ws = websocket.WebSocketApp(
                BINANCE_WS,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as exc:
            set_state(
                fed_status="RECONNECTING",
                error=f"Websocket connection: {str(exc)}",
            )

        time.sleep(3)


# ============================================================
# KALSHI
# ============================================================

def parse_time(value):
    if value is None:
        return None

    try:
        if isinstance(value, (int, float)):
            # Handle milliseconds.
            if value > 100000000000:
                return float(value) / 1000

            return float(value)

        text = str(value).strip()

        if text.isdigit():
            number = float(text)

            if number > 100000000000:
                number /= 1000

            return number

        text = text.replace("Z", "+00:00")

        return datetime.fromisoformat(text).timestamp()

    except Exception:
        return None


def find_strike(market):
    possible_fields = [
        "floor_strike",
        "strike_price",
        "functional_strike",
        "target_price",
        "strike",
        "cap_strike",
    ]

    for field in possible_fields:
        value = safe_float(market.get(field))

        if value is not None:
            return value

    # Sometimes strike can be embedded in title/subtitle.
    text = " ".join(
        str(market.get(x, ""))
        for x in ["title", "subtitle", "ticker"]
    )

    matches = re.findall(r"\$?(\d{4,6}(?:\.\d+)?)", text)

    if matches:
        numbers = [float(x) for x in matches]

        # Bitcoin strike should be a realistic BTC price.
        realistic = [
            x for x in numbers
            if 1000 < x < 1000000
        ]

        if realistic:
            return realistic[0]

    return None


def find_close_time(market):
    fields = [
        "close_time",
        "expiration_time",
        "end_time",
        "close_ts",
    ]

    for field in fields:
        value = parse_time(market.get(field))

        if value is not None:
            return value

    return None


def get_kalshi_markets():
    url = f"{KALSHI_BASE}/markets"

    # First try the BTC 15-minute series.
    try:
        response = requests.get(
            url,
            params={
                "status": "open",
                "limit": 100,
                "series_ticker": "KXBTC15M",
            },
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status()

        data = response.json()

        markets = data.get("markets", [])

        if markets:
            return markets

    except Exception:
        pass

    # Fallback.
    response = requests.get(
        url,
        params={
            "status": "open",
            "limit": 200,
        },
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    data = response.json()

    markets = data.get("markets", [])

    return [
        m for m in markets
        if str(m.get("series_ticker", "")).upper() == "KXBTC15M"
        or str(m.get("ticker", "")).upper().startswith("KXBTC15M")
    ]


def select_current_market(markets):
    now = time.time()

    candidates = []

    for market in markets:
        ticker = market.get("ticker")

        if not ticker:
            continue

        ticker_upper = str(ticker).upper()

        if not ticker_upper.startswith("KXBTC15M"):
            continue

        close_time = find_close_time(market)

        if close_time is None:
            continue

        seconds_left = close_time - now

        # Current market should expire within approximately 20 minutes.
        if 0 < seconds_left <= 20 * 60:
            candidates.append(
                (
                    seconds_left,
                    market,
                    close_time,
                )
            )

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])

    return candidates[0][1], candidates[0][2]


def update_kalshi():
    while True:

        try:
            markets = get_kalshi_markets()

            selected = select_current_market(markets)

            if not selected:
                set_state(
                    kalshi_status="NO_MARKET",
                    error=None,
                )

                time.sleep(3)
                continue

            market, close_time = selected

            ticker = market.get("ticker")

            strike = find_strike(market)

            yes_bid = safe_float(market.get("yes_bid"))
            yes_ask = safe_float(market.get("yes_ask"))
            last_price = safe_float(market.get("last_price"))

            # Market-implied probability.
            if yes_bid is not None and yes_ask is not None:
                probability = (yes_bid + yes_ask) / 2

            elif last_price is not None:
                probability = last_price

            else:
                probability = None

            now = time.time()
            seconds_left = max(0, int(close_time - now))

            btc = get_state().get("btc")

            btc_vs_strike = None
            distance_pct = None

            if btc is not None and strike is not None:
                btc_vs_strike = btc - strike

                if strike != 0:
                    distance_pct = (
                        (btc - strike) / strike
                    ) * 100

            # Reset order flow when a new market begins.
            old_market = get_state().get("market")

            if old_market != ticker:
                with flow_lock:
                    state["delta"] = 0
                    state["cvd"] = 0
                    state["large_buy"] = 0
                    state["large_sell"] = 0

            set_state(
                market=ticker,
                strike=strike,
                market_close=datetime.fromtimestamp(
                    close_time,
                    tz=timezone.utc
                ).isoformat(),
                seconds_left=seconds_left,
                btc_vs_strike=btc_vs_strike,
                distance_pct=distance_pct,
                kalshi_probability=probability,
                kalshi_yes_bid=yes_bid,
                kalshi_yes_ask=yes_ask,
                kalshi_status="CONNECTED",
                error=None,
            )

        except Exception as exc:
            set_state(
                kalshi_status="ERROR",
                error=f"Kalshi: {str(exc)}",
            )

        time.sleep(3)


# ============================================================
# AI DECISION ENGINE
# ============================================================

def calculate_decision():

    s = get_state()

    btc = s["btc"]
    strike = s["strike"]
    seconds_left = s["seconds_left"]

    if btc is None or strike is None or seconds_left is None:
        set_state(
            verdict="WAITING",
            confidence=0,
            score=0,
            phase="WAITING",
            reason="Waiting for market data",
        )
        return

    # Phase.
    if seconds_left <= 60:
        phase = "FINAL MINUTE"
    elif seconds_left <= 300:
        phase = "LATE"
    elif seconds_left <= 600:
        phase = "MIDDLE"
    else:
        phase = "EARLY"

    score = 0
    reasons = []

    # --------------------------------------------------------
    # STRIKE POSITION
    # --------------------------------------------------------

    distance = btc - strike

    if distance > 0:
        score += 3
        reasons.append("BTC above strike")
    elif distance < 0:
        score -= 3
        reasons.append("BTC below strike")

    # --------------------------------------------------------
    # MULTI-TIMEFRAME TREND
    # --------------------------------------------------------

    trends = [
        s["trend_1m"],
        s["trend_5m"],
        s["trend_15m"],
    ]

    up_count = trends.count("UP")
    down_count = trends.count("DOWN")

    score += up_count * 2
    score -= down_count * 2

    if up_count >= 2:
        reasons.append("multi-timeframe UP")

    if down_count >= 2:
        reasons.append("multi-timeframe DOWN")

    # --------------------------------------------------------
    # MOMENTUM
    # --------------------------------------------------------

    m1 = safe_float(s["momentum_1m"]) or 0
    m5 = safe_float(s["momentum_5m"]) or 0
    m15 = safe_float(s["momentum_15m"]) or 0

    if m1 > 0:
        score += 1
    elif m1 < 0:
        score -= 1

    if m5 > 0:
        score += 1
    elif m5 < 0:
        score -= 1

    if m15 > 0:
        score += 1
    elif m15 < 0:
        score -= 1

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    rsi = s["rsi_1m"]

    if rsi is not None:

        if 52 <= rsi <= 68:
            score += 1

        elif 32 <= rsi <= 48:
            score -= 1

        elif rsi >= 75:
            score -= 1

        elif rsi <= 25:
            score += 1

    # --------------------------------------------------------
    # DELTA
    # --------------------------------------------------------

    delta = s["delta"]

    if delta > 0:
        score += 2
        reasons.append("positive delta")

    elif delta < 0:
        score -= 2
        reasons.append("negative delta")

    # --------------------------------------------------------
    # CVD
    # --------------------------------------------------------

    cvd = s["cvd"]

    if cvd > 0:
        score += 2

    elif cvd < 0:
        score -= 2

    # --------------------------------------------------------
    # LARGE TRADES
    # --------------------------------------------------------

    large_buy = s["large_buy"]
    large_sell = s["large_sell"]

    if large_buy > large_sell:
        score += 2

    elif large_sell > large_buy:
        score -= 2

    # --------------------------------------------------------
    # KALSHI MARKET SIGNAL
    # --------------------------------------------------------

    probability = s["kalshi_probability"]

    if probability is not None:

        if probability >= 0.60:
            score += 1

        elif probability <= 0.40:
            score -= 1

    # --------------------------------------------------------
    # CONFIDENCE
    # --------------------------------------------------------

    max_score = 25

    confidence = min(
        95,
        max(
            0,
            int(abs(score) / max_score * 100)
        )
    )

    # Require stronger agreement before giving a verdict.
    if score >= 7:
        verdict = "UP"

    elif score <= -7:
        verdict = "DOWN"

    else:
        verdict = "WAITING"

    # Last-minute behavior:
    # Do not force a trade simply because time is running out.
    if phase == "FINAL MINUTE":

        # Require stronger confirmation.
        if score >= 9:
            verdict = "UP"

        elif score <= -9:
            verdict = "DOWN"

        else:
            verdict = "WAITING"

    reason = "; ".join(reasons[-5:])

    if not reason:
        reason = "Signals not strong enough"

    set_state(
        score=score,
        confidence=confidence,
        verdict=verdict,
        phase=phase,
        reason=reason,
    )


# ============================================================
# MAIN ENGINE LOOP
# ============================================================

def engine_loop():
    set_state(
        fed_status="STARTING",
        kalshi_status="STARTING",
    )

    # Start Binance websocket.
    threading.Thread(
        target=binance_websocket_loop,
        daemon=True,
        name="binance-trades",
    ).start()

    # Start Binance order book.
    threading.Thread(
        target=update_book,
        daemon=True,
        name="binance-book",
    ).start()

    # Start Kalshi.
    threading.Thread(
        target=update_kalshi,
        daemon=True,
        name="kalshi-market",
    ).start()

    # Main technical/decision loop.
    while True:

        try:
            update_technicals()
            calculate_decision()

        except Exception as exc:
            set_state(
                error=f"Engine: {str(exc)}"
            )

        time.sleep(3)


# ============================================================
# START ENGINE WHEN GUNICORN IMPORTS APP
# ============================================================

def start_engine():
    global engine_started

    with engine_lock:

        if engine_started:
            return

        engine_started = True

        thread = threading.Thread(
            target=engine_loop,
            daemon=True,
            name="btc-strike-engine",
        )

        thread.start()


# IMPORTANT:
# This runs when Gunicorn imports "app:app".
start_engine()


# ============================================================
# API
# ============================================================

@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/state")
def api_state():
    return jsonify(get_state())


@app.route("/health")
def health():
    s = get_state()

    return jsonify({
        "status": "ok",
        "btc_feed": s["fed_status"],
        "kalshi_feed": s["kalshi_status"],
        "market": s["market"],
        "updated": s["last_update"],
    })


# ============================================================
# DASHBOARD
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">

<title>BTC Strike AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #070b12;
    color: #f4f6fb;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}

.header {
    padding: 28px 20px;
    border-bottom: 1px solid #202938;
}

.title {
    font-size: 36px;
    font-weight: 800;
}

.subtitle {
    margin-top: 8px;
    color: #91a0ba;
    font-size: 18px;
}

.container {
    padding: 20px;
    max-width: 900px;
    margin: auto;
}

.card {
    background: #111925;
    border: 1px solid #263246;
    border-radius: 25px;
    padding: 22px;
    margin-bottom: 18px;
}

.label {
    color: #8292ad;
    font-size: 16px;
    text-transform: uppercase;
    letter-spacing: .5px;
}

.big {
    font-size: 48px;
    font-weight: 800;
    margin-top: 15px;
}

.verdict {
    text-align: center;
}

.wait {
    color: #ffd21a;
}

.up {
    color: #37e37f;
}

.down {
    color: #ff5d6c;
}

.confidence {
    margin-top: 8px;
    color: #b4bfd0;
    font-size: 20px;
}

.grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 15px;
}

.row {
    display: flex;
    justify-content: space-between;
    padding: 15px 0;
    border-bottom: 1px solid #253044;
    font-size: 18px;
}

.row:last-child {
    border-bottom: 0;
}

.value {
    font-weight: 700;
}

.status {
    font-size: 14px;
    margin-top: 12px;
    color: #8392aa;
}

.green {
    color: #37e37f;
}

.yellow {
    color: #ffd21a;
}

.red {
    color: #ff5d6c;
}

@media(max-width:600px) {

    .title {
        font-size: 32px;
    }

    .grid {
        grid-template-columns: 1fr 1fr;
    }

    .big {
        font-size: 42px;
    }
}

</style>
</head>

<body>

<div class="header">

    <div class="title">
        ⚡ BTC STRIKE AI
    </div>

    <div class="subtitle">
        Kalshi 15-Minute Decision Engine
    </div>

</div>

<div class="container">

    <div class="card verdict">

        <div class="label">
            Model Verdict
        </div>

        <div id="verdict"
             class="big wait">
            WAITING
        </div>

        <div id="confidence"
             class="confidence">
            Confidence: 0%
        </div>

        <div id="reason"
             class="status">
            Waiting for market data
        </div>

    </div>


    <div class="grid">

        <div class="card">

            <div class="label">
                BTC Price
            </div>

            <div id="btc"
                 class="big">
                —
            </div>

        </div>


        <div class="card">

            <div class="label">
                Countdown
            </div>

            <div id="countdown"
                 class="big">
                —
            </div>

            <div id="phase"
                 class="status">
                WAITING
            </div>

        </div>

    </div>


    <div class="card">

        <div class="label">
            Kalshi Strike
        </div>

        <div id="strike"
             class="big">
            —
        </div>

        <div class="row">
            <span>BTC vs Strike</span>
            <span id="vsStrike">—</span>
        </div>

        <div class="row">
            <span>Distance %</span>
            <span id="distance">—</span>
        </div>

        <div class="row">
            <span>Kalshi UP Probability</span>
            <span id="probability">—</span>
        </div>

    </div>


    <div class="card">

        <div class="label">
            Multi-Timeframe Trend
        </div>

        <div class="row">
            <span>1 Minute</span>
            <span id="trend1">WAIT</span>
        </div>

        <div class="row">
            <span>5 Minute</span>
            <span id="trend5">WAIT</span>
        </div>

        <div class="row">
            <span>15 Minute</span>
            <span id="trend15">WAIT</span>
        </div>

    </div>


    <div class="card">

        <div class="label">
            Momentum
        </div>

        <div class="row">
            <span>1m Momentum</span>
            <span id="m1">0%</span>
        </div>

        <div class="row">
            <span>5m Momentum</span>
            <span id="m5">0%</span>
        </div>

        <div class="row">
            <span>15m Momentum</span>
            <span id="m15">0%</span>
        </div>

        <div class="row">
            <span>RSI 1m</span>
            <span id="rsi">—</span>
        </div>

    </div>


    <div class="card">

        <div class="label">
            Order Flow
        </div>

        <div class="row">
            <span>Delta</span>
            <span id="delta">0</span>
        </div>

        <div class="row">
            <span>CVD</span>
            <span id="cvd">0</
