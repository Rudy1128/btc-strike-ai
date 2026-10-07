import os
import time
import math
import threading
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify, render_template_string


# ============================================================
# BTC STRIKE AI
# Kalshi 15-Minute BTC Decision Support Engine
# ============================================================

app = Flask(__name__)

# -----------------------------
# Configuration
# -----------------------------

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://external-api.kalshi.com/trade-api/v2"
)

BINANCE_REST = "https://api.binance.com"

SYMBOL = "BTCUSDT"

REQUEST_TIMEOUT = 8

state_lock = threading.Lock()

state = {
    "btc": None,
    "bid": None,
    "ask": None,
    "microprice": None,

    "market": None,
    "strike": None,
    "market_close": None,
    "seconds_left": None,

    "kalshi_yes_bid": None,
    "kalshi_yes_ask": None,
    "kalshi_probability": None,

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

    "book_imbalance": 0,
    "spread": None,

    "large_buy": 0,
    "large_sell": 0,

    "btc_vs_strike": None,
    "distance_pct": None,

    "score": 0,
    "confidence": 0,
    "verdict": "WAITING",

    "phase": "WAITING",
    "reason": "Waiting for market data",

    "feed_status": "STARTING",
    "kalshi_status": "STARTING",

    "prediction_window": None,
    "prediction_started": None,

    "wins": 0,
    "losses": 0,
    "accuracy": 50.0,

    "last_update": None,
    "error": None,
}


# ============================================================
# Utility functions
# ============================================================

def now_ts():
    return time.time()


def safe_float(value):
    try:
        if value is None:
            return None

        if isinstance(value, bool):
            return None

        return float(value)
    except Exception:
        return None


def clamp(value, low, high):
    return max(low, min(high, value))


def ema(values, period):
    if not values:
        return None

    if len(values) < period:
        period = len(values)

    if period <= 0:
        return None

    multiplier = 2 / (period + 1)

    result = values[0]

    for value in values[1:]:
        result = (value - result) * multiplier + result

    return result


def calculate_rsi(values, period=14):
    if len(values) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(values)):
        change = values[i] - values[i - 1]

        if change > 0:
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


def trend_from_candles(candles):
    if len(candles) < 22:
        return "WAIT"

    closes = [c["close"] for c in candles]

    fast = ema(closes, 9)
    slow = ema(closes, 21)

    if fast is None or slow is None:
        return "WAIT"

    recent = closes[-1]
    previous = closes[-4]

    if fast > slow and recent > previous:
        return "UP"

    if fast < slow and recent < previous:
        return "DOWN"

    return "MIXED"


def momentum_from_candles(candles, lookback=5):
    if len(candles) <= lookback:
        return 0

    current = candles[-1]["close"]
    old = candles[-1 - lookback]["close"]

    if old == 0:
        return 0

    return ((current - old) / old) * 100


# ============================================================
# Binance REST
# ============================================================

def get_klines(interval, limit=100):
    try:
        url = f"{BINANCE_REST}/api/v3/klines"

        params = {
            "symbol": SYMBOL,
            "interval": interval,
            "limit": limit
        }

        response = requests.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        raw = response.json()

        candles = []

        for item in raw:
            candles.append({
                "time": item[0],
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5])
            })

        return candles

    except Exception:
        return []


def refresh_technicals():

    candles_1m = get_klines("1m", 100)
    candles_5m = get_klines("5m", 100)
    candles_15m = get_klines("15m", 100)

    if not candles_1m:
        return

    closes_1m = [x["close"] for x in candles_1m]

    rsi_1m = calculate_rsi(closes_1m, 14)

    rsi_5m = None

    if candles_5m:
        rsi_5m = calculate_rsi(
            [x["close"] for x in candles_5m],
            14
        )

    ema9_value = ema(closes_1m, 9)
    ema21_value = ema(closes_1m, 21)

    with state_lock:

        state["trend_1m"] = trend_from_candles(candles_1m)

        if candles_5m:
            state["trend_5m"] = trend_from_candles(candles_5m)

        if candles_15m:
            state["trend_15m"] = trend_from_candles(candles_15m)

        state["momentum_1m"] = round(
            momentum_from_candles(candles_1m),
            4
        )

        if candles_5m:
            state["momentum_5m"] = round(
                momentum_from_candles(candles_5m),
                4
            )

        if candles_15m:
            state["momentum_15m"] = round(
                momentum_from_candles(candles_15m),
                4
            )

        state["rsi_1m"] = (
            round(rsi_1m, 2)
            if rsi_1m is not None
            else None
        )

        state["rsi_5m"] = (
            round(rsi_5m, 2)
            if rsi_5m is not None
            else None
        )

        state["ema9"] = ema9_value
        state["ema21"] = ema21_value


# ============================================================
# Kalshi
# ============================================================

def parse_timestamp(value):

    if value is None:
        return None

    if isinstance(value, (int, float)):
        value = float(value)

        if value > 100000000000:
            value /= 1000

        return value

    text = str(value).strip()

    try:
        return float(text)
    except Exception:
        pass

    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        return datetime.fromisoformat(text).timestamp()

    except Exception:
        return None


def extract_strike(market):

    preferred = [
        "floor_strike",
        "strike_price",
        "functional_strike",
        "target_price",
        "strike",
        "cap_strike"
    ]

    for key in preferred:
        value = safe_float(market.get(key))

        if value is not None and value > 100:
            return value

    return None


def get_close_time(market):

    for key in [
        "close_time",
        "expiration_time",
        "end_time",
        "close_ts"
    ]:

        value = parse_timestamp(
            market.get(key)
        )

        if value:
            return value

    return None


def get_kalshi_markets():

    urls = []

    # Preferred efficient query
    urls.append(
        f"{KALSHI_BASE}/markets"
        "?status=open"
        "&limit=100"
        "&series_ticker=KXBTC15M"
    )

    # Fallback
    urls.append(
        f"{KALSHI_BASE}/markets"
        "?status=open"
        "&limit=200"
    )

    for url in urls:

        try:

            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code != 200:
                continue

            data = response.json()

            markets = data.get("markets", [])

            if markets:
                return markets

        except Exception:
            continue

    return []


def find_current_market():

    markets = get_kalshi_markets()

    now = now_ts()

    candidates = []

    for market in markets:

        ticker = str(
            market.get("ticker", "")
        ).upper()

        series = str(
            market.get("series_ticker", "")
        ).upper()

        event = str(
            market.get("event_ticker", "")
        ).upper()

        if not (
            ticker.startswith("KXBTC15M")
            or series == "KXBTC15M"
            or event.startswith("KXBTC15M")
        ):
            continue

        close_time = get_close_time(market)

        if close_time is None:
            continue

        seconds_left = close_time - now

        # Only accept a market that is currently active
        # or about to become active.
        if seconds_left <= 0:
            continue

        if seconds_left > 20 * 60:
            continue

        strike = extract_strike(market)

        if strike is None:
            continue

        candidates.append(
            (
                seconds_left,
                market,
                strike,
                close_time
            )
        )

    if not candidates:
        return None

    # The active 15-minute contract is the one
    # with the nearest positive expiration.
    candidates.sort(key=lambda x: x[0])

    return candidates[0]


def refresh_kalshi():

    result = find_current_market()

    if result is None:

        with state_lock:
            state["kalshi_status"] = "WAITING"
            state["market"] = None
            state["strike"] = None

        return

    seconds_left, market, strike, close_time = result

    ticker = market.get("ticker")

    yes_bid = safe_float(
        market.get("yes_bid")
    )

    yes_ask = safe_float(
        market.get("yes_ask")
    )

    last_price = safe_float(
        market.get("last_price")
    )

    probability = None

    if yes_bid is not None and yes_ask is not None:
        probability = (yes_bid + yes_ask) / 2

    elif last_price is not None:
        probability = last_price

    elif yes_bid is not None:
        probability = yes_bid

    elif yes_ask is not None:
        probability = yes_ask

    with state_lock:

        previous_market = state["market"]

        state["market"] = ticker
        state["strike"] = strike
        state["market_close"] = close_time
        state["seconds_left"] = max(0, seconds_left)

        state["kalshi_yes_bid"] = yes_bid
        state["kalshi_yes_ask"] = yes_ask
        state["kalshi_probability"] = probability

        state["kalshi_status"] = "CONNECTED"

        # Reset window-specific flow when Kalshi rolls
        if previous_market != ticker:

            state["delta"] = 0
            state["cvd"] = 0
            state["large_buy"] = 0
            state["large_sell"] = 0

            state["prediction_window"] = ticker
            state["prediction_started"] = now_ts()


# ============================================================
# Binance WebSocket
# ============================================================

def process_trade(message):

    try:

        price = safe_float(message.get("p"))
        quantity = safe_float(message.get("q"))

        if price is None or quantity is None:
            return

        # Binance:
        # m=True means buyer was maker,
        # therefore seller was taker.
        buyer_taker = not bool(
            message.get("m", False)
        )

        signed_volume = (
            quantity
            if buyer_taker
            else -quantity
        )

        notional = price * quantity

        # Dynamic large-trade threshold.
        # $250k+ is treated as significant.
        large_trade = notional >= 250000

        with state_lock:

            state["btc"] = price

            state["delta"] += signed_volume
            state["cvd"] += signed_volume

            if large_trade:

                if signed_volume > 0:
                    state["large_buy"] += notional
                else:
                    state["large_sell"] += notional

            state["feed_status"] = "CONNECTED"
            state["last_update"] = now_ts()

    except Exception:
        pass


def process_book(message):

    try:

        bid = safe_float(message.get("b"))
        ask = safe_float(message.get("a"))

        if bid is None or ask is None:
            return

        if bid <= 0 or ask <= 0:
            return

        spread = ask - bid

        # Binance bookTicker doesn't provide depth,
        # so use the best-price relationship as a
        # lightweight microstructure signal.
        mid = (bid + ask) / 2

        with state_lock:

            state["bid"] = bid
            state["ask"] = ask
            state["spread"] = spread

            if mid > 0:
                state["microprice"] = mid

            state["feed_status"] = "CONNECTED"

    except Exception:
        pass


def binance_ws_worker():

    streams = (
        "btcusdt@aggTrade/"
        "btcusdt@bookTicker"
    )

    url = (
        "wss://stream.binance.com:9443/stream"
        "?streams=" + streams
    )

    while True:

        try:

            def on_message(ws, message):

                try:

                    import json

                    data = json.loads(message)

                    payload = data.get(
                        "data",
                        {}
                    )

                    event_type = payload.get("e")

                    if event_type == "aggTrade":
                        process_trade(payload)

                    elif event_type == "bookTicker":
                        process_book(payload)

                except Exception:
                    pass

            def on_error(ws, error):

                with state_lock:
                    state["feed_status"] = "RECONNECTING"

            def on_close(ws, close_status_code, close_msg):

                with state_lock:
                    state["feed_status"] = "RECONNECTING"

            ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:

            with state_lock:
                state["feed_status"] = "RECONNECTING"

        time.sleep(3)


# ============================================================
# AI Decision Engine
# ============================================================

def calculate_decision():

    with state_lock:

        btc = state["btc"]
        strike = state["strike"]

        seconds_left = state["seconds_left"]

        trend1 = state["trend_1m"]
        trend5 = state["trend_5m"]
        trend15 = state["trend_15m"]

        mom1 = state["momentum_1m"]
        mom5 = state["momentum_5m"]
        mom15 = state["momentum_15m"]

        rsi = state["rsi_1m"]

        delta = state["delta"]
        cvd = state["cvd"]

        probability = state["kalshi_probability"]

        large_buy = state["large_buy"]
        large_sell = state["large_sell"]

        spread = state["spread"]

        accuracy = state["accuracy"]

    if btc is None or strike is None:

        with state_lock:
            state["verdict"] = "WAITING"
            state["confidence"] = 0
            state["score"] = 0
            state["reason"] = "Waiting for BTC and Kalshi strike"

        return

    # --------------------------------------------------------
    # Distance from strike
    # --------------------------------------------------------

    distance = btc - strike

    distance_pct = (
        distance / strike * 100
        if strike
        else 0
    )

    score = 0
    reasons = []

    # --------------------------------------------------------
    # Strike position
    # --------------------------------------------------------

    if distance_pct > 0.08:
        score += 18
        reasons.append("BTC above strike")

    elif distance_pct > 0.025:
        score += 8
        reasons.append("BTC slightly above strike")

    elif distance_pct < -0.08:
        score -= 18
        reasons.append("BTC below strike")

    elif distance_pct < -0.025:
        score -= 8
        reasons.append("BTC slightly below strike")

    else:
        reasons.append("BTC near strike")

    # --------------------------------------------------------
    # Multi-timeframe trend
    # --------------------------------------------------------

    trend_score = 0

    for trend, weight in [
        (trend1, 12),
        (trend5, 14),
        (trend15, 16)
    ]:

        if trend == "UP":
            trend_score += weight

        elif trend == "DOWN":
            trend_score -= weight

    score += trend_score

    # --------------------------------------------------------
    # Momentum
    # --------------------------------------------------------

    score += clamp(mom1 * 7, -10, 10)
    score += clamp(mom5 * 5, -8, 8)
    score += clamp(mom15 * 3, -6, 6)

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if rsi is not None:

        if rsi >= 55 and rsi <= 70:
            score += 8

        elif rsi >= 70:
            score += 2

        elif rsi <= 45 and rsi >= 30:
            score -= 8

        elif rsi < 30:
            score -= 2

    # --------------------------------------------------------
    # Delta
    # --------------------------------------------------------

    delta_scale = clamp(delta / 2.0, -15, 15)

    score += delta_scale

    if delta > 0:
        reasons.append("buying pressure")

    elif delta < 0:
        reasons.append("selling pressure")

    # --------------------------------------------------------
    # CVD
    # --------------------------------------------------------

    cvd_scale = clamp(cvd / 5.0, -12, 12)

    score += cvd_scale

    # --------------------------------------------------------
    # Large trades
    # --------------------------------------------------------

    large_net = large_buy - large_sell

    large_scale = clamp(
        large_net / 100000,
        -10,
        10
    )

    score += large_scale

    # --------------------------------------------------------
    # Kalshi probability
    # --------------------------------------------------------

    if probability is not None:

        if probability >= 65:
            score += 10
            reasons.append("Kalshi favors UP")

        elif probability >= 55:
            score += 4

        elif probability <= 35:
            score -= 10
            reasons.append("Kalshi favors DOWN")

        elif probability <= 45:
            score -= 4

    # --------------------------------------------------------
    # Trend agreement
    # --------------------------------------------------------

    up_count = sum(
        1 for x in [
            trend1,
            trend5,
            trend15
        ]
        if x == "UP"
    )

    down_count = sum(
        1 for x in [
            trend1,
            trend5,
            trend15
        ]
        if x == "DOWN"
    )

    if up_count == 3:
        score += 12
        reasons.append("all timeframes UP")

    elif down_count == 3:
        score -= 12
        reasons.append("all timeframes DOWN")

    # --------------------------------------------------------
    # Conflict detection
    # --------------------------------------------------------

    conflict = False

    if trend1 == "UP" and trend5 == "DOWN":
        conflict = True

    if trend1 == "DOWN" and trend5 == "UP":
        conflict = True

    if delta > 0 and cvd < 0:
        conflict = True

    if delta < 0 and cvd > 0:
        conflict = True

    if conflict:
        score *= 0.65
        reasons.append("signal conflict")

    # --------------------------------------------------------
    # Time adaptation
    # --------------------------------------------------------

    if seconds_left is None:
        seconds_left = 900

    if seconds_left > 600:

        phase = "EARLY"

        # Early window:
        # trend matters more than strike noise.
        score *= 0.90

    elif seconds_left > 300:

        phase = "MIDDLE"

        score *= 1.00

    elif seconds_left > 60:

        phase = "LATE"

        score *= 1.12

    else:

        phase = "FINAL MINUTE"

        # Final minute requires stronger confirmation.
        score *= 1.20

    # --------------------------------------------------------
    # Spread quality
    # --------------------------------------------------------

    if spread is not None and btc:

        spread_pct = (
            spread / btc * 100
        )

        if spread_pct > 0.02:
            score *= 0.80
            reasons.append("wide spread")

    # --------------------------------------------------------
    # Adaptive model
    # --------------------------------------------------------

    # Small adjustment only.
    # Prevents early predictions from becoming
    # wildly amplified.
    adaptive_multiplier = (
        0.90 + (accuracy / 100) * 0.20
    )

    score *= adaptive_multiplier

    score = clamp(score, -100, 100)

    # --------------------------------------------------------
    # Verdict
    # --------------------------------------------------------

    if phase == "FINAL MINUTE":

        threshold = 40

    elif phase == "LATE":

        threshold = 32

    else:

        threshold = 28

    verdict = "WAITING"

    if score >= threshold:
        verdict = "UP"

    elif score <= -threshold:
        verdict = "DOWN"

    # --------------------------------------------------------
    # Confidence
    # --------------------------------------------------------

    confidence = abs(score)

    if conflict:
        confidence -= 10

    if abs(distance_pct) < 0.015:
        confidence -= 10

    if probability is None:
        confidence -= 5

    confidence = clamp(
        confidence,
        0,
        95
    )

    # Very close to strike:
    # do not pretend certainty.
    if abs(distance_pct) < 0.01:
        verdict = "WAITING"
        confidence = min(
            confidence,
            45
        )

    # If the system has no meaningful edge,
    # remain neutral.
    if confidence < 35:
        verdict = "WAITING"

    # --------------------------------------------------------
    # Explanation
    # --------------------------------------------------------

    if verdict == "UP":
        reason = " • ".join(reasons[:5])

    elif verdict == "DOWN":
        reason = " • ".join(reasons[:5])

    else:
        reason = " • ".join(reasons[:5])

        if not reason:
            reason = "Signals are not aligned"

    with state_lock:

        state["score"] = round(score, 1)
        state["confidence"] = round(confidence, 1)
        state["verdict"] = verdict

        state["phase"] = phase

        state["btc_vs_strike"] = distance
        state["distance_pct"] = distance_pct

        state["reason"] = reason


# ============================================================
# Main background loop
# ============================================================

def engine_loop():

    last_technical_refresh = 0
    last_kalshi_refresh = 0

    while True:

        current = now_ts()

        try:

            # Kalshi refresh every 3 seconds
            if current - last_kalshi_refresh >= 3:

                refresh_kalshi()

                last_kalshi_refresh = current

            # Technicals every 10 seconds
            if current - last_technical_refresh >= 10:

                refresh_technicals()

                last_technical_refresh = current

            calculate_decision()

        except Exception as exc:

            with state_lock:
                state["error"] = str(exc)

        time.sleep(1)


# ============================================================
# API
# ============================================================

@app.route("/")
def index():

    return render_template_string(HTML)


@app.route("/api/state")
def api_state():

    with state_lock:

        output = dict(state)

    # Convert timestamps to readable values.
    if output["market_close"]:

        try:
            output["market_close_iso"] = (
                datetime.fromtimestamp(
                    output["market_close"],
                    tz=timezone.utc
                ).isoformat()
            )

        except Exception:
            output["market_close_iso"] = None

    # Calculate countdown fresh.
    if output["market_close"]:

        remaining = (
            output["market_close"] - now_ts()
        )

        output["seconds_left"] = max(
            0,
            remaining
        )

    return jsonify(output)


# ============================================================
# Dashboard
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html>
<head>

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>BTC Strike AI</title>

<style>

* {
    box-sizing: border-box;
}

body {
    margin: 0;
    background: #070b12;
    color: #ffffff;
    font-family: Arial, sans-serif;
}

.header {
    padding: 18px;
    background: #0d131d;
    border-bottom: 1px solid #202938;
}

.title {
    font-size: 25px;
    font-weight: bold;
}

.subtitle {
    color: #7f8da3;
    margin-top: 5px;
    font-size: 13px;
}

.container {
    padding: 14px;
    max-width: 900px;
    margin: auto;
}

.card {
    background: #101722;
    border: 1px solid #202938;
    border-radius: 16px;
    padding: 15px;
    margin-bottom: 12px;
}

.label {
    color: #77869d;
    font-size: 12px;
    text-transform: uppercase;
}

.value {
    font-size: 25px;
    font-weight: bold;
    margin-top: 5px;
}

.big {
    font-size: 46px;
    font-weight: bold;
}

.verdict {
    text-align: center;
    padding: 22px;
    border-radius: 18px;
    background: #111a27;
    border: 1px solid #2a374a;
}

#verdict {
    font-size: 42px;
    font-weight: bold;
}

#confidence {
    margin-top: 7px;
    color: #a9b6c9;
}

.grid {
    display: grid;
    grid-template-columns:
        repeat(2, minmax(0, 1fr));
    gap: 10px;
}

.row {
    display: flex;
    justify-content: space-between;
    padding: 9px 0;
    border-bottom: 1px solid #1d2634;
}

.green {
    color: #4ade80;
}

.red {
    color: #fb7185;
}

.yellow {
    color: #facc15;
}

.gray {
    color: #94a3b8;
}

.small {
    font-size: 12px;
    color: #718096;
}

.bar {
    height: 10px;
    background: #1c2635;
    border-radius: 10px;
    overflow: hidden;
    margin-top: 10px;
}

.bar-inner {
    height: 100%;
    width: 50%;
    background: #60a5fa;
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

    <div class="verdict">

        <div class="label">
            MODEL VERDICT
        </div>

        <div id="verdict">
            WAITING
        </div>

        <div id="confidence">
            Confidence: 0%
        </div>

        <div class="small" id="reason">
            Waiting for live data...
        </div>

    </div>


    <div class="grid">

        <div class="card">

            <div class="label">
                BTC PRICE
            </div>

            <div class="big" id="btc">
                —
            </div>

        </div>

        <div class="card">

            <div class="label">
                COUNTDOWN
            </div>

            <div class="big" id="countdown">
                —
            </div>

            <div class="small" id="phase">
                —
            </div>

        </div>

    </div>


    <div class="card">

        <div class="label">
            KALSHI STRIKE
        </div>

        <div class="value" id="strike">
            —
        </div>

        <div class="row">
            <span>BTC vs Strike</span>
            <strong id="difference">—</strong>
        </div>

        <div class="row">
            <span>Distance %</span>
            <strong id="distance">—</strong>
        </div>

        <div class="row">
            <span>Kalshi UP Probability</span>
            <strong id="probability">—</strong>
        </div>

    </div>


    <div class="card">

        <div class="label">
            MULTI-TIMEFRAME TREND
        </div>

        <div class="row">
            <span>1 Minute</span>
            <strong id="trend1">—</strong>
        </div>

        <div class="row">
            <span>5 Minute</span>
            <strong id="trend5">—</strong>
        </div>

        <div class="row">
            <span>15 Minute</span>
            <strong id="trend15">—</strong>
        </div>

    </div>


    <div class="card">

        <div class="label">
            MOMENTUM
        </div>

        <div class="row">
            <span>1m Momentum</span>
            <strong id="mom1">—</strong>
        </div>

        <div class="row">
            <span>5m Momentum</span>
            <strong id="mom5">—</strong>
        </div>

        <div class="row">
            <span>15m Momentum</span>
            <strong id="mom15">—</strong>
        </div>

        <div class="row">
            <span>RSI 1m</span>
            <strong id="rsi">—</strong>
        </div>

    </div>


    <div class="card">

        <div class="label">
            ORDER FLOW
        </div>

        <div class="row">
            <span>Delta</span>
            <strong id="delta">0</strong>
        </div>

        <div class="row">
            <span>CVD</span>
            <strong id="cvd">0</strong>
        </div>

        <div class="row">
            <span>Large Buys</span>
            <strong id="largeBuy">—</strong>
        </div>

        <div class="row">
            <span>Large Sells</span>
            <strong id="largeSell">—</strong>
        </div>

        <div class="row">
            <span>Bid</span>
            <strong id="bid">—</strong>
        </div>

        <div class="row">
            <span>Ask</span>
            <strong id="ask">—</strong>
        </div>

        <div class="row">
            <span>Spread</span>
            <strong id="spread">—</strong>
        </div>

    </div>


    <div class="card">

        <div class="label">
            MODEL SCORE
        </div>

        <div class="value" id="score">
            0
        </div>

        <div class="bar">
            <div
                class="bar-inner"
                id="scorebar">
            </div>
        </div>

        <div class="small">
            -100 = strong DOWN
            &nbsp;&nbsp; 0 = neutral
            &nbsp;&nbsp; +100 = strong UP
        </div>

    </div>


    <div class="card">

        <div class="label">
            SYSTEM STATUS
        </div>

        <div class="row">
            <span>BTC Feed</span>
            <strong id="feed">—</strong>
        </div>

        <div class="row">
            <span>Kalshi</span>
            <strong id="kalshi">—</strong>
        </div>

        <div class="row">
            <span>Market</span>
            <strong id="market">—</strong>
        </div>

        <div class="row">
            <span>Adaptive Accuracy</span>
            <strong id="accuracy">—</strong>
        </div>

    </div>


    <div class="small">
        BTC market data is a fast proxy.
        Kalshi's official BTC 15-minute settlement
        uses its designated settlement index.
        This dashboard provides decision support,
        not guaranteed outcomes.
    </div>

</div>


<script>

function money(value) {

    if (value === null ||
        value === undefined) {

        return "—";
    }

    return "$" +
        Number(value).toLocaleString(
            undefined,
            {
                maximumFractionDigits: 2
            }
        );
}


function number(value) {

    if (value === null ||
        value === undefined) {

        return "—";
    }

    return Number(value).toLocaleString(
        undefined,
        {
            maximumFractionDigits: 4
        }
    );
}


function trendClass(value) {

    if (value === "UP") {
        return "green";
    }

    if (value === "DOWN") {
        return "red";
    }

    return "yellow";
}


function setTrend(id, value) {

    const element =
        document.getElementById(id);

    element.textContent =
        value || "—";

    element.className =
        trendClass(value);
}


function formatCountdown(seconds) {

    if (seconds === null ||
        seconds === undefined) {

        return "—";
    }

    seconds = Math.max(
        0,
        Math.floor(seconds)
    );

    const minutes =
        Math.floor(seconds / 60);

    const secs =
        seconds % 60;

    return String(minutes).padStart(2, "0")
        + ":"
        + String(secs).padStart(2, "0");
}


async function update() {

    try {

        const response =
            await fetch(
                "/api/state",
                {
                    cache: "no-store"
                }
            );

        const data =
            await response.json();


        document.getElementById("btc")
            .textContent =
            money(data.btc);


        document.getElementById("strike")
            .textContent =
            money(data.strike);


        document.getElementById("countdown")
            .textContent =
            formatCountdown(
                data.seconds_left
            );


        document.getElementById("phase")
            .textContent =
            data.phase || "—";


        document.getElementById("difference")
            .textContent =
            money(data.btc_vs_strike);


        document.getElementById("distance")
            .textContent =
            data.distance_pct !== null
                ? Number(
                    data.distance_pct
                  ).toFixed(4) + "%"
                : "—";


        document.getElementById("probability")
            .textContent =
            data.kalshi_probability !== null
                ? Number(
                    data.kalshi_probability
                  ).toFixed(1) + "%"
                : "—";


        setTrend(
            "trend1",
            data.trend_1m
        );

        setTrend(
            "trend5",
            data.trend_5m
        );

        setTrend(
            "trend15",
            data.trend_15m
        );


        document.getElementById("mom1")
            .textContent =
            Number(data.momentum_1m || 0)
            .toFixed(4) + "%";


        document.getElementById("mom5")
            .textContent =
            Number(data.momentum_5m || 0)
            .toFixed(4) + "%";


        document.getElementById("mom15")
            .textContent =
            Number(data.momentum_15m || 0)
            .toFixed(4) + "%";


        document.getElementById("rsi")
            .textContent =
            data.rsi_1m !== null
                ? Number(
                    data.rsi_1m
                  ).toFixed(1)
                : "—";


        document.getElementById("delta")
            .textContent =
            number(data.delta);


        document.getElementById("cvd")
            .textContent =
            number(data.cvd);


        document.getElementById("largeBuy")
            .textContent =
            money(data.large_buy);


        document.getElementById("largeSell")
            .textContent =
            money(data.large_sell);


        document.getElementById("bid")
            .textContent =
            money(data.bid);


        document.getElementById("ask")
            .textContent =
            money(data.ask);


        document.getElementById("spread")
            .textContent =
            data.spread !== null
                ? Number(
                    data.spread
                  ).toFixed(2)
                : "—";


        const verdict =
            document.getElementById("verdict");

        verdict.textContent =
            data.verdict || "WAITING";


        if (data.verdict === "UP") {

            verdict.className =
                "green";

        } else if (
            data.verdict === "DOWN"
        ) {

            verdict.className =
                "red";

        } else {

            verdict.className =
                "yellow";
        }


        document.getElementById(
            "confidence"
        ).textContent =
            "Confidence: "
            + Number(
                data.confidence || 0
            ).toFixed(0)
            + "%";


        document.getElementById("reason")
            .textContent =
            data.reason ||
            "Waiting for confirmation";


        document.getElementById("score")
            .textContent =
            Number(
                data.score || 0
            ).toFixed(1);


        const bar =
            document.getElementById(
                "scorebar"
            );

        const percentage =
            (
                Number(data.score || 0) + 100
            ) / 2;

        bar.style.width =
            Math.max(
                0,
                Math.min(
                    100,
                    percentage
                )
            ) + "%";


        document.getElementById("feed")
            .textContent =
            data.feed_status || "—";


        document.getElementById("kalshi")
            .textContent =
            data.kalshi_status || "—";


        document.getElementById("market")
            .textContent =
            data.market || "—";


        document.getElementById("accuracy")
            .textContent =
            Number(
                data.accuracy || 0
            ).toFixed(1) + "%";


    } catch (error) {

        document.getElementById("feed")
            .textContent =
            "ERROR";

    }
}


update();

setInterval(
    update,
    1000
);

</script>

</body>
</html>
"""


# ============================================================
# Start background workers
# ============================================================

def start_workers():

    threading.Thread(
        target=binance_ws_worker,
        daemon=True
    ).start()

    threading.Thread(
        target=engine_loop,
        daemon=True
    ).start()


start_workers()


# ============================================================
# Local development
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
