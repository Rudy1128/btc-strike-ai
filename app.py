# BTC STRIKE AI — SIMPLE POWERFUL BUILD
# Live BTC + momentum + Kalshi strike confirmation

import os
import time
import threading
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

PORT = int(os.getenv("PORT", "10000"))

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
).rstrip("/")

KALSHI_TICKER = os.getenv("KALSHI_TICKER", "").strip()

TIMEOUT = 5
UPDATE_SECONDS = 2

history = deque(maxlen=1200)
lock = threading.Lock()

STATE = {
    "btc": None,
    "coinbase": None,
    "kalshi": {},
    "m1": None,
    "m5": None,
    "m15": None,
    "trend": "WAIT",
    "score": 50,
    "reason": "Waiting for live data...",
    "status": "STARTING",
    "last_update": None,
    "error": None,
}


# ============================================================
# BASIC HELPERS
# ============================================================

def get_json(url):
    response = requests.get(
        url,
        timeout=TIMEOUT,
        headers={
            "User-Agent": "BTC-Strike-Simple/1.0"
        },
    )

    response.raise_for_status()

    return response.json()


def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ============================================================
# BTC PRICE
# ============================================================

def btc_price():

    data = get_json(
        "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT"
    )

    bid = number(
        data.get("bidPrice")
    )

    ask = number(
        data.get("askPrice")
    )

    if bid is None or ask is None:
        raise RuntimeError(
            "Binance returned no BTC price."
        )

    return (bid + ask) / 2


def coinbase_price():

    data = get_json(
        "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    )

    bid = number(
        data.get("bid")
    )

    ask = number(
        data.get("ask")
    )

    if bid is None or ask is None:
        return None

    return (bid + ask) / 2


# ============================================================
# KALSHI
# ============================================================

def normalize_market(market):

    if not isinstance(
        market,
        dict
    ):
        return {}

    strike = None

    for key in (
        "floor_strike",
        "strike_price",
        "strike",
        "target",
        "cap_strike",
    ):

        value = number(
            market.get(key)
        )

        if value is not None:

            strike = value

            break

    return {

        "ticker":
            market.get("ticker"),

        "title":
            market.get("title"),

        "strike":
            strike,

        "yes_bid":
            number(
                market.get("yes_bid")
            ),

        "yes_ask":
            number(
                market.get("yes_ask")
            ),

        "last_price":
            number(
                market.get("last_price")
            ),

        "close_time":
            market.get("close_time"),
    }


def get_kalshi():

    try:

        # If you know the exact Kalshi ticker,
        # put it in the KALSHI_TICKER environment variable.

        if KALSHI_TICKER:

            data = get_json(
                f"{KALSHI_BASE}/markets/"
                f"{KALSHI_TICKER}"
            )

            market = data.get(
                "market",
                data
            )

            return normalize_market(
                market
            )

        # Otherwise search open markets.

        data = get_json(
            f"{KALSHI_BASE}/markets"
            "?status=open&limit=100"
        )

        markets = data.get(
            "markets",
            []
        )

        candidates = []

        for market in markets:

            text = " ".join(
                str(
                    market.get(
                        key,
                        ""
                    )
                )

                for key in (
                    "ticker",
                    "title",
                    "subtitle",
                    "event_ticker",
                )
            ).lower()

            if (
                "btc" in text
                or
                "bitcoin" in text
            ):

                candidates.append(
                    market
                )

        if not candidates:

            return {
                "error":
                    "No open BTC market found."
            }

        # Prefer something that looks like a
        # 15-minute market.

        candidates.sort(
            key=lambda market: (
                "15"
                not in str(
                    market.get(
                        "title",
                        ""
                    )
                ).lower()

                and

                "15"
                not in str(
                    market.get(
                        "ticker",
                        ""
                    )
                ).lower()
            )
        )

        return normalize_market(
            candidates[0]
        )

    except Exception as exc:

        return {
            "error": str(exc)
        }


# ============================================================
# MOMENTUM
# ============================================================

def pct(old, new):

    if (
        old is None
        or new is None
        or old == 0
    ):
        return 0.0

    return (
        (new - old)
        / old
    ) * 100.0


def momentum(seconds):

    cutoff = (
        time.time()
        - seconds
    )

    points = [
        (timestamp, price)
        for timestamp, price
        in history
        if timestamp >= cutoff
    ]

    if len(points) < 2:
        return 0.0

    return pct(
        points[0][1],
        points[-1][1]
    )


# ============================================================
# DECISION ENGINE
# ============================================================

def make_decision(
    price,
    market
):

    m1 = momentum(60)

    m5 = momentum(300)

    m15 = momentum(900)

    strike = (
        market.get("strike")
        if market
        else None
    )

    score = 50

    reasons = []

    # --------------------------------------------------------
    # 1-MINUTE
    # --------------------------------------------------------

    if m1 > 0.03:

        score += 15

        reasons.append(
            "1m momentum UP"
        )

    elif m1 < -0.03:

        score -= 15

        reasons.append(
            "1m momentum DOWN"
        )

    # --------------------------------------------------------
    # 5-MINUTE
    # --------------------------------------------------------

    if m5 > 0.05:

        score += 15

        reasons.append(
            "5m trend UP"
        )

    elif m5 < -0.05:

        score -= 15

        reasons.append(
            "5m trend DOWN"
        )

    # --------------------------------------------------------
    # 15-MINUTE
    # --------------------------------------------------------

    if m15 > 0.08:

        score += 15

        reasons.append(
            "15m trend UP"
        )

    elif m15 < -0.08:

        score -= 15

        reasons.append(
            "15m trend DOWN"
        )

    # --------------------------------------------------------
    # KALSHI STRIKE
    # --------------------------------------------------------

    if strike is not None:

        distance = pct(
            strike,
            price
        )

        if distance > 0:

            score += 10

            reasons.append(
                "BTC above strike"
            )

        elif distance < 0:

            score -= 10

            reasons.append(
                "BTC below strike"
            )

    # --------------------------------------------------------
    # AGREEMENT
    # --------------------------------------------------------

    bullish = sum(
        x > 0.03
        for x in (
            m1,
            m5,
            m15
        )
    )

    bearish = sum(
        x < -0.03
        for x in (
            m1,
            m5,
            m15
        )
    )

    score = max(
        0,
        min(
            100,
            int(score)
        )
    )

    # --------------------------------------------------------
    # FINAL DECISION
    # --------------------------------------------------------

    if (
        score >= 70
        and
        bullish >= 2
    ):

        verdict = "UP"

    elif (
        score <= 30
        and
        bearish >= 2
    ):

        verdict = "DOWN"

    else:

        verdict = "WAIT"

    if not reasons:

        reasons.append(
            "Waiting for enough movement."
        )

    return {

        "m1":
            m1,

        "m5":
            m5,

        "m15":
            m15,

        "score":
            score,

        "verdict":
            verdict,

        "reason":
            " • ".join(
                reasons
            ),
    }


# ============================================================
# LIVE DATA LOOP
# ============================================================

def update_loop():

    while True:

        try:

            # BTC primary feed
            price = btc_price()

            # Secondary feed
            try:

                coinbase = (
                    coinbase_price()
                )

            except Exception:

                coinbase = None

            with lock:

                STATE["btc"] = price

                STATE[
                    "coinbase"
                ] = coinbase

                history.append(
                    (
                        time.time(),
                        price
                    )
                )

            # Kalshi
            market = get_kalshi()

            # Decision
            with lock:

                result = (
                    make_decision(
                        price,
                        market
                    )
                )

                STATE[
                    "kalshi"
                ] = market

                STATE[
                    "m1"
                ] = result[
                    "m1"
                ]

                STATE[
                    "m5"
                ] = result[
                    "m5"
                ]

                STATE[
                    "m15"
                ] = result[
                    "m15"
                ]

                STATE[
                    "trend"
                ] = result[
                    "verdict"
                ]

                STATE[
                    "score"
                ] = result[
                    "score"
                ]

                STATE[
                    "reason"
                ] = result[
                    "reason"
                ]

                STATE[
                    "status"
                ] = "LIVE"

                STATE[
                    "error"
                ] = (
                    market.get("error")
                    if isinstance(
                        market,
                        dict
                    )
                    else None
                )

                STATE[
                    "last_update"
                ] = datetime.now(
                    timezone.utc
                ).isoformat()

        except Exception as exc:

            with lock:

                STATE[
                    "status"
                ] = "DATA ERROR"

                STATE[
                    "error"
                ] = str(exc)

        time.sleep(
            UPDATE_SECONDS
        )


# ============================================================
# API
# ============================================================

@app.get("/api/state")
def api_state():

    with lock:

        return jsonify(
            dict(STATE)
        )


# ============================================================
# DASHBOARD
# ============================================================

PAGE = r"""
<!doctype html>

<html>

<head>

<meta
    name="viewport"
    content="width=device-width,initial-scale=1"
>

<title>
BTC Strike AI
</title>

<style>

*{
    box-sizing:border-box;
}

body{

    margin:0;

    background:#070a10;

    color:#fff;

    font-family:
        Arial,
        sans-serif;
}

.wrap{

    max-width:900px;

    margin:auto;

    padding:16px;
}

h1{

    text-align:center;

    margin:
        5px
        0;

    font-size:26px;
}

.sub{

    text-align:center;

    color:#8b95a7;

    font-size:12px;

    margin-bottom:16px;
}

.card{

    background:#111722;

    border:
        1px solid
        #273142;

    border-radius:16px;

    padding:16px;

    margin-bottom:12px;
}

.verdict{

    text-align:center;

    padding:
        25px
        10px;

    border:
        2px solid
        #d1a82f;

    border-radius:15px;
}

.verdict.up{

    border-color:#16d995;

    background:
        rgba(
            22,
            217,
            149,
            .12
        );
}

.verdict.down{

    border-color:#ff4e59;

    background:
        rgba(
            255,
            78,
            89,
            .12
        );
}

.verdict.wait{

    border-color:#e0b63e;

    background:
        rgba(
            224,
            182,
            62,
            .12
        );
}

#decision{

    font-size:46px;

    font-weight:900;
}

.upText{

    color:#19e69b;
}

.downText{

    color:#ff5962;
}

.waitText{

    color:#f1c94d;
}

.conf{

    color:#aeb8c7;

    margin-top:8px;
}

.grid{

    display:grid;

    grid-template-columns:
        1fr 1fr;

    gap:12px;
}

.label{

    color:#7f8b9f;

    font-size:10px;

    font-weight:bold;

    text-transform:uppercase;

    letter-spacing:.1em;
}

.value{

    font-size:25px;

    font-weight:800;

    margin-top:7px;
}

.reason{

    color:#c7cfdb;

    font-size:13px;

    line-height:1.5;
}

.status{

    color:#8e99aa;

    font-size:11px;

    word-break:break-word;
}

@media(max-width:650px){

    .grid{

        grid-template-columns:
            1fr;
    }
}

</style>

</head>

<body>

<div class="wrap">

<h1>
₿ BTC STRIKE AI
</h1>

<div class="sub">
Simple live engine •
BTC momentum + Kalshi strike confirmation
</div>


<div
    id="box"
    class="card verdict wait"
>

<div class="label">
CURRENT DECISION
</div>

<div
    id="decision"
    class="waitText"
>
WAIT
</div>

<div
    id="confidence"
    class="conf"
>
Strength: --/100
</div>

</div>


<div class="grid">


<div class="card">

<div class="label">
BTC Price
</div>

<div
    id="btc"
    class="value"
>
--
</div>

</div>


<div class="card">

<div class="label">
Kalshi Strike
</div>

<div
    id="strike"
    class="value"
>
--
</div>

</div>


<div class="card">

<div class="label">
1 Minute
</div>

<div
    id="m1"
    class="value"
>
--
</div>

</div>


<div class="card">

<div class="label">
5 Minute
</div>

<div
    id="m5"
    class="value"
>
--
</div>

</div>


<div class="card">

<div class="label">
15 Minute
</div>

<div
    id="m15"
    class="value"
>
--
</div>

</div>


<div class="card">

<div class="label">
Signal Strength
</div>

<div
    id="score"
    class="value"
>
--/100
</div>

</div>


</div>


<div class="card">

<div class="label">
WHY?
</div>

<div
    id="reason"
    class="reason"
>
Waiting for live data...
</div>

</div>


<div class="card">

<div class="label">
SYSTEM STATUS
</div>

<div
    id="status"
    class="status"
>
Starting...
</div>

</div>


</div>


<script>

function money(v){

    if(
        v === null ||
        v === undefined ||
        isNaN(v)
    ){

        return "--";
    }

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


function percent(v){

    if(
        v === null ||
        v === undefined ||
        isNaN(v)
    ){

        return "--";
    }

    return (
        v >= 0
        ? "+"
        : ""
    )
    +
    Number(v).toFixed(3)
    +
    "%";
}


function setMomentum(
    id,
    value
){

    const element =
        document.getElementById(
            id
        );

    element.textContent =
        percent(value);

    element.className =
        "value "
        +
        (
            value > 0
            ? "upText"
            :
            value < 0
            ? "downText"
            :
            "waitText"
        );
}


async function refresh(){

    try{

        const response =
            await fetch(
                "/api/state?t="
                +
                Date.now(),
                {
                    cache:
                        "no-store"
                }
            );

        const state =
            await response.json();


        document
            .getElementById(
                "btc"
            )
            .textContent =
            money(
                state.btc
            );


        const market =
            state.kalshi
            || {};


        document
            .getElementById(
                "strike"
            )
            .textContent =
            money(
                market.strike
            );


        setMomentum(
            "m1",
            state.m1
        );

        setMomentum(
            "m5",
            state.m5
        );

        setMomentum(
            "m15",
            state.m15
        );


        document
            .getElementById(
                "score"
            )
            .textContent =
            (
                state.score
                ??
                "--"
            )
            +
            "/100";


        document
            .getElementById(
                "reason"
            )
            .textContent =
            state.reason
            ||
            "Waiting...";


        document
            .getElementById(
                "status"
            )
            .textContent =
            state.status
            +
            (
                state.error
                ?
                " — "
                +
                state.error
                :
                ""
            )
            +
            (
                state.last_update
                ?
                " — Updated "
                +
                new Date(
                    state.last_update
                ).toLocaleTimeString()
                :
                ""
            );


        const decision =
            (
                state.trend
                ||
                "WAIT"
            ).toUpperCase();


        const box =
            document.getElementById(
                "box"
            );

        const decisionElement =
            document.getElementById(
                "decision"
            );


        box.className =
            "card verdict "
            +
            (
                decision === "UP"
                ?
                "up"
                :
                decision === "DOWN"
                ?
                "down"
                :
                "wait"
            );


        decisionElement.className =
            decision === "UP"
            ?
            "upText"
            :
            decision === "DOWN"
            ?
            "downText"
            :
            "waitText";


        decisionElement.textContent =
            decision;


        document
            .getElementById(
                "confidence"
            )
            .textContent =
            "Signal strength: "
            +
            (
                state.score
                ??
                "--"
            )
            +
            "/100";

    }

    catch(error){

        document
            .getElementById(
                "status"
            )
            .textContent =
            "CONNECTION ERROR — "
            +
            error;
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


@app.route("/")
def index():

    return render_template_string(
        PAGE
    )


# ============================================================
# IMPORTANT FOR RENDER
# ============================================================
# Render uses:
#
#     gunicorn app:app
#
# Gunicorn imports this file instead of running it as
# "__main__", so the live data engine must start here.

_update_thread = threading.Thread(
    target=update_loop,
    daemon=True,
    name="btc-live-engine",
)

_update_thread.start()


if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True,
    )
