import os
import time
import statistics
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string
import requests


app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 2.0

BINANCE = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
COINBASE = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
KRAKEN = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
BITSTAMP = "https://www.bitstamp.net/api/v2/ticker/btcusd/"

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
)

session = requests.Session()
session.headers.update({
    "User-Agent": "BTC-Strike-AI/1.0"
})

cache = {
    "timestamp": 0.0,
    "state": None,
}

price_history = []


def safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def request_json(url, params=None):
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


# =========================================================
# BTC PRICE FEEDS
# =========================================================

def get_binance():
    data = request_json(BINANCE)

    if isinstance(data, dict):
        return safe_float(data.get("price"))

    return None


def get_coinbase():
    data = request_json(COINBASE)

    try:
        return safe_float(data["data"]["amount"])
    except (TypeError, KeyError):
        return None


def get_kraken():
    data = request_json(KRAKEN)

    try:
        result = data["result"]
        pair = next(iter(result))
        return safe_float(result[pair]["c"][0])
    except (
        TypeError,
        KeyError,
        StopIteration,
        IndexError
    ):
        return None


def get_bitstamp():
    data = request_json(BITSTAMP)

    if isinstance(data, dict):
        return safe_float(data.get("last"))

    return None


def get_reference_price():
    sources = {
        "Binance": get_binance(),
        "Coinbase": get_coinbase(),
        "Kraken": get_kraken(),
        "Bitstamp": get_bitstamp(),
    }

    valid = [
        value
        for value in sources.values()
        if value is not None and value > 0
    ]

    if not valid:
        return None, sources

    median = statistics.median(valid)

    # Remove obvious outliers.
    filtered = [
        value
        for value in valid
        if abs(value - median) / median <= 0.0035
    ]

    if not filtered:
        filtered = valid

    return statistics.median(filtered), sources


# =========================================================
# KALSHI
# =========================================================

def normalize_market(market):

    if not isinstance(market, dict):
        return None

    ticker = market.get("ticker")

    if not ticker:
        return None

    def cents_to_probability(value):

        value = safe_float(value)

        if value is None:
            return None

        # Kalshi commonly returns prices in cents.
        if value > 1:
            return value / 100.0

        return value

    yes_bid = cents_to_probability(
        market.get(
            "yes_bid",
            market.get("yes_bid_dollars")
        )
    )

    yes_ask = cents_to_probability(
        market.get(
            "yes_ask",
            market.get("yes_ask_dollars")
        )
    )

    no_bid = cents_to_probability(
        market.get(
            "no_bid",
            market.get("no_bid_dollars")
        )
    )

    no_ask = cents_to_probability(
        market.get(
            "no_ask",
            market.get("no_ask_dollars")
        )
    )

    last = cents_to_probability(
        market.get(
            "last_price",
            market.get("last_price_dollars")
        )
    )

    return {
        "ticker": ticker,
        "title": (
            market.get("title")
            or market.get("subtitle")
            or ticker
        ),
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "last": last,
        "close_time": (
            market.get("close_time")
            or market.get("expiration_time")
        ),
        "status": market.get("status"),
        "raw": market,
    }


def find_target_in_text(market):

    raw = market.get("raw", {})

    # Try direct strike fields.
    for key in (
        "floor_strike",
        "cap_strike",
        "strike",
        "target",
        "floor",
        "cap",
    ):

        value = safe_float(raw.get(key))

        if value is not None and value > 1000:
            return value

    # Try other fields containing strike/target.
    for key, value in raw.items():

        key_text = str(key).lower()

        if (
            "strike" in key_text
            or "target" in key_text
        ):

            number = safe_float(value)

            if number is not None and number > 1000:
                return number

    # Last fallback: look in title/subtitle.
    text = (
        str(market.get("title", ""))
        + " "
        + str(raw.get("subtitle", ""))
    )

    numbers = []

    for token in (
        text
        .replace(",", "")
        .replace("$", " ")
        .split()
    ):

        number = safe_float(token)

        if number is not None and 1000 < number < 1000000:
            numbers.append(number)

    return numbers[0] if numbers else None


def get_kalshi():

    manual = os.getenv(
        "KALSHI_TICKER",
        ""
    ).strip()

    # -----------------------------------------------------
    # Manual ticker if supplied
    # -----------------------------------------------------

    if manual:

        data = request_json(
            f"{KALSHI_BASE}/markets/{manual}"
        )

        if isinstance(data, dict):

            market = data.get(
                "market",
                data
            )

            normalized = normalize_market(market)

            if normalized:

                normalized["target"] = (
                    find_target_in_text(normalized)
                )

                return normalized

    # -----------------------------------------------------
    # Automatic discovery
    # -----------------------------------------------------

    data = request_json(
        f"{KALSHI_BASE}/markets",
        params={
            "status": "open",
            "limit": 100
        }
    )

    if not isinstance(data, dict):
        return None

    markets = data.get("markets", [])

    if not isinstance(markets, list):
        return None

    btc_markets = []

    for market in markets:

        text = (
            str(market.get("ticker", ""))
            + " "
            + str(market.get("title", ""))
            + " "
            + str(market.get("subtitle", ""))
        ).lower()

        if (
            "btc" in text
            or "bitcoin" in text
        ):
            btc_markets.append(market)

    # Prefer 15-minute BTC markets.
    fifteen = [
        market
        for market in btc_markets
        if "15" in (
            str(market.get("ticker", ""))
            + " "
            + str(market.get("title", ""))
            + " "
            + str(market.get("subtitle", ""))
        )
    ]

    candidates = fifteen or btc_markets

    if not candidates:
        return None

    def close_timestamp(market):

        value = (
            market.get("close_time")
            or market.get("expiration_time")
        )

        if not value:
            return float("inf")

        try:

            text = str(value).replace(
                "Z",
                "+00:00"
            )

            return datetime.fromisoformat(
                text
            ).timestamp()

        except Exception:

            return float("inf")

    # Nearest closing market first.
    candidates.sort(
        key=close_timestamp
    )

    normalized = normalize_market(
        candidates[0]
    )

    if normalized:

        normalized["target"] = (
            find_target_in_text(normalized)
        )

    return normalized


# =========================================================
# PRICE HISTORY
# =========================================================

def update_history(price):

    if price is None:
        return

    now = time.time()

    price_history.append(
        (now, price)
    )

    cutoff = now - (20 * 60)

    while (
        price_history
        and price_history[0][0] < cutoff
    ):
        price_history.pop(0)


def price_at_or_before(seconds_ago):

    if not price_history:
        return None

    target_time = (
        time.time() - seconds_ago
    )

    chosen = price_history[0][1]

    for timestamp, price in price_history:

        if timestamp <= target_time:
            chosen = price
        else:
            break

    return chosen


def momentum(seconds):

    if not price_history:
        return None

    current = price_history[-1][1]

    previous = price_at_or_before(
        seconds
    )

    if (
        current is None
        or previous in (None, 0)
    ):
        return None

    return (
        (current - previous)
        / previous
    ) * 100.0


def price_structure():

    if len(price_history) < 8:
        return "WAIT"

    recent = [
        price
        for _, price in price_history[-8:]
    ]

    first_half = recent[:4]
    second_half = recent[4:]

    high1 = max(first_half)
    low1 = min(first_half)

    high2 = max(second_half)
    low2 = min(second_half)

    if (
        high2 > high1
        and low2 > low1
    ):
        return "HIGHER HIGHS / HIGHER LOWS"

    if (
        high2 < high1
        and low2 < low1
    ):
        return "LOWER HIGHS / LOWER LOWS"

    return "MIXED"


# =========================================================
# KALSHI ORDER BOOK PRESSURE
# =========================================================

def kalshi_pressure(market):

    if not market:
        return None

    bid = market.get("yes_bid")
    ask = market.get("yes_ask")

    if (
        bid is None
        or ask is None
    ):
        return None

    midpoint = (
        bid + ask
    ) / 2

    if midpoint <= 0:
        return None

    return (
        (
            bid
            - (1 - ask)
        )
        / midpoint
    ) * 100.0


# =========================================================
# DECISION ENGINE
# =========================================================

def build_signal(
    price,
    market,
    m1,
    m5,
    m15
):

    target = (
        market.get("target")
        if market
        else None
    )

    structure_name = (
        price_structure()
    )

    score = 0.0
    reasons = []

    # -----------------------------------------------------
    # BTC vs Kalshi target
    # -----------------------------------------------------

    if (
        target is not None
        and price is not None
    ):

        distance = price - target

        if distance > 0:

            score += 2.0

            reasons.append(
                "BTC is above the Kalshi target"
            )

        elif distance < 0:

            score -= 2.0

            reasons.append(
                "BTC is below the Kalshi target"
            )

    # -----------------------------------------------------
    # Momentum
    # -----------------------------------------------------

    for value, weight, label in (
        (m1, 1.0, "1m"),
        (m5, 1.5, "5m"),
        (m15, 2.0, "15m"),
    ):

        if value is None:
            continue

        if value > 0.01:

            score += weight

            reasons.append(
                f"{label} momentum is positive"
            )

        elif value < -0.01:

            score -= weight

            reasons.append(
                f"{label} momentum is negative"
            )

    # -----------------------------------------------------
    # Price structure
    # -----------------------------------------------------

    if structure_name == (
        "HIGHER HIGHS / HIGHER LOWS"
    ):

        score += 1.5

        reasons.append(
            "Price structure is bullish"
        )

    elif structure_name == (
        "LOWER HIGHS / LOWER LOWS"
    ):

        score -= 1.5

        reasons.append(
            "Price structure is bearish"
        )

    # -----------------------------------------------------
    # Kalshi book
    # -----------------------------------------------------

    pressure = kalshi_pressure(
        market
    )

    if pressure is not None:

        if pressure > 5:

            score += 0.75

            reasons.append(
                "Kalshi book leans YES"
            )

        elif pressure < -5:

            score -= 0.75

            reasons.append(
                "Kalshi book leans NO"
            )

    # -----------------------------------------------------
    # Conservative verdict
    # -----------------------------------------------------

    if score >= 4.0:

        verdict = "UP"

    elif score <= -4.0:

        verdict = "DOWN"

    else:

        verdict = "WAIT"

    confidence = min(
        95,
        max(
            0,
            50 + abs(score) * 9
        )
    )

    return {
        "verdict": verdict,
        "score": round(score, 2),
        "confidence": round(confidence),
        "reasons": reasons[:5],
        "structure": structure_name,
        "pressure": (
            round(pressure, 2)
            if pressure is not None
            else None
        ),
    }


# =========================================================
# COUNTDOWN
# =========================================================

def countdown(close_time):

    if not close_time:
        return None

    try:

        text = str(
            close_time
        ).replace(
            "Z",
            "+00:00"
        )

        close = datetime.fromisoformat(
            text
        )

        if close.tzinfo is None:

            close = close.replace(
                tzinfo=timezone.utc
            )

        seconds = max(
            0,
            int(
                close.timestamp()
                - time.time()
            )
        )

        return seconds

    except Exception:

        return None


# =========================================================
# BUILD STATE
# =========================================================

def collect_state():

    price, feeds = (
        get_reference_price()
    )

    update_history(price)

    market = get_kalshi()

    m1 = momentum(60)
    m5 = momentum(300)
    m15 = momentum(900)

    signal = build_signal(
        price,
        market,
        m1,
        m5,
        m15
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

        if target != 0:

            distance_pct = (
                distance
                / target
            ) * 100

    live_feed_count = sum(
        1
        for value in feeds.values()
        if value is not None
    )

    return {
        "ok": price is not None,

        "updated": datetime.now(
            timezone.utc
        ).isoformat(),

        "btc": (
            round(price, 2)
            if price is not None
            else None
        ),

        "feeds": {
            name: (
                round(value, 2)
                if value is not None
                else None
            )
            for name, value
            in feeds.items()
        },

        "feed_count": live_feed_count,

        "kalshi": {
            "ticker": (
                market.get("ticker")
                if market
                else None
            ),

            "title": (
                market.get("title")
                if market
                else None
            ),

            "target": (
                round(target, 2)
                if target
                else None
            ),

            "yes_bid": (
                market.get("yes_bid")
                if market
                else None
            ),

            "yes_ask": (
                market.get("yes_ask")
                if market
                else None
            ),

            "no_bid": (
                market.get("no_bid")
                if market
                else None
            ),

            "no_ask": (
                market.get("no_ask")
                if market
                else None
            ),

            "last": (
                market.get("last")
                if market
                else None
            ),

            "close_time": (
                market.get("close_time")
                if market
                else None
            ),

            "status": (
                market.get("status")
                if market
                else None
            ),

            "countdown": (
                countdown(
                    market.get("close_time")
                )
                if market
                else None
            ),
        },

        "distance": {
            "dollars": (
                round(distance, 2)
                if distance is not None
                else None
            ),

            "percent": (
                round(distance_pct, 4)
                if distance_pct is not None
                else None
            ),
        },

        "momentum": {
            "m1": (
                round(m1, 4)
                if m1 is not None
                else None
            ),

            "m5": (
                round(m5, 4)
                if m5 is not None
                else None
            ),

            "m15": (
                round(m15, 4)
                if m15 is not None
                else None
            ),
        },

        "signal": signal,

        "history_points": len(
            price_history
        ),
    }


# =========================================================
# WEB ROUTES
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
        and now - cache["timestamp"]
        < CACHE_SECONDS
    ):

        return jsonify(
            cache["state"]
        )

    state = collect_state()

    cache["state"] = state
    cache["timestamp"] = now

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

.bar{
 height:8px;
 background:#202833;
 border-radius:20px;
 overflow:hidden;
 margin-top:8px;
}

.bar i{
 display:block;
 height:100%;
 width:0;
 background:#20d879;
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
   WAIT
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

   <h3>Book Pressure</h3>

   <div
    class="value"
    id="pressure"
   >
    --
   </div>

   <div class="bar">

    <i id="pressureBar"></i>

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

  </div>


 </div>

</div>


<script>

function money(v){

 if(v == null)
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

 if(v == null)
  return "--";

 return (
  v >= 0 ? "+" : ""
 ) +
 Number(v).toFixed(3) +
 "%";

}


function cents(v){

 if(v == null)
  return "--";

 return (
  Number(v) * 100
 ).toFixed(1) + "¢";

}


function colorClass(v){

 if(v == null)
  return "";

 if(v > 0)
  return "green";

 if(v < 0)
  return "red";

 return "yellow";

}


function setValue(
 id,
 text,
 cls
){

 const e =
  document.getElementById(id);

 e.textContent = text;

 e.className =
  "value " + (cls || "");

}


function clock(seconds){

 if(seconds == null)
  return "--";

 seconds =
  Math.max(
   0,
   Math.floor(seconds)
  );

 const m =
  Math.floor(seconds / 60);

 const s =
  seconds % 60;

 return (
  String(m).padStart(2,"0")
  + ":"
  + String(s).padStart(2,"0")
 );

}


function refresh(){

 fetch(
  "/api/state?t=" +
  Date.now(),
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
    money(d.kalshi.target);


   document.getElementById(
    "ticker"
   ).textContent =
    d.kalshi.ticker || "--";


   document.getElementById(
    "feedCount"
   ).textContent =
    (d.feed_count || 0)
    + " feeds live";


   const distance =
    document.getElementById(
     "distance"
    );

   distance.textContent =
    money(d.distance.dollars);

   distance.className =
    "big " +
    colorClass(
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
    pct(d.momentum.m1),
    colorClass(d.momentum.m1)
   );


   setValue(
    "m5",
    pct(d.momentum.m5),
    colorClass(d.momentum.m5)
   );


   setValue(
    "m15",
    pct(d.momentum.m15),
    colorClass(d.momentum.m15)
   );


   setValue(
    "structure",
    d.signal.structure || "--",
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


   const pressure =
    d.signal.pressure;


   setValue(
    "pressure",

    pressure == null
     ? "--"
     :
     (
      pressure >= 0
       ? "+"
       : ""
     )
     +
     Number(pressure).toFixed(1)
     +
     "%",

    colorClass(pressure)
   );


   const bar =
    document.getElementById(
     "pressureBar"
    );


   const width =
    Math.min(
     100,
     Math.abs(pressure || 0) * 2
    );


   bar.style.width =
    width + "%";


   if(
    pressure != null
    && pressure < 0
   ){

    bar.style.marginLeft =
     (100 - width) + "%";

   }else{

    bar.style.marginLeft =
     "0";

   }


   setValue(
    "score",

    d.signal.score == null
     ? "--"
     : d.signal.score.toFixed(2),

    colorClass(
     d.signal.score
    )
   );


   const v =
    d.signal.verdict || "WAIT";


   const box =
    document.getElementById(
     "verdict"
    );


   box.className =
    "verdict " +
    (
     v === "UP"
      ? "up"
      : v === "DOWN"
       ? "down"
       : "wait"
    );


   document.getElementById(
    "verdictLabel"
   ).textContent =
    v === "UP"
     ? "🟢 UP"
     : v === "DOWN"
      ? "🔴 DOWN"
      : "🟡 WAIT";


   document.getElementById(
    "confidence"
   ).textContent =
    (d.signal.confidence || 0)
    + "%";


   const ul =
    document.getElementById(
     "reasons"
    );


   ul.innerHTML = "";


   (
    d.signal.reasons ||
    [
     "Waiting for stronger alignment..."
    ]
   ).forEach(
    x => {

     const li =
      document.createElement(
       "li"
      );

     li.textContent = x;

     ul.appendChild(li);

    }
   );


   document.getElementById(
    "feeds"
   ).textContent =
    Object.entries(
     d.feeds || {}
    )
    .map(
     ([k,v]) =>
      k +
      ": " +
      (
       v == null
        ? "OFFLINE"
        : money(v)
      )
    )
    .join("  •  ");

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
        threaded=True
    )
