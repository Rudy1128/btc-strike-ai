import time
from datetime import datetime
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# Public APIs - no keys required
KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
BINANCE_BASE = "https://data-api.binance.vision"
COINBASE_BASE = "https://api.exchange.coinbase.com"

TIMEOUT = 8
SERIES = "KXBTC15M"


def get_json(url, params=None):
    r = requests.get(
        url,
        params=params,
        timeout=TIMEOUT,
        headers={"User-Agent": "BTC-Strike-AI/1.0"},
    )
    r.raise_for_status()
    return r.json()


def parse_time(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except Exception:
        pass

    try:
        return datetime.fromisoformat(
            text.replace("Z", "+00:00")
        ).timestamp()
    except Exception:
        return None


def number(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def btc_from_binance():
    data = get_json(
        BINANCE_BASE + "/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": "1m", "limit": 16},
    )

    closes = [float(row[4]) for row in data]
    if len(closes) < 16:
        raise RuntimeError("Binance returned fewer than 16 candles")

    price = closes[-1]

    def move(n):
        old = closes[-1 - n]
        return (price - old) / old * 100.0

    return {
        "source": "Binance",
        "price": price,
        "m1": move(1),
        "m5": move(5),
        "m15": move(15),
    }


def btc_from_coinbase():
    data = get_json(
        COINBASE_BASE + "/products/BTC-USD/candles",
        {"granularity": 60},
    )

    # Coinbase rows are [time, low, high, open, close, volume]
    rows = sorted(data, key=lambda x: x[0])
    closes = [float(row[4]) for row in rows[-16:]]

    if len(closes) < 16:
        raise RuntimeError("Coinbase returned fewer than 16 candles")

    price = closes[-1]

    def move(n):
        old = closes[-1 - n]
        return (price - old) / old * 100.0

    return {
        "source": "Coinbase",
        "price": price,
        "m1": move(1),
        "m5": move(5),
        "m15": move(15),
    }


def get_btc():
    try:
        return btc_from_binance()
    except Exception as first_error:
        try:
            return btc_from_coinbase()
        except Exception as second_error:
            raise RuntimeError(
                "BTC feed failed. Binance: "
                + str(first_error)
                + " | Coinbase: "
                + str(second_error)
            )


def get_kalshi_market():
    # Kalshi's current public API supports series_ticker + status=open.
    data = get_json(
        KALSHI_BASE + "/markets",
        {
            "series_ticker": SERIES,
            "status": "open",
            "limit": 1000,
        },
    )

    markets = data.get("markets", [])
    if not markets:
        raise RuntimeError("No open KXBTC15M market was returned by Kalshi")

    now = time.time()
    future = []

    for market in markets:
        close_ts = parse_time(
            market.get("close_time")
            or market.get("expiration_time")
            or market.get("expected_expiration_time")
        )

        if close_ts is not None and close_ts > now:
            future.append((close_ts, market))

    if not future:
        raise RuntimeError(
            "Kalshi returned KXBTC15M markets, but none has a future close time"
        )

    close_ts, market = min(future, key=lambda item: item[0])

    strike = number(market.get("floor_strike"))

    if strike is None:
        for key in (
            "strike_price",
            "target_price",
            "functional_strike",
            "cap_strike",
        ):
            strike = number(market.get(key))
            if strike is not None:
                break

    if strike is None:
        raise RuntimeError("Kalshi market has no usable BTC target/strike")

    yes_bid = number(market.get("yes_bid_dollars"))
    yes_ask = number(market.get("yes_ask_dollars"))
    last = number(market.get("last_price_dollars"))

    # Older field names are accepted as a fallback.
    if yes_bid is None:
        yes_bid = number(market.get("yes_bid"))
        if yes_bid is not None and yes_bid > 1:
            yes_bid /= 100.0

    if yes_ask is None:
        yes_ask = number(market.get("yes_ask"))
        if yes_ask is not None and yes_ask > 1:
            yes_ask /= 100.0

    if last is None:
        last = number(market.get("last_price"))
        if last is not None and last > 1:
            last /= 100.0

    probability = None
    if yes_bid is not None and yes_ask is not None:
        probability = (yes_bid + yes_ask) / 2.0
    elif last is not None:
        probability = last

    return {
        "ticker": market.get("ticker"),
        "title": market.get("title"),
        "strike": strike,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "last": last,
        "probability": probability,
        "close_ts": close_ts,
    }


def get_orderbook(ticker):
    if not ticker:
        return {
            "yes_bid": None,
            "yes_size": None,
            "no_bid": None,
            "no_size": None,
            "yes_ask": None,
            "imbalance": None,
        }

    data = get_json(
        KALSHI_BASE + "/markets/" + ticker + "/orderbook",
        {"depth": 5},
    )

    book = data.get("orderbook_fp", {})

    yes_levels = book.get("yes_dollars", []) or []
    no_levels = book.get("no_dollars", []) or []

    yes_bid = number(yes_levels[0][0]) if yes_levels else None
    yes_size = number(yes_levels[0][1]) if yes_levels else None
    no_bid = number(no_levels[0][0]) if no_levels else None
    no_size = number(no_levels[0][1]) if no_levels else None

    # A YES bid at p is equivalent to a NO ask at 1-p.
    yes_ask = (1.0 - no_bid) if no_bid is not None else None

    total = (yes_size or 0) + (no_size or 0)
    imbalance = None
    if total > 0:
        imbalance = ((yes_size or 0) - (no_size or 0)) / total

    return {
        "yes_bid": yes_bid,
        "yes_size": yes_size,
        "no_bid": no_bid,
        "no_size": no_size,
        "yes_ask": yes_ask,
        "imbalance": imbalance,
    }


def make_signal(btc, market, book):
    price = btc["price"]
    strike = market["strike"]

    # Price vs target is the main factor.
    score = 0.0
    reasons = []

    if price > strike:
        score += 2.0
        reasons.append("BTC is above the target")
    elif price < strike:
        score -= 2.0
        reasons.append("BTC is below the target")

    # Short-term momentum.
    for label, value, weight in (
        ("1m", btc["m1"], 1.0),
        ("5m", btc["m5"], 1.0),
        ("15m", btc["m15"], 1.5),
    ):
        if value > 0.01:
            score += weight
            reasons.append(label + " momentum is positive")
        elif value < -0.01:
            score -= weight
            reasons.append(label + " momentum is negative")

    # Kalshi book pressure.
    imbalance = book.get("imbalance")
    if imbalance is not None:
        if imbalance > 0.15:
            score += 1.0
            reasons.append("Kalshi YES book has stronger bid size")
        elif imbalance < -0.15:
            score -= 1.0
            reasons.append("Kalshi NO book has stronger bid size")

    if score >= 4.0:
        verdict = "UP"
    elif score <= -4.0:
        verdict = "DOWN"
    else:
        verdict = "WAITING"

    # Confidence is deliberately conservative.
    confidence = min(95, max(0, int(abs(score) / 6.0 * 100)))

    return verdict, confidence, round(score, 2), reasons


def build_state():
    result = {
        "ok": False,
        "verdict": "WAITING",
        "confidence": 0,
        "score": 0,
        "reason": "Loading data...",
        "btc": None,
        "btc_source": None,
        "strike": None,
        "distance": None,
        "distance_pct": None,
        "m1": None,
        "m5": None,
        "m15": None,
        "kalshi_probability": None,
        "yes_bid": None,
        "yes_ask": None,
        "book_imbalance": None,
        "seconds_left": None,
        "market": None,
        "feed_status": "OFFLINE",
        "kalshi_status": "OFFLINE",
        "error": None,
    }

    # BTC feed
    try:
        btc = get_btc()
        result.update(
            {
                "btc": btc["price"],
                "btc_source": btc["source"],
                "m1": btc["m1"],
                "m5": btc["m5"],
                "m15": btc["m15"],
                "feed_status": "LIVE",
            }
        )
    except Exception as e:
        result["error"] = str(e)
        result["reason"] = "BTC data could not be loaded"
        return result

    # Kalshi market
    try:
        market = get_kalshi_market()
        result.update(
            {
                "strike": market["strike"],
                "kalshi_probability": market["probability"],
                "yes_bid": market["yes_bid"],
                "yes_ask": market["yes_ask"],
                "market": market["ticker"],
                "seconds_left": max(
                    0, int(market["close_ts"] - time.time())
                ),
                "kalshi_status": "LIVE",
            }
        )
    except Exception as e:
        result["error"] = str(e)
        result["reason"] = "Kalshi market could not be loaded"
        return result

    # Order book is useful, but it must never break the main dashboard.
    try:
        book = get_orderbook(market["ticker"])
        result["book_imbalance"] = book["imbalance"]
    except Exception:
        book = {
            "imbalance": None
        }

    result["distance"] = btc["price"] - market["strike"]
    result["distance_pct"] = (
        (btc["price"] - market["strike"]) / market["strike"] * 100.0
    )

    verdict, confidence, score, reasons = make_signal(
        btc, market, book
    )

    result.update(
        {
            "ok": True,
            "verdict": verdict,
            "confidence": confidence,
            "score": score,
            "reason": " • ".join(reasons) if reasons else "No strong setup",
        }
    )

    return result


HTML = r"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>BTC Strike AI</title>
<style>
body{
    margin:0;
    padding:16px;
    background:#070707;
    color:#fff;
    font-family:Arial,sans-serif;
}
.wrap{max-width:700px;margin:auto}
h1{margin:4px 0;font-size:28px}
.sub{color:#888;margin-bottom:14px}
.card{
    background:#141414;
    border:1px solid #282828;
    border-radius:16px;
    padding:16px;
    margin:10px 0;
}
.label{color:#888;font-size:12px;letter-spacing:1px}
.price{font-size:42px;font-weight:800;margin-top:6px}
.verdict{font-size:38px;font-weight:900;margin:6px 0}
.grid{
    display:grid;
    grid-template-columns:1fr 1fr;
    gap:10px;
}
.box{
    background:#101010;
    border-radius:12px;
    padding:12px;
}
.value{font-size:20px;font-weight:700;margin-top:5px}
.row{
    display:flex;
    justify-content:space-between;
    gap:12px;
    padding:10px 0;
    border-bottom:1px solid #292929;
}
.row:last-child{border-bottom:0}
.small{color:#999;font-size:12px;line-height:1.5}
.status{font-weight:700}
button{
    width:100%;
    padding:13px;
    border:0;
    border-radius:12px;
    background:#222;
    color:#fff;
    font-size:16px;
}
</style>
</head>
<body>
<div class="wrap">
<h1>BTC Strike AI</h1>
<div class="sub">Kalshi 15-Minute Decision Engine</div>

<div class="card">
<div class="label">MODEL VERDICT</div>
<div id="verdict" class="verdict">WAITING</div>
<div>Confidence: <b id="confidence">0%</b></div>
<div class="small" id="reason">Loading data...</div>
</div>

<div class="card">
<div class="label">BITCOIN</div>
<div id="btc" class="price">--</div>
<div class="small" id="btcsource">Feed: --</div>
</div>

<div class="grid">
<div class="box">
<div class="label">KALSHI TARGET</div>
<div id="strike" class="value">--</div>
</div>
<div class="box">
<div class="label">COUNTDOWN</div>
<div id="countdown" class="value">--</div>
</div>
<div class="box">
<div class="label">BTC vs TARGET</div>
<div id="distance" class="value">--</div>
</div>
<div class="box">
<div class="label">KALSHI YES</div>
<div id="prob" class="value">--</div>
</div>
</div>

<div class="card">
<div class="label">MOMENTUM</div>
<div class="row"><span>1 Minute</span><b id="m1">--</b></div>
<div class="row"><span>5 Minutes</span><b id="m5">--</b></div>
<div class="row"><span>15 Minutes</span><b id="m15">--</b></div>
</div>

<div class="card">
<div class="label">KALSHI ORDER BOOK</div>
<div class="row"><span>YES Bid</span><b id="yesbid">--</b></div>
<div class="row"><span>YES Ask</span><b id="yesask">--</b></div>
<div class="row"><span>Book Pressure</span><b id="imbalance">--</b></div>
</div>

<div class="card">
<div class="row"><span>BTC Feed</span><span id="feed" class="status">--</span></div>
<div class="row"><span>Kalshi Feed</span><span id="kalshi" class="status">--</span></div>
<div class="row"><span>Market</span><span id="market">--</span></div>
<div class="small" id="error"></div>
</div>

<div class="card">
<button onclick="loadState()">REFRESH NOW</button>
</div>
</div>

<script>
function money(x){
    return x === null || x === undefined ? "--" :
        "$" + Number(x).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
}
function pct(x){
    return x === null || x === undefined ? "--" :
        (x > 0 ? "+" : "") + Number(x).toFixed(3) + "%";
}
function countdown(s){
    if(s === null || s === undefined) return "--";
    s = Math.max(0,Number(s));
    return Math.floor(s/60) + "m " + String(s%60).padStart(2,"0") + "s";
}
function loadState(){
    fetch("/api/state?ts=" + Date.now())
    .then(r => r.json())
    .then(d => {
        document.getElementById("verdict").textContent = d.verdict || "WAITING";
        document.getElementById("confidence").textContent = (d.confidence || 0) + "%";
        document.getElementById("reason").textContent = d.reason || "";
        document.getElementById("btc").textContent = money(d.btc);
        document.getElementById("btcsource").textContent = "Feed: " + (d.btc_source || "--");
        document.getElementById("strike").textContent = money(d.strike);
        document.getElementById("countdown").textContent = countdown(d.seconds_left);
        document.getElementById("distance").textContent =
            d.distance === null ? "--" : money(d.distance) + " (" + pct(d.distance_pct) + ")";
        document.getElementById("prob").textContent =
            d.kalshi_probability === null ? "--" :
            (Number(d.kalshi_probability)*100).toFixed(1) + "%";
        document.getElementById("m1").textContent = pct(d.m1);
        document.getElementById("m5").textContent = pct(d.m5);
        document.getElementById("m15").textContent = pct(d.m15);
        document.getElementById("yesbid").textContent =
            d.yes_bid === null ? "--" : (Number(d.yes_bid)*100).toFixed(1) + "¢";
        document.getElementById("yesask").textContent =
            d.yes_ask === null ? "--" : (Number(d.yes_ask)*100).toFixed(1) + "¢";
        document.getElementById("imbalance").textContent =
            d.book_imbalance === null ? "--" :
            (Number(d.book_imbalance)*100).toFixed(1) + "%";
        document.getElementById("feed").textContent = d.feed_status || "--";
        document.getElementById("kalshi").textContent = d.kalshi_status || "--";
        document.getElementById("market").textContent = d.market || "--";
        document.getElementById("error").textContent = d.error || "";
    })
    .catch(e => {
        document.getElementById("error").textContent = "Dashboard error: " + e;
    });
}

loadState();
setInterval(loadState,5000);
</script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HTML)


@app.route("/api/state")
def api_state():
    return jsonify(build_state())


@app.route("/health")
def health():
    return jsonify({"ok": True, "service": "btc-strike-ai"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
