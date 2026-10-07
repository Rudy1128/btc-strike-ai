import time
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

BINANCE = "https://data-api.binance.vision"
KALSHI = "https://external-api.kalshi.com/trade-api/v2"

def get_json(url, params=None):
    r = requests.get(url, params=params, timeout=8)
    r.raise_for_status()
    return r.json()

def btc_data():
    data = get_json(
        BINANCE + "/api/v3/klines",
        {"symbol": "BTCUSDT", "interval": "1m", "limit": 16}
    )
    closes = [float(x[4]) for x in data]
    price = closes[-1]

    def move(n):
        if len(closes) <= n:
            return 0
        return (price - closes[-1-n]) / closes[-1-n] * 100

    return {
        "price": price,
        "m1": move(1),
        "m5": move(5),
        "m15": move(15),
    }

def find_value(m, names):
    for name in names:
        value = m.get(name)
        if value not in (None, ""):
            try:
                return float(value)
            except Exception:
                pass
    return None

def kalshi_data():
    data = get_json(
        KALSHI + "/markets",
        {"status": "open", "series_ticker": "KXBTC15M", "limit": 100}
    )
    markets = data.get("markets", [])

    if not markets:
        data = get_json(
            KALSHI + "/markets",
            {"status": "open", "limit": 200}
        )
        markets = [
            m for m in data.get("markets", [])
            if str(m.get("series_ticker", "")).upper() == "KXBTC15M"
            or str(m.get("ticker", "")).upper().startswith("KXBTC15M")
        ]

    if not markets:
        return None

    now = time.time()
    future = []

    for m in markets:
        close_text = (
            m.get("close_time")
            or m.get("expiration_time")
            or m.get("end_time")
        )
        if not close_text:
            continue
        try:
            ts = close_text.replace("Z", "+00:00")
            from datetime import datetime
            close_ts = datetime.fromisoformat(ts).timestamp()
            if close_ts > now:
                future.append((close_ts, m))
        except Exception:
            continue

    if not future:
        return None

    close_ts, m = min(future, key=lambda x: x[0])

    strike = find_value(
        m,
        ["floor_strike", "strike_price", "functional_strike",
         "target_price", "strike", "cap_strike"]
    )

    yes_bid = find_value(m, ["yes_bid"])
    yes_ask = find_value(m, ["yes_ask"])
    last = find_value(m, ["last_price"])

    probability = None
    if yes_bid is not None and yes_ask is not None:
        probability = (yes_bid + yes_ask) / 2
    elif last is not None:
        probability = last

    return {
        "ticker": m.get("ticker"),
        "strike": strike,
        "probability": probability,
        "close_ts": close_ts,
    }

def state():
    result = {
        "btc": None,
        "m1": 0,
        "m5": 0,
        "m15": 0,
        "strike": None,
        "probability": None,
        "seconds_left": None,
        "verdict": "WAITING",
        "score": 0,
        "market": None,
        "error": None,
    }

    try:
        b = btc_data()
        result.update({
            "btc": b["price"],
            "m1": b["m1"],
            "m5": b["m5"],
            "m15": b["m15"],
        })

        k = kalshi_data()
        if k:
            result.update({
                "strike": k["strike"],
                "probability": k["probability"],
                "market": k["ticker"],
                "seconds_left": max(0, int(k["close_ts"] - time.time())),
            })

            if k["strike"] is not None:
                score = 2 if b["price"] > k["strike"] else -2

                for x in (b["m1"], b["m5"], b["m15"]):
                    if x > 0:
                        score += 1
                    elif x < 0:
                        score -= 1

                result["score"] = score

                if score >= 4:
                    result["verdict"] = "UP"
                elif score <= -4:
                    result["verdict"] = "DOWN"

    except Exception as e:
        result["error"] = str(e)

    return result

HTML = (
    "<!doctype html><html><head>"
    "<meta name='viewport' content='width=device-width,initial-scale=1'>"
    "<title>BTC Strike AI</title>"
    "<style>"
    "body{background:#080808;color:white;font-family:Arial;margin:0;padding:20px}"
    ".box{background:#151515;border-radius:16px;padding:18px;margin:12px 0}"
    ".price{font-size:42px;font-weight:bold}"
    ".verdict{font-size:34px;font-weight:bold}"
    ".row{display:flex;justify-content:space-between;padding:9px 0;border-bottom:1px solid #333}"
    ".muted{color:#999}"
    "</style></head><body>"
    "<h1>BTC Strike AI</h1>"
    "<div class='box'><div class='muted'>BTC PRICE</div>"
    "<div id='btc' class='price'>--</div></div>"
    "<div class='box'><div class='row'><span>Kalshi Strike</span><b id='strike'>--</b></div>"
    "<div class='row'><span>Time Left</span><b id='time'>--</b></div>"
    "<div class='row'><span>Kalshi UP</span><b id='prob'>--</b></div>"
    "<div class='row'><span>Market</span><b id='market'>--</b></div></div>"
    "<div class='box'><div class='row'><span>1 Minute</span><b id='m1'>--</b></div>"
    "<div class='row'><span>5 Minutes</span><b id='m5'>--</b></div>"
    "<div class='row'><span>15 Minutes</span><b id='m15'>--</b></div></div>"
    "<div class='box'><div class='muted'>SIGNAL</div>"
    "<div id='verdict' class='verdict'>WAITING</div>"
    "<div>Score: <span id='score'>0</span></div>"
    "<div id='error' class='muted'></div></div>"
    "<script>"
    "function pct(x){return (x>0?'+':'')+x.toFixed(3)+'%'}"
    "function tick(){fetch('/api/state').then(r=>r.json()).then(d=>{"
    "document.getElementById('btc').textContent=d.btc?'$'+d.btc.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2}):'--';"
    "document.getElementById('strike').textContent=d.strike?'$'+d.strike.toLocaleString():'--';"
    "document.getElementById('time').textContent=d.seconds_left===null?'--':Math.floor(d.seconds_left/60)+'m '+d.seconds_left%60+'s';"
    "document.getElementById('prob').textContent=d.probability===null?'--':(d.probability*100).toFixed(1)+'%';"
    "document.getElementById('market').textContent=d.market||'--';"
    "document.getElementById('m1').textContent=pct(d.m1);"
    "document.getElementById('m5').textContent=pct(d.m5);"
    "document.getElementById('m15').textContent=pct(d.m15);"
    "document.getElementById('verdict').textContent=d.verdict;"
    "document.getElementById('score').textContent=d.score;"
    "document.getElementById('error').textContent=d.error||'';"
    "}).catch(e=>document.getElementById('error').textContent=e)}"
    "tick();setInterval(tick,5000);"
    "</script></body></html>"
)

@app.route("/")
def home():
    return render_template_string(HTML)

@app.route("/api/state")
def api_state():
    return jsonify(state())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
