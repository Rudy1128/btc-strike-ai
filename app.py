import os
import time
import math
import threading
from collections import deque
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, render_template_string

# ============================================================
# BTC STRIKE AI — STABLE BUILD
# ============================================================
# BRTI-STYLE multi-source reference + Kalshi 15-minute analysis.
#
# This is NOT the official CF Benchmarks BRTI calculation.
# It uses multiple public BTC/USD feeds, filters obvious
# outliers, and combines the reference price with technical
# signals. The system is deliberately conservative:
# conflicting signals -> UNDECIDED.
#
# Environment variables:
#   PORT
#   KALSHI_BASE_URL
#   KALSHI_TICKER (recommended if you know the exact market)
# ============================================================

app = Flask(__name__)

PORT = int(os.getenv("PORT", "10000"))
KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
).rstrip("/")
KALSHI_TICKER = os.getenv("KALSHI_TICKER", "").strip()

TIMEOUT = 4
UPDATE_SECONDS = 2

EXCHANGE_URLS = {
    "Binance": "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT",
    "Coinbase": "https://api.exchange.coinbase.com/products/BTC-USD/ticker",
    "Kraken": "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
    "Bitstamp": "https://www.bitstamp.net/api/v2/ticker/btcusd/",
}

history = deque(maxlen=1800)
lock = threading.Lock()

STATE = {
    "reference_price": None,
    "binance_price": None,
    "sources": {},
    "source_count": 0,
    "kalshi": {},
    "signals": {},
    "score": 0,
    "verdict": "UNDECIDED",
    "confidence": 0,
    "delta": 0.0,
    "cvd": 0.0,
    "last_update": None,
    "error": None,
}


def num(value):
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def now():
    return time.time()


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def pct_change(old, new):
    if old is None or new is None or old == 0:
        return 0.0
    return ((new - old) / old) * 100.0


def get_json(url):
    r = requests.get(
        url,
        timeout=TIMEOUT,
        headers={"User-Agent": "BTC-Strike-AI/1.0"},
    )
    r.raise_for_status()
    return r.json()


# -----------------------------
# Exchange feeds
# -----------------------------

def binance():
    d = get_json(EXCHANGE_URLS["Binance"])
    bid = num(d.get("bidPrice"))
    ask = num(d.get("askPrice"))
    if bid is None or ask is None:
        return None
    return {"price": (bid + ask) / 2, "bid": bid, "ask": ask}


def coinbase():
    d = get_json(EXCHANGE_URLS["Coinbase"])
    bid = num(d.get("bid"))
    ask = num(d.get("ask"))
    if bid is None or ask is None:
        return None
    return {"price": (bid + ask) / 2, "bid": bid, "ask": ask}


def kraken():
    d = get_json(EXCHANGE_URLS["Kraken"])
    result = d.get("result", {})
    if not result:
        return None
    pair = next(iter(result.values()))
    bid = num(pair.get("b", [None])[0])
    ask = num(pair.get("a", [None])[0])
    if bid is None or ask is None:
        return None
    return {"price": (bid + ask) / 2, "bid": bid, "ask": ask}


def bitstamp():
    d = get_json(EXCHANGE_URLS["Bitstamp"])
    bid = num(d.get("bid"))
    ask = num(d.get("ask"))
    if bid is None or ask is None:
        return None
    return {"price": (bid + ask) / 2, "bid": bid, "ask": ask}


FEEDS = {
    "Binance": binance,
    "Coinbase": coinbase,
    "Kraken": kraken,
    "Bitstamp": bitstamp,
}


def get_feeds():
    data = {}
    errors = []
    for name, fn in FEEDS.items():
        try:
            item = fn()
            if item and item.get("price") is not None:
                data[name] = item
        except Exception as exc:
            errors.append(f"{name}: {exc}")
    return data, errors


def reference_price(feeds):
    """
    Transparent BRTI-style approximation:
      1. collect exchange mid-prices
      2. calculate the median
      3. discard obvious outliers
      4. average the remaining sources

    This is NOT the official BRTI methodology.
    """
    prices = [x["price"] for x in feeds.values()]
    if not prices:
        return None, {}

    ordered = sorted(prices)
    middle = len(ordered) // 2
    median = (
        ordered[middle]
        if len(ordered) % 2
        else (ordered[middle - 1] + ordered[middle]) / 2
    )

    valid = {
        name: item
        for name, item in feeds.items()
        if median and abs(item["price"] - median) / median <= 0.0035
    }

    if not valid:
        valid = feeds

    return (
        sum(item["price"] for item in valid.values()) / len(valid),
        valid,
    )


# -----------------------------
# Kalshi market
# -----------------------------

def normalize_market(market):
    if not isinstance(market, dict):
        return {}

    strike = None
    for key in (
        "floor_strike",
        "strike_price",
        "strike",
        "target",
        "cap_strike",
    ):
        candidate = num(market.get(key))
        if candidate is not None:
            strike = candidate
            break

    return {
        "ticker": market.get("ticker"),
        "title": market.get("title"),
        "strike": strike,
        "yes_bid": num(market.get("yes_bid")),
        "yes_ask": num(market.get("yes_ask")),
        "no_bid": num(market.get("no_bid")),
        "no_ask": num(market.get("no_ask")),
        "last_price": num(market.get("last_price")),
        "close_time": market.get("close_time"),
    }


def get_kalshi():
    try:
        if KALSHI_TICKER:
            d = get_json(f"{KALSHI_BASE}/markets/{KALSHI_TICKER}")
            return normalize_market(d.get("market", d))

        d = get_json(f"{KALSHI_BASE}/markets?status=open&limit=100")
        markets = d.get("markets", [])

        candidates = []
        for m in markets:
            text = " ".join(
                str(m.get(k, ""))
                for k in ("ticker", "title", "subtitle", "event_ticker")
            ).lower()
            if "bitcoin" in text or "btc" in text:
                candidates.append(m)

        if not candidates:
            return {}

        candidates.sort(
            key=lambda m: (
                "15" not in str(m.get("title", "")).lower()
                and "15" not in str(m.get("ticker", "")).lower()
            )
        )
        return normalize_market(candidates[0])

    except Exception as exc:
        return {"error": str(exc)}


# -----------------------------
# Technical engine
# -----------------------------

def window_points(seconds):
    cutoff = now() - seconds
    return [(t, p) for t, p in history if t >= cutoff]


def momentum(seconds):
    points = window_points(seconds)
    if len(points) < 2:
        return 0.0
    return pct_change(points[0][1], points[-1][1])


def price_structure():
    if len(history) < 12:
        return "NOT ENOUGH DATA"

    values = [p for _, p in list(history)[-40:]]
    split = len(values) // 2
    first = values[:split]
    second = values[split:]

    if max(second) > max(first) and min(second) > min(first):
        return "HIGHER HIGHS / HIGHER LOWS"

    if max(second) < max(first) and min(second) < min(first):
        return "LOWER HIGHS / LOWER LOWS"

    return "MIXED / CHOP"


def calculate_signal(price, market):
    if price is None:
        return {
            "signals": {},
            "score": 0,
            "verdict": "UNDECIDED",
            "confidence": 0,
        }

    m1 = momentum(60)
    m5 = momentum(300)
    m15 = momentum(900)
    structure = price_structure()

    score = 0
    reasons = []

    strike = market.get("strike") if market else None
    distance_pct = None

    # Strike location is useful, but cannot decide by itself.
    if strike is not None and strike != 0:
        distance_pct = pct_change(strike, price)
        if price > strike:
            score += 2
            reasons.append("price above strike")
        elif price < strike:
            score -= 2
            reasons.append("price below strike")

    for value, weight, label in (
        (m1, 1, "1m"),
        (m5, 2, "5m"),
        (m15, 3, "15m"),
    ):
        if value > 0.025:
            score += weight
            reasons.append(f"{label} positive")
        elif value < -0.025:
            score -= weight
            reasons.append(f"{label} negative")

    if structure == "HIGHER HIGHS / HIGHER LOWS":
        score += 2
        reasons.append("bullish structure")
    elif structure == "LOWER HIGHS / LOWER LOWS":
        score -= 2
        reasons.append("bearish structure")

    delta = STATE["delta"]
    cvd = STATE["cvd"]

    if delta > 0:
        score += 1
        reasons.append("positive delta")
    elif delta < 0:
        score -= 1
        reasons.append("negative delta")

    if cvd > 0:
        score += 1
        reasons.append("CVD rising")
    elif cvd < 0:
        score -= 1
        reasons.append("CVD falling")

    bullish = (
        sum(x > 0.025 for x in (m1, m5, m15))
        + int(structure == "HIGHER HIGHS / HIGHER LOWS")
        + int(delta > 0)
        + int(cvd > 0)
    )
    bearish = (
        sum(x < -0.025 for x in (m1, m5, m15))
        + int(structure == "LOWER HIGHS / LOWER LOWS")
        + int(delta < 0)
        + int(cvd < 0)
    )

    confidence = 50 + min(abs(score) * 4, 40)

    if bullish and bearish:
        confidence -= min(18, min(bullish, bearish) * 6)

    confidence = max(50, min(90, int(confidence)))

    if score >= 7 and bullish >= 4 and bearish <= 2:
        verdict = "STRONG UP"
    elif score >= 3 and bullish >= 3 and bullish > bearish:
        verdict = "UP"
    elif score <= -7 and bearish >= 4 and bullish <= 2:
        verdict = "STRONG DOWN"
    elif score <= -3 and bearish >= 3 and bearish > bullish:
        verdict = "DOWN"
    else:
        verdict = "UNDECIDED"
        confidence = min(confidence, 59)

    return {
        "signals": {
            "m1": m1,
            "m5": m5,
            "m15": m15,
            "structure": structure,
            "strike_distance_pct": distance_pct,
            "delta": delta,
            "cvd": cvd,
            "bullish_signals": bullish,
            "bearish_signals": bearish,
            "reasons": reasons[-8:],
        },
        "score": score,
        "verdict": verdict,
        "confidence": confidence,
    }


def update_loop():
    while True:
        errors = []

        try:
            feeds, feed_errors = get_feeds()
            errors.extend(feed_errors)

            ref, valid = reference_price(feeds)

            with lock:
                if ref is not None:
                    old = STATE["reference_price"]

                    if old is not None:
                        move = ref - old
                        STATE["delta"] = move
                        STATE["cvd"] += move

                    STATE["reference_price"] = ref
                    history.append((now(), ref))

                    if "Binance" in feeds:
                        STATE["binance_price"] = feeds["Binance"]["price"]

                    STATE["sources"] = {
                        name: round(item["price"], 2)
                        for name, item in valid.items()
                    }
                    STATE["source_count"] = len(valid)

            market = get_kalshi()

            with lock:
                STATE["kalshi"] = market
                result = calculate_signal(
                    STATE["reference_price"],
                    market,
                )

                STATE["signals"] = result["signals"]
                STATE["score"] = result["score"]
                STATE["verdict"] = result["verdict"]
                STATE["confidence"] = result["confidence"]
                STATE["last_update"] = iso_now()
                STATE["error"] = "; ".join(errors) if errors else None

        except Exception as exc:
            with lock:
                STATE["error"] = str(exc)

        time.sleep(UPDATE_SECONDS)


# -----------------------------
# API
# -----------------------------

@app.get("/api/state")
def api_state():
    with lock:
        return jsonify({
            "reference_price": STATE["reference_price"],
            "binance_price": STATE["binance_price"],
            "sources": STATE["sources"],
            "source_count": STATE["source_count"],
            "kalshi": STATE["kalshi"],
            "signals": STATE["signals"],
            "score": STATE["score"],
            "verdict": STATE["verdict"],
            "confidence": STATE["confidence"],
            "delta": STATE["delta"],
            "cvd": STATE["cvd"],
            "last_update": STATE["last_update"],
            "error": STATE["error"],
        })


# -----------------------------
# Dashboard
# -----------------------------

PAGE = r"""
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>BTC Strike AI</title>
<style>
*{box-sizing:border-box}
body{
 margin:0;background:#070a10;color:#f5f7fb;
 font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,sans-serif
}
.wrap{max-width:1050px;margin:auto;padding:14px}
.header{
 display:flex;justify-content:space-between;align-items:center;
 gap:12px;padding:8px 2px 16px
}
.brand{font-size:24px;font-weight:900}
.sub{font-size:11px;color:#7f8a9d;margin-top:3px}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.card{
 background:#101620;border:1px solid #202a39;border-radius:17px;
 padding:14px;box-shadow:0 10px 28px rgba(0,0,0,.20)
}
.hero{
 grid-column:1/-1;text-align:center;padding:24px 14px;
 border:2px solid #303a4b;transition:.2s
}
.hero.up{border-color:#16c784;background:rgba(22,199,132,.13)}
.hero.down{border-color:#ff4d57;background:rgba(255,77,87,.13)}
.hero.wait{border-color:#d8aa35;background:rgba(216,170,53,.12)}
.label{
 color:#818da0;font-size:10px;font-weight:800;
 letter-spacing:.12em;text-transform:uppercase
}
.verdict{margin-top:6px;font-size:38px;line-height:1;font-weight:950}
.hero.up .verdict{color:#16e59a}
.hero.down .verdict{color:#ff5961}
.hero.wait .verdict{color:#f0c94d}
.conf{font-size:14px;color:#b7c0cf;margin-top:9px}
.big{font-size:26px;font-weight:850;margin-top:5px}
.small{font-size:12px;color:#a0aaba;margin-top:5px}
.row{
 display:flex;justify-content:space-between;padding:8px 0;
 border-bottom:1px solid #202938;font-size:13px
}
.row:last-child{border-bottom:0}
.upText{color:#16e59a}
.downText{color:#ff5961}
.waitText{color:#f0c94d}
.pills{display:flex;flex-wrap:wrap;gap:6px;margin-top:9px}
.pill{
 padding:5px 8px;border-radius:999px;background:#192230;
 color:#cbd3df;font-size:10px
}
.status{
 margin-top:10px;padding:9px;border-radius:10px;
 background:#0c1119;color:#758195;font-size:10px;word-break:break-word
}
@media(max-width:700px){
 .grid{grid-template-columns:1fr}
 .hero{grid-column:auto}
 .verdict{font-size:34px}
}
</style>
</head>
<body>
<div class="wrap">
 <div class="header">
  <div>
   <div class="brand">₿ BTC STRIKE AI</div>
   <div class="sub">Multi-exchange reference • Kalshi 15-minute decision engine</div>
  </div>
  <div class="sub" id="clock">CONNECTING</div>
 </div>

 <div class="grid">
  <div id="hero" class="card hero wait">
   <div class="label">CURRENT DECISION</div>
   <div id="verdict" class="verdict">UNDECIDED</div>
   <div id="confidence" class="conf">Confidence: --</div>
  </div>

  <div class="card">
   <div class="label">Composite BTC Reference</div>
   <div id="price" class="big">--</div>
   <div id="binance" class="small">Binance: --</div>
   <div id="sources" class="pills"></div>
  </div>

  <div class="card">
   <div class="label">Kalshi Strike</div>
   <div id="strike" class="big">--</div>
   <div id="distance" class="small">Distance: --</div>
  </div>

  <div class="card">
   <div class="label">1 Minute Momentum</div>
   <div id="m1" class="big">--</div>
  </div>

  <div class="card">
   <div class="label">5 Minute Momentum</div>
   <div id="m5" class="big">--</div>
  </div>

  <div class="card">
   <div class="label">15 Minute Momentum</div>
   <div id="m15" class="big">--</div>
  </div>

  <div class="card">
   <div class="label">Price Structure</div>
   <div id="structure" class="big" style="font-size:17px">--</div>
  </div>

  <div class="card">
   <div class="label">Pressure</div>
   <div class="row"><span>Delta proxy</span><b id="delta">--</b></div>
   <div class="row"><span>CVD proxy</span><b id="cvd">--</b></div>
   <div class="row"><span>Score</span><b id="score">--</b></div>
   <div class="row"><span>Agreement</span><b id="agreement">--</b></div>
  </div>

  <div class="card">
   <div class="label">Kalshi Market</div>
   <div class="row"><span>YES Bid</span><b id="yb">--</b></div>
   <div class="row"><span>YES Ask</span><b id="ya">--</b></div>
   <div class="row"><span>Last</span><b id="last">--</b></div>
  </div>

  <div class="card">
   <div class="label">System Status</div>
   <div class="small">
    Conservative engine: conflicting evidence produces UNDECIDED
    instead of forcing a direction.
   </div>
   <div id="error" class="status">No errors reported.</div>
  </div>
 </div>
</div>

<script>
function money(v){
 if(v===null||v===undefined||isNaN(v))return "--";
 return "$"+Number(v).toLocaleString(undefined,{
  minimumFractionDigits:2,maximumFractionDigits:2
 });
}
function pct(v){
 if(v===null||v===undefined||isNaN(v))return "--";
 return (v>=0?"+":"")+Number(v).toFixed(3)+"%";
}
function signed(v){
 if(v===null||v===undefined||isNaN(v))return "--";
 return (v>=0?"+":"")+Number(v).toFixed(4);
}
function move(id,v){
 const e=document.getElementById(id);
 e.textContent=pct(v);
 e.className="big "+(v>0?"upText":v<0?"downText":"waitText");
}
function pressure(id,v){
 const e=document.getElementById(id);
 e.textContent=signed(v);
 e.className=v>0?"upText":v<0?"downText":"waitText";
}

async function refresh(){
 try{
  const response=await fetch("/api/state?x="+Date.now(),{cache:"no-store"});
  const s=await response.json();

  document.getElementById("price").textContent=money(s.reference_price);
  document.getElementById("binance").textContent=
   "Binance: "+money(s.binance_price);

  const sources=document.getElementById("sources");
  sources.innerHTML="";
  for(const [name,value] of Object.entries(s.sources||{})){
   const p=document.createElement("span");
   p.className="pill";
   p.textContent=name+" "+money(value);
   sources.appendChild(p);
  }

  const k=s.kalshi||{};
  document.getElementById("strike").textContent=money(k.strike);

  const sig=s.signals||{};
  document.getElementById("distance").textContent=
   "Distance: "+pct(sig.strike_distance_pct);

  move("m1",sig.m1);
  move("m5",sig.m5);
  move("m15",sig.m15);

  document.getElementById("structure").textContent=
   sig.structure||"--";

  pressure("delta",s.delta);
  pressure("cvd",s.cvd);

  document.getElementById("score").textContent=
   (s.score>=0?"+":"")+s.score;

  document.getElementById("agreement").textContent=
   (sig.bullish_signals??"--")+" UP / "+
   (sig.bearish_signals??"--")+" DOWN";

  document.getElementById("yb").textContent=
   k.yes_bid==null?"--":k.yes_bid+"¢";
  document.getElementById("ya").textContent=
   k.yes_ask==null?"--":k.yes_ask+"¢";
  document.getElementById("last").textContent=
   k.last_price==null?"--":k.last_price+"¢";

  const verdict=(s.verdict||"UNDECIDED").toUpperCase();
  const hero=document.getElementById("hero");
  hero.className="card hero "+
   (verdict.includes("UP")?"up":
    verdict.includes("DOWN")?"down":"wait");

  document.getElementById("verdict").textContent=verdict;
  document.getElementById("confidence").textContent=
   "Confidence: "+(s.confidence??"--")+"%";

  document.getElementById("clock").textContent=
   s.last_update?
   new Date(s.last_update).toLocaleTimeString():
   "WAITING";

  document.getElementById("error").textContent=
   s.error?s.error:"All available feeds responding.";
 }catch(e){
  document.getElementById("clock").textContent="CONNECTION ERROR";
  document.getElementById("error").textContent=e.toString();
 }
}

refresh();
setInterval(refresh,2000);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(PAGE)


if __name__ == "__main__":
    threading.Thread(target=update_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
