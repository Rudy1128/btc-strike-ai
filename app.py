import os
import json
import time
import threading
from collections import deque
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template_string

try:
    import websocket
except Exception:
    websocket = None


app = Flask(__name__)


# ============================================================
# CONFIG
# ============================================================

KALSHI_BASES = [
    os.getenv(
        "KALSHI_BASE_URL",
        "https://external-api.kalshi.com/trade-api/v2"
    ).rstrip("/"),

    "https://api.elections.kalshi.com/trade-api/v2"
]

SERIES = "KXBTC15M"

POLL_SECONDS = 2.0

HTTP_TIMEOUT = 5


# ============================================================
# STATE
# ============================================================

lock = threading.Lock()

prices = {}

price_history = deque(
    maxlen=1800
)


orderbook = {
    "bids": [],
    "asks": [],
    "updated": 0.0,
    "connected": False
}


trade_flow = {
    "buy": 0.0,
    "sell": 0.0,
    "delta": 0.0,
    "updated": 0.0,
    "connected": False
}


state = {

    "btc": None,

    "composite": None,

    "kalshi": None,

    "seconds_left": None,

    "momentum_1m": 0.0,

    "momentum_5m": 0.0,

    "momentum_15m": 0.0,

    "structure": "NEUTRAL",

    "book_imbalance": 0.0,

    "book_bid_pct": None,

    "book_ask_pct": None,

    "trade_buy_pct": None,

    "trade_sell_pct": None,

    "trade_delta": 0.0,

    "settlement_estimate": None,

    "settlement_gap": None,

    "score": 0,

    "confidence": 0,

    "verdict": "WAIT",

    "reasons": [],

    "status": "Starting...",

    "error": None
}


# ============================================================
# HELPERS
# ============================================================

def num(value, default=None):

    try:
        return float(value)

    except Exception:

        return default


def parse_ts(value):

    if not value:
        return None

    try:

        return datetime.fromisoformat(
            str(value).replace(
                "Z",
                "+00:00"
            )
        ).timestamp()

    except Exception:

        return None


def get_json(url, params=None):

    try:

        response = requests.get(
            url,
            params=params,
            timeout=HTTP_TIMEOUT,
            headers={
                "User-Agent":
                    "BTC-Strike-AI"
            }
        )

        response.raise_for_status()

        return response.json()

    except Exception:

        return None


# ============================================================
# BTC LIVE PRICE — COINBASE
# ============================================================

def coinbase_ws():

    if websocket is None:
        return

    url = (
        "wss://advanced-trade-ws.coinbase.com"
    )

    while True:

        try:

            def opened(ws):

                ws.send(
                    json.dumps({
                        "type": "subscribe",
                        "product_ids": [
                            "BTC-USD"
                        ],
                        "channel": "ticker"
                    })
                )


            def message(ws, raw):

                try:

                    data = json.loads(
                        raw
                    )

                    for event in data.get(
                        "events",
                        []
                    ):

                        for tick in event.get(
                            "tickers",
                            []
                        ):

                            price = num(
                                tick.get(
                                    "price"
                                )
                            )

                            if price:

                                with lock:

                                    prices[
                                        "Coinbase"
                                    ] = price

                                    price_history.append(
                                        (
                                            time.time(),
                                            price
                                        )
                                    )

                except Exception:

                    pass


            ws = websocket.WebSocketApp(
                url,
                on_open=opened,
                on_message=message
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:

            pass

        time.sleep(2)


# ============================================================
# BACKUP EXCHANGE PRICES
# ============================================================

def exchange_poll():

    while True:

        # Binance

        data = get_json(
            "https://api.binance.com/api/v3/ticker/price",
            {
                "symbol": "BTCUSDT"
            }
        )

        price = (
            num(
                data.get("price")
            )
            if isinstance(data, dict)
            else None
        )

        if price:

            with lock:

                prices[
                    "Binance"
                ] = price

                price_history.append(
                    (
                        time.time(),
                        price
                    )
                )


        # Kraken

        data = get_json(
            "https://api.kraken.com/0/public/Ticker",
            {
                "pair": "XBTUSD"
            }
        )

        try:

            item = next(
                iter(
                    data[
                        "result"
                    ].values()
                )
            )

            price = num(
                item[
                    "c"
                ][0]
            )

        except Exception:

            price = None


        if price:

            with lock:

                prices[
                    "Kraken"
                ] = price


        # Bitstamp

        data = get_json(
            "https://www.bitstamp.net/api/v2/ticker/btcusd/"
        )

        price = (
            num(
                data.get("last")
            )
            if isinstance(data, dict)
            else None
        )

        if price:

            with lock:

                prices[
                    "Bitstamp"
                ] = price


        time.sleep(2)


# ============================================================
# MULTI-EXCHANGE COMPOSITE
# ============================================================

def composite_price():

    with lock:

        values = list(
            prices.values()
        )

    if not values:

        return None

    values.sort()

    if len(values) >= 4:

        values = values[1:-1]

    return sum(values) / len(values)


# ============================================================
# BINANCE BTC ORDER BOOK
#
# THIS IS THE IMPORTANT FIX
#
# Binance depth20@100ms provides the top 20
# bid/ask levels every 100ms.
# ============================================================

def binance_market_ws():

    if websocket is None:

        return

    url = (
        "wss://stream.binance.com:9443/"
        "ws/btcusdt@depth20@100ms"
    )

    while True:

        try:

            def depth_message(
                ws,
                raw
            ):

                try:

                    data = json.loads(
                        raw
                    )

                    bids = data.get(
                        "bids",
                        []
                    )

                    asks = data.get(
                        "asks",
                        []
                    )

                    with lock:

                        orderbook[
                            "bids"
                        ] = bids

                        orderbook[
                            "asks"
                        ] = asks

                        orderbook[
                            "updated"
                        ] = time.time()

                        orderbook[
                            "connected"
                        ] = True

                except Exception:

                    pass


            ws = websocket.WebSocketApp(
                url,
                on_message=depth_message
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:

            pass


        with lock:

            orderbook[
                "connected"
            ] = False


        time.sleep(2)


# ============================================================
# BINANCE LIVE TRADE FLOW
# ============================================================

def binance_trade_ws():

    if websocket is None:

        return

    url = (
        "wss://stream.binance.com:9443/"
        "ws/btcusdt@aggTrade"
    )

    while True:

        try:

            def trade_message(
                ws,
                raw
            ):

                try:

                    data = json.loads(
                        raw
                    )

                    quantity = num(
                        data.get("q"),
                        0
                    ) or 0

                    price = num(
                        data.get("p"),
                        0
                    ) or 0

                    value = (
                        quantity *
                        price
                    )


                    with lock:

                        if data.get("m"):

                            trade_flow[
                                "sell"
                            ] += value

                        else:

                            trade_flow[
                                "buy"
                            ] += value


                        trade_flow[
                            "delta"
                        ] = (
                            trade_flow[
                                "buy"
                            ]
                            -
                            trade_flow[
                                "sell"
                            ]
                        )


                        trade_flow[
                            "updated"
                        ] = time.time()


                        trade_flow[
                            "connected"
                        ] = True


                except Exception:

                    pass


            ws = websocket.WebSocketApp(
                url,
                on_message=trade_message
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception:

            pass


        with lock:

            trade_flow[
                "connected"
            ] = False


        time.sleep(2)


# ============================================================
# ORDER BOOK REST FALLBACK
# ============================================================

def orderbook_fallback():

    while True:

        with lock:

            fresh = (
                orderbook[
                    "updated"
                ]
                and
                time.time()
                -
                orderbook[
                    "updated"
                ]
                <
                3
            )


        if not fresh:

            data = get_json(
                "https://data-api.binance.vision/api/v3/depth",
                {
                    "symbol": "BTCUSDT",
                    "limit": 20
                }
            )


            if isinstance(
                data,
                dict
            ):

                with lock:

                    orderbook[
                        "bids"
                    ] = data.get(
                        "bids",
                        []
                    )

                    orderbook[
                        "asks"
                    ] = data.get(
                        "asks",
                        []
                    )

                    orderbook[
                        "updated"
                    ] = time.time()


        time.sleep(3)


# ============================================================
# KALSHI
# ============================================================

def kalshi_market():

    for base in KALSHI_BASES:

        data = get_json(
            base + "/markets",
            {
                "series_ticker":
                    SERIES,

                "status":
                    "open",

                "limit":
                    100
            }
        )


        markets = (
            data.get(
                "markets",
                []
            )
            if isinstance(
                data,
                dict
            )
            else []
        )


        candidates = []

        now = time.time()


        for market in markets:

            close = parse_ts(
                market.get(
                    "close_time"
                )
                or
                market.get(
                    "expiration_time"
                )
            )


            if (
                close is not None
                and
                close >= now - 2
            ):

                candidates.append(
                    (
                        close,
                        market
                    )
                )


        if not candidates:

            continue


        candidates.sort(
            key=lambda x: x[0]
        )


        market = candidates[0][1]


        target = None


        for key in (

            "floor_strike",

            "strike_price",

            "strike",

            "target",

            "floor_strike_dollars"

        ):

            target = num(
                market.get(key)
            )

            if target is not None:

                break


        if target is None:

            continue


        return {

            "ticker":
                market.get(
                    "ticker"
                ),

            "target":
                target,

            "yes_bid":
                num(
                    market.get(
                        "yes_bid_dollars",
                        market.get(
                            "yes_bid"
                        )
                    )
                ),

            "yes_ask":
                num(
                    market.get(
                        "yes_ask_dollars",
                        market.get(
                            "yes_ask"
                        )
                    )
                ),

            "last":
                num(
                    market.get(
                        "last_price_dollars",
                        market.get(
                            "last_price"
                        )
                    )
                ),

            "close_time":
                market.get(
                    "close_time"
                )
                or
                market.get(
                    "expiration_time"
                )
        }


    return None


# ============================================================
# KALSHI BOOK
# ============================================================

def kalshi_book(
    ticker
):

    if not ticker:

        return 0.0


    for base in KALSHI_BASES:

        data = get_json(
            f"{base}/markets/{ticker}/orderbook",
            {
                "depth":
                    20
            }
        )


        if not isinstance(
            data,
            dict
        ):

            continue


        book = (
            data.get(
                "orderbook_fp"
            )
            or
            data.get(
                "orderbook"
            )
            or
            data
        )


        yes = (
            book.get(
                "yes_dollars"
            )
            or
            book.get(
                "yes"
            )
            or
            []
        )


        no = (
            book.get(
                "no_dollars"
            )
            or
            book.get(
                "no"
            )
            or
            []
        )


        def size(item):

            if isinstance(
                item,
                (list, tuple)
            ):

                if len(item) >= 2:

                    return (
                        num(
                            item[1],
                            0
                        )
                        or
                        0
                    )


            if isinstance(
                item,
                dict
            ):

                return (
                    num(
                        item.get(
                            "quantity",
                            item.get(
                                "size"
                            )
                        ),
                        0
                    )
                    or
                    0
                )


            return 0


        yes_size = sum(
            size(x)
            for x in yes[:10]
        )


        no_size = sum(
            size(x)
            for x in no[:10]
        )


        total = (
            yes_size +
            no_size
        )


        if total:

            return (
                yes_size -
                no_size
            ) / total


    return 0.0


# ============================================================
# MOMENTUM
# ============================================================

def momentum(
    seconds
):

    with lock:

        history = list(
            price_history
        )


    if len(history) < 2:

        return 0.0


    cutoff = (
        time.time()
        -
        seconds
    )


    old_price = next(
        (
            price
            for timestamp, price
            in reversed(
                history[:-1]
            )
            if timestamp <= cutoff
        ),
        history[0][1]
    )


    latest = history[-1][1]


    if not old_price:

        return 0.0


    return (
        (
            latest /
            old_price
        )
        -
        1
    ) * 100


# ============================================================
# PRICE STRUCTURE
# ============================================================

def structure():

    with lock:

        points = [
            price
            for _, price
            in list(
                price_history
            )[-90:]
        ]


    if len(points) < 20:

        return "NEUTRAL"


    first = points[:20]

    last = points[-20:]


    if (
        min(last) > min(first)
        and
        max(last) > max(first)
    ):

        return (
            "HIGHER-HIGHS / "
            "HIGHER-LOWS"
        )


    if (
        min(last) < min(first)
        and
        max(last) < max(first)
    ):

        return (
            "LOWER-HIGHS / "
            "LOWER-LOWS"
        )


    return "NEUTRAL"


# ============================================================
# ORDER BOOK PRESSURE
# ============================================================

def pressure_snapshot():

    with lock:

        bids = list(
            orderbook[
                "bids"
            ]
        )

        asks = list(
            orderbook[
                "asks"
            ]
        )

        buy = trade_flow[
            "buy"
        ]

        sell = trade_flow[
            "sell"
        ]

        delta = trade_flow[
            "delta"
        ]

        order_updated = (
            orderbook[
                "updated"
            ]
        )

        flow_updated = (
            trade_flow[
                "updated"
            ]
        )


    bid_quantity = sum(

        (
            num(
                item[1],
                0
            )
            or
            0
        )

        for item in bids

        if len(item) >= 2
    )


    ask_quantity = sum(

        (
            num(
                item[1],
                0
            )
            or
            0
        )

        for item in asks

        if len(item) >= 2
    )


    total = (
        bid_quantity +
        ask_quantity
    )


    bid_pct = (
        bid_quantity /
        total *
        100
        if total
        else None
    )


    ask_pct = (
        ask_quantity /
        total *
        100
        if total
        else None
    )


    order_age = (
        time.time()
        -
        order_updated
        if order_updated
        else None
    )


    flow_age = (
        time.time()
        -
        flow_updated
        if flow_updated
        else None
    )


    return {

        "bid_pct":
            bid_pct,

        "ask_pct":
            ask_pct,

        "imbalance":
            (
                (
                    bid_quantity -
                    ask_quantity
                )
                /
                total
                if total
                else 0
            ),

        "buy":
            buy,

        "sell":
            sell,

        "delta":
            delta,

        "book_age_ms":
            (
                order_age * 1000
                if order_age is not None
                else None
            ),

        "flow_age_ms":
            (
                flow_age * 1000
                if flow_age is not None
                else None
            ),

        "book_live":
            bool(
                order_age is not None
                and
                order_age < 3
            ),

        "flow_live":
            bool(
                flow_age is not None
                and
                flow_age < 3
            )
    }


# ============================================================
# SIGNAL ENGINE
# ============================================================

def calculate():

    market = kalshi_market()

    reference = (
        composite_price()
    )

    m1 = momentum(60)

    m5 = momentum(300)

    m15 = momentum(900)

    price_structure = structure()

    pressure = (
        pressure_snapshot()
    )


    score = 0

    reasons = []


    if market and reference is not None:

        gap = (
            reference -
            market["target"]
        )


        if gap > 0:

            score += 2

            reasons.append(
                "BTC is above the Kalshi strike"
            )

        else:

            score -= 2

            reasons.append(
                "BTC is below the Kalshi strike"
            )


        if m1 > 0.01:

            score += 1

        elif m1 < -0.01:

            score -= 1


        if m5 > 0.03:

            score += 2

            reasons.append(
                "5m momentum is UP"
            )

        elif m5 < -0.03:

            score -= 2

            reasons.append(
                "5m momentum is DOWN"
            )


        if m15 > 0.05:

            score += 1

        elif m15 < -0.05:

            score -= 1


        if price_structure.startswith(
            "HIGHER"
        ):

            score += 2

            reasons.append(
                "Price structure is bullish"
            )


        elif price_structure.startswith(
            "LOWER"
        ):

            score -= 2

            reasons.append(
                "Price structure is bearish"
            )


        # ----------------------------------------
        # BTC ORDER BOOK
        # ----------------------------------------

        if pressure[
            "bid_pct"
        ] is not None:

            if pressure[
                "bid_pct"
            ] > 55:

                score += 1

                reasons.append(
                    "BTC order book favors bids"
                )


            elif pressure[
                "ask_pct"
            ] > 55:

                score -= 1

                reasons.append(
                    "BTC order book favors asks"
                )


        # ----------------------------------------
        # TRADE FLOW
        # ----------------------------------------

        total_flow = (
            pressure["buy"]
            +
            pressure["sell"]
        )


        if total_flow > 0:

            if (
                pressure["buy"]
                >
                pressure["sell"]
                * 1.08
            ):

                score += 1

                reasons.append(
                    "Recent trade flow favors buyers"
                )


            elif (
                pressure["sell"]
                >
                pressure["buy"]
                * 1.08
            ):

                score -= 1

                reasons.append(
                    "Recent trade flow favors sellers"
                )


        # ----------------------------------------
        # KALSHI YES
        # ----------------------------------------

        yes = market.get(
            "last"
        )


        if (
            yes is None
            and
            market.get(
                "yes_bid"
            ) is not None
            and
            market.get(
                "yes_ask"
            ) is not None
        ):

            yes = (
                market["yes_bid"]
                +
                market["yes_ask"]
            ) / 2


        if yes is not None:

            if yes >= 0.60:

                score += 1


            elif yes <= 0.40:

                score -= 1


    # ========================================================
    # COUNTDOWN
    # ========================================================

    seconds_left = None

    settlement_gap = None


    if market:

        close = parse_ts(
            market.get(
                "close_time"
            )
        )


        if close is not None:

            seconds_left = max(
                0,
                int(
                    close -
                    time.time()
                )
            )


        if reference is not None:

            settlement_gap = (
                reference -
                market["target"]
            )


    # ========================================================
    # FINAL MINUTE
    # ========================================================

    if (
        seconds_left is not None
        and
        seconds_left <= 60
        and
        settlement_gap is not None
    ):

        if settlement_gap > 0:

            score += 1

        elif settlement_gap < 0:

            score -= 1


        reasons.append(
            "Final-minute settlement mode"
        )


    # ========================================================
    # VERDICT
    # ========================================================

    if score >= 5:

        verdict = "UP"

    elif score <= -5:

        verdict = "DOWN"

    else:

        verdict = "WAIT"


    if verdict != "WAIT":

        confidence = min(
            95,
            50 +
            abs(score) * 6
        )

    else:

        confidence = min(
            49,
            35 +
            abs(score) * 3
        )


    # ========================================================
    # SAVE
    # ========================================================

    with lock:

        state.update({

            "btc":
                reference,

            "composite":
                reference,

            "kalshi":
                market,

            "seconds_left":
                seconds_left,

            "momentum_1m":
                m1,

            "momentum_5m":
                m5,

            "momentum_15m":
                m15,

            "structure":
                price_structure,

            "book_imbalance":
                pressure[
                    "imbalance"
                ],

            "book_bid_pct":
                pressure[
                    "bid_pct"
                ],

            "book_ask_pct":
                pressure[
                    "ask_pct"
                ],

            "trade_buy_pct":
                (
                    pressure["buy"]
                    /
                    (
                        pressure["buy"]
                        +
                        pressure["sell"]
                    )
                    *
                    100
                )
                if (
                    pressure["buy"]
                    +
                    pressure["sell"]
                )
                else None,

            "trade_sell_pct":
                (
                    pressure["sell"]
                    /
                    (
                        pressure["buy"]
                        +
                        pressure["sell"]
                    )
                    *
                    100
                )
                if (
                    pressure["buy"]
                    +
                    pressure["sell"]
                )
                else None,

            "trade_delta":
                pressure[
                    "delta"
                ],

            "settlement_estimate":
                reference,

            "settlement_gap":
                settlement_gap,

            "score":
                score,

            "confidence":
                confidence,

            "verdict":
                verdict,

            "reasons":
                reasons[-8:],

            "status":
                (
                    "LIVE • BTC ORDER BOOK"
                    if pressure[
                        "book_live"
                    ]
                    else
                    "LIVE • BTC PRICE"
                ),

            "error":
                None
        })


# ============================================================
# WORKER
# ============================================================

def worker():

    while True:

        try:

            calculate()

        except Exception as error:

            with lock:

                state[
                    "error"
                ] = str(error)

        time.sleep(
            POLL_SECONDS
        )


# ============================================================
# DASHBOARD
# ============================================================

HTML = r'''
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

* {
    box-sizing: border-box;
}

body {

    margin: 0;

    background:
        radial-gradient(
            circle at top,
            #121a25 0%,
            #070a0f 55%
        );

    color:
        #eef3f8;

    font-family:
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Arial,
        sans-serif;

    padding:
        14px;
}

.wrap {

    max-width:
        960px;

    margin:
        auto;
}

.title {

    font-size:
        29px;

    font-weight:
        900;

    letter-spacing:
        -1px;
}

.subtitle {

    color:
        #8995a6;

    margin-top:
        3px;

    margin-bottom:
        14px;

    font-size:
        13px;
}

.grid {

    display:
        grid;

    grid-template-columns:
        repeat(2, 1fr);

    gap:
        10px;
}

.card {

    background:
        #101620;

    border:
        1px solid
        #202b3a;

    border-radius:
        17px;

    padding:
        15px;

    box-shadow:
        0 10px 35px
        rgba(0,0,0,.18);
}

.label {

    color:
        #8995a6;

    font-size:
        11px;

    font-weight:
        700;

    text-transform:
        uppercase;

    letter-spacing:
        .09em;
}

.big {

    font-size:
        27px;

    font-weight:
        850;

    margin-top:
        5px;
}

.small {

    color:
        #778394;

    font-size:
        11px;

    margin-top:
        5px;
}

.verdict {

    text-align:
        center;

    padding:
        21px;

    border-radius:
        20px;

    margin-bottom:
        11px;

    border:
        2px solid
        #2a3442;

    transition:
        .2s ease;
}

.verdict .big {

    font-size:
        37px;

    margin:
        2px 0;
}

.up {

    background:
        #082518;

    border-color:
        #20d17a;

    box-shadow:
        0 0 35px
        rgba(32,209,122,.10);
}

.down {

    background:
        #2a0b11;

    border-color:
        #ff5064;

    box-shadow:
        0 0 35px
        rgba(255,80,100,.10);
}

.wait {

    background:
        #211c0b;

    border-color:
        #d3ad3b;
}

.row {

    display:
        flex;

    justify-content:
        space-between;

    padding:
        7px 0;

    border-bottom:
        1px solid
        #1c2531;

    font-size:
        13px;
}

.green {

    color:
        #29d47e;
}

.red {

    color:
        #ff5c6c;
}

.yellow {

    color:
        #e0bd48;
}

.pressure {

    margin-top:
        9px;

    height:
        12px;

    background:
        #1a2430;

    border-radius:
        8px;

    overflow:
        hidden;
}

.buybar {

    height:
        100%;

    background:
        #20d17a;
}

.reason {

    color:
        #d5dce5;

    line-height:
        1.7;

    font-size:
        13px;
}

@media(max-width:650px) {

    .grid {

        grid-template-columns:
            1fr 1fr;
    }

    .big {

        font-size:
            22px;
    }

    .verdict .big {

        font-size:
            33px;
    }
}

@media(max-width:480px) {

    .grid {

        grid-template-columns:
            1fr;
    }
}

</style>

</head>

<body>

<div class="wrap">


<div class="title">

₿ BTC Strike AI

</div>


<div class="subtitle">

BRTI-style • KXBTC15M • settlement-aware • live BTC order book

</div>


<div
    id="hero"
    class="verdict wait"
>

<div class="label">

AI VERDICT

</div>


<div
    id="verdict"
    class="big"
>

WAIT

</div>


<div id="confidence">

0% confidence

</div>

</div>


<div class="grid">


<div class="card">

<div class="label">

BTC REFERENCE

</div>

<div
    id="btc"
    class="big"
>

—

</div>

<div
    id="source"
    class="small"
>

Composite

</div>

</div>


<div class="card">

<div class="label">

KALSHI TARGET

</div>

<div
    id="target"
    class="big"
>

—

</div>

<div
    id="ticker"
    class="small"
>

—

</div>

</div>


<div class="card">

<div class="label">

60s SETTLEMENT EST.

</div>

<div
    id="avg"
    class="big"
>

—

</div>

<div
    id="gap"
    class="small"
>

—

</div>

</div>


<div class="card">

<div class="label">

TIME LEFT

</div>

<div
    id="time"
    class="big"
>

—

</div>

<div class="small">

Final minute =
settlement mode

</div>

</div>


<div class="card">

<div class="label">

1m / 5m / 15m

</div>

<div
    id="momentum"
    class="big"
>

—

</div>

</div>


<div class="card">

<div class="label">

PRICE STRUCTURE

</div>

<div
    id="structure"
    class="big"
>

—

</div>

</div>


<div class="card">

<div class="label">

BTC ORDER BOOK PRESSURE

</div>

<div
    id="book"
    class="big"
>

—

</div>

<div
    id="bookdetail"
    class="small"
>

Waiting for depth stream...

</div>

<div class="pressure">

<div
    id="buybar"
    class="buybar"
    style="width:50%"
>

</div>

</div>

</div>


<div class="card">

<div class="label">

TRADE FLOW

</div>

<div
    id="flow"
    class="big"
>

—

</div>

<div
    id="flowdetail"
    class="small"
>

—

</div>

</div>


<div class="card">

<div class="label">

KALSHI YES

</div>

<div
    id="yes"
    class="big"
>

—

</div>

</div>


<div class="card">

<div class="label">

MODEL SCORE

</div>

<div
    id="score"
    class="big"
>

—

</div>

</div>


</div>


<div
    class="card"
    style="margin-top:10px"
>

<div class="label">

SIGNAL REASONS

</div>

<div
    id="reasons"
    class="reason"
>

Waiting for data...

</div>

</div>


<div
    class="card"
    style="margin-top:10px"
>

<div class="label">

SYSTEM STATUS

</div>

<div
    id="status"
    class="small"
>

Starting...

</div>

<div
    id="error"
    class="small red"
>

</div>

</div>


</div>


<script>

const $ =
    id =>
        document.getElementById(id);


function money(value) {

    if (
        value === null ||
        value === undefined
    )
        return "—";


    return "$" +
        Number(
            value
        ).toLocaleString(
            undefined,
            {
                minimumFractionDigits:
                    2,

                maximumFractionDigits:
                    2
            }
        );
}


function pct(value) {

    if (
        value === null ||
        value === undefined
    )
        return "—";


    const n =
        Number(value);


    return (
        n >= 0
            ? "+"
            : ""
    )
    +
    n.toFixed(3)
    +
    "%";
}


function clock(seconds) {

    if (
        seconds === null ||
        seconds === undefined
    )
        return "—";


    seconds =
        Math.max(
            0,
            Math.floor(
                seconds
            )
        );


    return (
        Math.floor(
            seconds / 60
        )
        +
        ":"
        +
        String(
            seconds % 60
        ).padStart(
            2,
            "0"
        )
    );
}


async function refresh() {

    try {

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


        const s =
            await response.json();


        $("btc").textContent =
            money(
                s.btc
            );


        $("target").textContent =
            money(
                s.kalshi &&
                s.kalshi.target
            );


        $("ticker").textContent =
            (
                s.kalshi &&
                s.kalshi.ticker
            )
            ||
            "—";


        $("avg").textContent =
            money(
                s.settlement_estimate
            );


        $("gap").textContent =
            s.settlement_gap == null

            ?

            "—"

            :

            (
                s.settlement_gap >= 0
                    ? "+"
                    : ""
            )
            +
            money(
                s.settlement_gap
            )
            +
            " vs strike";


        $("time").textContent =
            clock(
                s.seconds_left
            );


        $("momentum").textContent =
            pct(
                s.momentum_1m
            )
            +
            " / "
            +
            pct(
                s.momentum_5m
            )
            +
            " / "
            +
            pct(
                s.momentum_15m
            );


        $("structure").textContent =
            s.structure ||
            "—";


        // ==========================================
        // ORDER BOOK PRESSURE
        // ==========================================

        const bidPct =
            s.book_bid_pct;

        const askPct =
            s.book_ask_pct;


        if (
            bidPct == null ||
            askPct == null
        ) {

            $("book").textContent =
                "UNAVAILABLE";

            $("bookdetail").textContent =
                "Waiting for Binance depth...";

            $("buybar").style.width =
                "50%";

        }

        else {

            if (bidPct > 55) {

                $("book").textContent =
                    "🟢 BID PRESSURE";

            }

            else if (askPct > 55) {

                $("book").textContent =
                    "🔴 ASK PRESSURE";

            }

            else {

                $("book").textContent =
                    "🟡 BALANCED";
            }


            $("bookdetail").textContent =
                "Bids "
                +
                bidPct.toFixed(1)
                +
                "% • Asks "
                +
                askPct.toFixed(1)
                +
                "% • "
                +
                (
                    s.book_age_ms
                    ?
                    Math.round(
                        s.book_age_ms
                    )
                    +
                    " ms"
                    :
                    "LIVE"
                );


            $("buybar").style.width =
                bidPct +
                "%";
        }


        // ==========================================
        // TRADE FLOW
        // ==========================================

        const buy =
            s.trade_buy_pct;

        const sell =
            s.trade_sell_pct;


        if (
            buy == null ||
            sell == null
        ) {

            $("flow").textContent =
                "UNAVAILABLE";

            $("flowdetail").textContent =
                "Waiting for trade stream...";

        }

        else {

            $("flow").textContent =
                buy > sell
                    ?
                    "🟢 BUYERS"
                    :
                    "🔴 SELLERS";


            $("flowdetail").textContent =
                "Buy "
                +
                buy.toFixed(1)
                +
                "% • Sell "
                +
                sell.toFixed(1)
                +
                "% • Delta "
                +
                (
                    s.trade_delta >= 0
                        ? "+$"
                        : "-$"
                )
                +
                Math.abs(
                    s.trade_delta
                ).toLocaleString(
                    undefined,
                    {
                        maximumFractionDigits:
                            0
                    }
                );
        }


        // ==========================================
        // KALSHI YES
        // ==========================================

        let yes = null;


        if (s.kalshi) {

            if (
                s.kalshi.last != null
            ) {

                yes =
                    s.kalshi.last;

            }

            else if (
                s.kalshi.yes_bid != null
                &&
                s.kalshi.yes_ask != null
            ) {

                yes =
                    (
                        s.kalshi.yes_bid
                        +
                        s.kalshi.yes_ask
                    )
                    /
                    2;
            }
        }


        $("yes").textContent =
            yes == null
                ?
                "—"
                :
                (
                    yes * 100
                ).toFixed(1)
                +
                "%";


        // ==========================================
        // SCORE
        // ==========================================

        $("score").textContent =
            s.score;


        // ==========================================
        // VERDICT
        // ==========================================

        $("verdict").textContent =
            s.verdict;


        $("confidence").textContent =
            Math.round(
                s.confidence
            )
            +
            "% confidence";


        // ==========================================
        // REASONS
        // ==========================================

        $("reasons").textContent =
            (
                s.reasons || []
            ).join(
                " • "
            )
            ||
            "Waiting for enough confirmation";


        // ==========================================
        // STATUS
        // ==========================================

        $("status").textContent =
            s.status ||
            "—";


        $("error").textContent =
            s.error ||
            "";


        // ==========================================
        // VERDICT COLOR
        // ==========================================

        const verdict =
            s.verdict ||
            "WAIT";


        if (
            verdict === "UP"
        ) {

            $("hero").className =
                "verdict up";

        }

        else if (
            verdict === "DOWN"
        ) {

            $("hero").className =
                "verdict down";

        }

        else {

            $("hero").className =
                "verdict wait";
        }


    }

    catch (error) {

        $("error").textContent =
            error.toString();
    }
}


refresh();


setInterval(
    refresh,
    1000
);

</script>

</body>

</html>
'''


# ============================================================
# ROUTES
# ============================================================

@app.get("/")
def home():

    return render_template_string(
        HTML
    )


@app.get("/api/state")
def api_state():

    with lock:

        output = dict(
            state
        )


        output[
            "exchange_prices"
        ] = dict(
            prices
        )


        output[
            "book_age_ms"
        ] = (
            (
                time.time()
                -
                orderbook[
                    "updated"
                ]
            )
            *
            1000
            if orderbook[
                "updated"
            ]
            else None
        )


        output[
            "book_live"
        ] = bool(
            orderbook[
                "updated"
            ]
            and
            time.time()
            -
            orderbook[
                "updated"
            ]
            <
            3
        )


        output[
            "flow_live"
        ] = bool(
            trade_flow[
                "updated"
            ]
            and
            time.time()
            -
            trade_flow[
                "updated"
            ]
            <
            3
        )


    return jsonify(
        output
    )


@app.get("/health")
def health():

    return jsonify({
        "ok":
            True
    })


# ============================================================
# START BACKGROUND SERVICES
#
# IMPORTANT:
# These start at import time so Gunicorn/Render
# also receives the live feeds.
# ============================================================

if websocket is not None:

    threading.Thread(
        target=coinbase_ws,
        daemon=True
    ).start()


    threading.Thread(
        target=binance_market_ws,
        daemon=True
    ).start()


    threading.Thread(
        target=binance_trade_ws,
        daemon=True
    ).start()


threading.Thread(
    target=exchange_poll,
    daemon=True
).start()


threading.Thread(
    target=orderbook_fallback,
    daemon=True
).start()


threading.Thread(
    target=worker,
    daemon=True
).start()


# ============================================================
# LOCAL DEVELOPMENT
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "5000"
            )
        ),
        debug=False
    )
