import os
import time
import json
import statistics
from datetime import datetime, timezone, timedelta

import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

TIMEOUT = 5
CACHE_SECONDS = 2
HISTORY_REFRESH = 15

# Signal Memory
MEMORY_MAX = 500
MEMORY_FILE = "signal_memory.json"

KALSHI_BASE = os.getenv(
    "KALSHI_BASE_URL",
    "https://api.elections.kalshi.com/trade-api/v2",
).rstrip("/")

KALSHI_TICKER = os.getenv(
    "KALSHI_TICKER",
    ""
).strip()

session = requests.Session()

session.headers.update({
    "User-Agent": "BTC-Strike-AI/8.0"
})


# =========================================================
# RUNTIME STATE
# =========================================================

cache = {
    "time": 0,
    "state": None
}

history_cache = {
    "time": 0,
    "candles": []
}

feed_health = {}

market_history = []

signal_memory = []

active_market = None


# =========================================================
# BASIC HELPERS
# =========================================================

def number(value):

    try:
        return float(value)

    except (
        TypeError,
        ValueError
    ):
        return None


def get_json(
    url,
    params=None
):

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


def seconds_left(value):

    dt = parse_time(value)

    if not dt:
        return None

    return max(
        0,
        int(
            dt.timestamp()
            - time.time()
        )
    )


# =========================================================
# SIGNAL MEMORY STORAGE
# =========================================================

def load_memory():

    global signal_memory

    try:

        with open(
            MEMORY_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        if isinstance(
            data,
            list
        ):

            signal_memory = (
                data[-MEMORY_MAX:]
            )

        else:

            signal_memory = []

    except Exception:

        signal_memory = []


def save_memory():

    try:

        with open(
            MEMORY_FILE,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                signal_memory[-MEMORY_MAX:],
                file
            )

    except Exception:

        # The engine continues working
        # even if local persistence is unavailable.
        pass


# =========================================================
# FEED HEALTH
# =========================================================

def record_feed(
    name,
    value
):

    item = feed_health.setdefault(
        name,
        {}
    )

    if value is not None:

        item["last_success"] = (
            time.time()
        )

        item["online"] = True

    else:

        item["online"] = False

    return value


# =========================================================
# BTC LIVE FEEDS
# =========================================================

def get_spot_feeds():

    feeds = {}

    # -----------------------------------------------------
    # BINANCE
    # -----------------------------------------------------

    data = get_json(
        "https://api.binance.com/api/v3/ticker/price",
        {
            "symbol": "BTCUSDT"
        }
    )

    binance = (
        number(
            data.get("price")
        )
        if isinstance(
            data,
            dict
        )
        else None
    )

    feeds["Binance"] = record_feed(
        "Binance",
        binance
    )

    # -----------------------------------------------------
    # COINBASE
    # -----------------------------------------------------

    data = get_json(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    )

    try:

        coinbase = number(
            data["data"]["amount"]
        )

    except Exception:

        coinbase = None

    feeds["Coinbase"] = record_feed(
        "Coinbase",
        coinbase
    )

    # -----------------------------------------------------
    # KRAKEN
    # -----------------------------------------------------

    data = get_json(
        "https://api.kraken.com/0/public/Ticker",
        {
            "pair": "XBTUSD"
        }
    )

    try:

        result = data["result"]

        pair = next(
            iter(result)
        )

        kraken = number(
            result[pair]["c"][0]
        )

    except Exception:

        kraken = None

    feeds["Kraken"] = record_feed(
        "Kraken",
        kraken
    )

    # -----------------------------------------------------
    # BITSTAMP
    # -----------------------------------------------------

    data = get_json(
        "https://www.bitstamp.net/api/v2/ticker/btcusd/"
    )

    bitstamp = (
        number(
            data.get("last")
        )
        if isinstance(
            data,
            dict
        )
        else None
    )

    feeds["Bitstamp"] = record_feed(
        "Bitstamp",
        bitstamp
    )

    # -----------------------------------------------------
    # COMPOSITE REFERENCE
    # -----------------------------------------------------

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
        if abs(
            value - median
        ) / median <= 0.0035
    ]

    reference = statistics.median(
        filtered or valid
    )

    return reference, feeds


# =========================================================
# DATA QUALITY BRAIN
# =========================================================

def calculate_feed_quality(
    feeds
):

    now = time.time()

    live = 0
    fresh = 0

    values = []

    for name, value in feeds.items():

        item = feed_health.get(
            name,
            {}
        )

        last_success = item.get(
            "last_success"
        )

        age = (
            now - last_success
            if last_success
            else None
        )

        if value is not None:

            live += 1

            values.append(
                value
            )

        if (
            value is not None
            and
            age is not None
            and
            age <= 20
        ):

            fresh += 1

    spread = None

    if len(values) >= 2:

        median = statistics.median(
            values
        )

        if median:

            spread = (
                max(values)
                - min(values)
            ) / median * 100

    score = 0

    if live == 4:
        score += 35

    elif live == 3:
        score += 30

    elif live == 2:
        score += 20

    elif live == 1:
        score += 8

    if fresh == 4:
        score += 25

    elif fresh == 3:
        score += 22

    elif fresh == 2:
        score += 15

    elif fresh == 1:
        score += 5

    if spread is not None:

        if spread <= 0.03:
            score += 30

        elif spread <= 0.08:
            score += 25

        elif spread <= 0.20:
            score += 15

        elif spread <= 0.35:
            score += 5

    if live >= 2:

        score += 10

    score = min(
        100,
        score
    )

    if score >= 85:
        grade = "HIGH"

    elif score >= 70:
        grade = "GOOD"

    elif score >= 50:
        grade = "FAIR"

    else:
        grade = "LOW"

    return {

        "score": score,

        "grade": grade,

        "live": live,

        "fresh": fresh,

        "spread": (
            round(
                spread,
                4
            )
            if spread is not None
            else None
        )
    }


# =========================================================
# BTC HISTORY
# =========================================================

def get_history():

    now = time.time()

    if (
        history_cache["candles"]
        and
        now - history_cache["time"]
        < HISTORY_REFRESH
    ):

        return (
            history_cache["candles"]
        )

    end = datetime.now(
        timezone.utc
    )

    start = (
        end
        - timedelta(
            minutes=21
        )
    )

    sources = [

        (
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",

            {
                "granularity": 60,
                "start": start.isoformat(),
                "end": end.isoformat()
            },

            "coinbase"
        ),

        (
            "https://api.kraken.com/0/public/OHLC",

            {
                "pair": "XBTUSD",
                "interval": 1
            },

            "kraken"
        ),

        (
            "https://api.binance.com/api/v3/klines",

            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "limit": 21
            },

            "binance"
        )
    ]

    for (
        url,
        params,
        mode
    ) in sources:

        data = get_json(
            url,
            params
        )

        candles = []

        try:

            if mode == "coinbase":

                rows = (
                    data
                    if isinstance(
                        data,
                        list
                    )
                    else []
                )

                for row in rows:

                    candles.append(
                        (
                            float(row[0]),
                            number(row[4])
                        )
                    )

            elif mode == "kraken":

                result = data["result"]

                pair = next(
                    key
                    for key in result
                    if key != "last"
                )

                for row in result[pair][-21:]:

                    candles.append(
                        (
                            float(row[0]),
                            number(row[4])
                        )
                    )

            else:

                rows = (
                    data
                    if isinstance(
                        data,
                        list
                    )
                    else []
                )

                for row in rows:

                    candles.append(
                        (
                            float(row[0]) / 1000,
                            number(row[4])
                        )
                    )

        except Exception:

            candles = []

        candles = [
            (
                timestamp,
                close
            )
            for timestamp, close in candles
            if close is not None
        ]

        candles.sort()

        if len(candles) >= 16:

            history_cache["candles"] = (
                candles
            )

            history_cache["time"] = now

            return candles

    history_cache["candles"] = []
    history_cache["time"] = now

    return []


# =========================================================
# MOMENTUM
# =========================================================

def momentum(
    candles,
    minutes
):

    if len(candles) < 2:

        return None

    current_time, current = (
        candles[-1]
    )

    target_time = (
        current_time
        - minutes * 60
    )

    previous = None

    for (
        timestamp,
        close
    ) in candles:

        if timestamp <= target_time:

            previous = close

        else:

            break

    if previous in (
        None,
        0
    ):

        return None

    return (
        (current - previous)
        / previous
    ) * 100


# =========================================================
# PRICE STRUCTURE
# =========================================================

def price_structure(
    candles
):

    if len(candles) < 8:

        return "WAIT"

    values = [
        close
        for _, close
        in candles[-8:]
    ]

    first = values[:4]
    second = values[4:]

    if (
        max(second) > max(first)
        and
        min(second) > min(first)
    ):

        return (
            "HIGHER HIGHS / "
            "HIGHER LOWS"
        )

    if (
        max(second) < max(first)
        and
        min(second) < min(first)
    ):

        return (
            "LOWER HIGHS / "
            "LOWER LOWS"
        )

    return "MIXED"


# =========================================================
# KALSHI
# =========================================================

def normalize_market(
    market
):

    if (
        not isinstance(
            market,
            dict
        )
        or
        not market.get(
            "ticker"
        )
    ):

        return None

    def probability(
        *keys
    ):

        for key in keys:

            value = number(
                market.get(
                    key
                )
            )

            if value is not None:

                if value > 1:

                    return (
                        value / 100
                    )

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

        value = number(
            market.get(
                key
            )
        )

        if value is not None:

            target = value

            break

    return {

        "ticker":
            market.get(
                "ticker"
            ),

        "title":
            (
                market.get(
                    "title"
                )
                or
                market.get(
                    "subtitle"
                )
                or
                ""
            ),

        "target":
            target,

        "yes_bid":
            probability(
                "yes_bid_dollars",
                "yes_bid"
            ),

        "yes_ask":
            probability(
                "yes_ask_dollars",
                "yes_ask"
            ),

        "last":
            probability(
                "last_price_dollars",
                "last_price"
            ),

        "close_time":
            (
                market.get(
                    "close_time"
                )
                or
                market.get(
                    "expiration_time"
                )
            )
    }


def get_kalshi():

    if KALSHI_TICKER:

        data = get_json(
            f"{KALSHI_BASE}/markets/"
            f"{KALSHI_TICKER}"
        )

        if isinstance(
            data,
            dict
        ):

            return normalize_market(
                data.get(
                    "market",
                    data
                )
            )

    data = get_json(
        f"{KALSHI_BASE}/markets",
        {
            "status": "open",
            "limit": 200
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
                "event_ticker"
            )
        ).lower()

        if (
            "btc" not in text
            and
            "bitcoin" not in text
        ):

            continue

        close = parse_time(
            market.get(
                "close_time"
            )
            or
            market.get(
                "expiration_time"
            )
        )

        if (
            close
            and
            close.timestamp()
            > now
        ):

            candidates.append(
                market
            )

    def sort_key(
        market
    ):

        ticker = str(
            market.get(
                "ticker",
                ""
            )
        ).lower()

        title = str(
            market.get(
                "title",
                ""
            )
        ).lower()

        close = parse_time(
            market.get(
                "close_time"
            )
            or
            market.get(
                "expiration_time"
            )
        )

        is_15m = (
            "15" in ticker
            or
            "15" in title
        )

        return (
            not is_15m,
            (
                close.timestamp()
                if close
                else float("inf")
            )
        )

    candidates.sort(
        key=sort_key
    )

    if not candidates:

        return None

    return normalize_market(
        candidates[0]
    )


def yes_mid(
    market
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
        and
        ask is not None
    ):

        return (
            bid + ask
        ) / 2

    return market.get(
        "last"
    )


# =========================================================
# KALSHI HISTORY
# =========================================================

def record_kalshi(
    market
):

    mid = yes_mid(
        market
    )

    if mid is None:

        return

    ticker = market.get(
        "ticker"
    )

    now = time.time()

    market_history.append(
        (
            now,
            ticker,
            mid
        )
    )

    cutoff = (
        now
        - 20 * 60
    )

    while (
        market_history
        and
        market_history[0][0]
        < cutoff
    ):

        market_history.pop(0)


def kalshi_change(
    ticker,
    seconds=60
):

    now = time.time()

    current = None
    previous = None

    for (
        timestamp,
        saved_ticker,
        mid
    ) in reversed(
        market_history
    ):

        if saved_ticker != ticker:

            continue

        if current is None:

            current = mid

        elif (
            timestamp
            <= now - seconds
        ):

            previous = mid

            break

    if (
        current is None
        or
        previous is None
    ):

        return None

    return (
        current - previous
    ) * 100


# =========================================================
# KALSHI QUALITY
# =========================================================

def calculate_kalshi_quality(
    market
):

    if not market:

        return {
            "score": 0,
            "grade": "OFFLINE"
        }

    score = 0

    if market.get(
        "ticker"
    ):

        score += 30

    if market.get(
        "target"
    ) is not None:

        score += 25

    if yes_mid(
        market
    ) is not None:

        score += 25

    if market.get(
        "close_time"
    ):

        score += 20

    if score >= 85:

        grade = "HIGH"

    elif score >= 70:

        grade = "GOOD"

    elif score >= 50:

        grade = "FAIR"

    else:

        grade = "LOW"

    return {
        "score": score,
        "grade": grade
    }


def overall_quality(
    btc_quality,
    kalshi_quality,
    history_points
):

    score = (
        btc_quality["score"]
        * 0.60
        +
        kalshi_quality["score"]
        * 0.25
    )

    if history_points >= 20:

        score += 15

    elif history_points >= 16:

        score += 10

    elif history_points >= 10:

        score += 5

    score = round(
        min(
            100,
            score
        )
    )

    if score >= 85:

        grade = "HIGH"

    elif score >= 70:

        grade = "GOOD"

    elif score >= 50:

        grade = "FAIR"

    else:

        grade = "LOW"

    return score, grade


# =========================================================
# MOMENTUM STATES
# =========================================================

def acceleration_state(
    m1,
    m5,
    m15
):

    if None in (
        m1,
        m5,
        m15
    ):

        return "UNKNOWN"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 < -0.01
    ):

        return "ACCELERATING DOWN"

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 > 0.01
    ):

        return "ACCELERATING UP"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 > 0.02
    ):

        return "SHORT-TERM REVERSAL UP"

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 < -0.02
    ):

        return "SHORT-TERM REVERSAL DOWN"

    return "STABLE / MIXED"


def reversal_state(
    m1,
    m5,
    m15
):

    if None in (
        m1,
        m5,
        m15
    ):

        return "UNKNOWN"

    if (
        m15 < -0.04
        and
        m5 < -0.02
        and
        m1 > 0.02
    ):

        return (
            "HIGH — BULLISH REVERSAL"
        )

    if (
        m15 > 0.04
        and
        m5 > 0.02
        and
        m1 < -0.02
    ):

        return (
            "HIGH — BEARISH REVERSAL"
        )

    if (
        m15 < -0.04
        and
        m5 < 0
        and
        m1 > 0
    ):

        return (
            "MEDIUM — POSSIBLE BULLISH TURN"
        )

    if (
        m15 > 0.04
        and
        m5 > 0
        and
        m1 < 0
    ):

        return (
            "MEDIUM — POSSIBLE BEARISH TURN"
        )

    return "LOW"


# =========================================================
# SIGNAL MEMORY
# =========================================================

def historical_match(
    current
):

    candidates = [
        record
        for record in signal_memory
        if record.get(
            "outcome"
        ) in (
            "UP",
            "DOWN"
        )
    ]

    if (
        len(candidates) < 3
    ):

        return {
            "matches": 0,
            "wins": 0,
            "losses": 0,
            "rate": None,
            "direction": None
        }

    def distance(
        current_record,
        old_record
    ):

        fields = [
            "m1",
            "m5",
            "m15",
            "distance_pct",
            "yes_mid"
        ]

        scales = {
            "m1": 0.05,
            "m5": 0.08,
            "m15": 0.12,
            "distance_pct": 0.10,
            "yes_mid": 0.20
        }

        total = 0
        count = 0

        for field in fields:

            current_value = (
                current_record.get(
                    field
                )
            )

            old_value = (
                old_record.get(
                    field
                )
            )

            if (
                current_value is None
                or
                old_value is None
            ):

                continue

            total += min(
                2,
                abs(
                    current_value
                    - old_value
                )
                /
                scales[field]
            )

            count += 1

        if (
            current_record.get(
                "structure"
            )
            !=
            old_record.get(
                "structure"
            )
        ):

            total += 0.8

        return (
            total
            /
            max(
                1,
                count
            )
        )

    scored = sorted(
        (
            (
                distance(
                    current,
                    record
                ),
                record
            )
            for record in candidates
        ),
        key=lambda item: item[0]
    )

    matches = [
        record
        for similarity, record
        in scored[:20]
        if similarity <= 1.35
    ]

    if not matches:

        return {
            "matches": 0,
            "wins": 0,
            "losses": 0,
            "rate": None,
            "direction": None
        }

    direction = current.get(
        "direction"
    )

    wins = sum(
        1
        for record in matches
        if record.get(
            "outcome"
        ) == direction
    )

    losses = (
        len(matches)
        - wins
    )

    rate = (
        wins
        /
        len(matches)
    ) * 100

    return {

        "matches":
            len(matches),

        "wins":
            wins,

        "losses":
            losses,

        "rate":
            round(
                rate,
                1
            ),

        "direction":
            direction
    }


# =========================================================
# MEMORY MARKET TRACKER
# =========================================================

def finalize_memory(
    ticker,
    target,
    final_price
):

    global active_market
    global signal_memory

    if (
        not active_market
        or
        active_market.get(
            "ticker"
        ) != ticker
    ):

        return

    if (
        final_price is None
        or
        target is None
    ):

        return

    if final_price > target:

        outcome = "UP"

    elif final_price < target:

        outcome = "DOWN"

    else:

        outcome = "PUSH"

    record = dict(
        active_market
    )

    record["outcome"] = outcome

    record["final_price"] = round(
        final_price,
        2
    )

    record["finished"] = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    signal_memory.append(
        record
    )

    signal_memory = (
        signal_memory[-MEMORY_MAX:]
    )

    save_memory()

    active_market = None


def update_memory(
    market,
    price,
    signal,
    distance_pct,
    yes_mid_value
):

    global active_market

    if not market:

        return

    ticker = market.get(
        "ticker"
    )

    target = market.get(
        "target"
    )

    close = parse_time(
        market.get(
            "close_time"
        )
    )

    if (
        not ticker
        or
        target is None
        or
        close is None
    ):

        return

    now = time.time()

    # -----------------------------------------------------
    # A new market arrived.
    # Finish the old one using the last price we saw
    # for that old market.
    # -----------------------------------------------------

    if (
        active_market
        and
        active_market.get(
            "ticker"
        ) != ticker
    ):

        if (
            active_market.get(
                "close_ts",
                0
            )
            <= now
        ):

            finalize_memory(
                active_market.get(
                    "ticker"
                ),
                active_market.get(
                    "target"
                ),
                active_market.get(
                    "last_price"
                )
            )

    # -----------------------------------------------------
    # Start tracking a new market.
    # -----------------------------------------------------

    if (
        not active_market
        or
        active_market.get(
            "ticker"
        ) != ticker
    ):

        direction = (
            signal.get(
                "verdict"
            )
            if signal.get(
                "verdict"
            ) in (
                "UP",
                "DOWN"
            )
            else "WAIT"
        )

        active_market = {

            "ticker":
                ticker,

            "target":
                target,

            "close_ts":
                close.timestamp(),

            "direction":
                direction,

            "m1":
                signal.get(
                    "m1"
                ),

            "m5":
                signal.get(
                    "m5"
                ),

            "m15":
                signal.get(
                    "m15"
                ),

            "distance_pct":
                distance_pct,

            "yes_mid":
                yes_mid_value,

            "structure":
                signal.get(
                    "structure"
                ),

            "quality":
                signal.get(
                    "quality_score"
                ),

            "last_price":
                price,

            "created":
                datetime.now(
                    timezone.utc
                ).isoformat()
        }

        return

    # -----------------------------------------------------
    # Update current market.
    # -----------------------------------------------------

    active_market[
        "last_price"
    ] = price

    if signal.get(
        "verdict"
    ) in (
        "UP",
        "DOWN"
    ):

        if (
            active_market.get(
                "direction"
            )
            == "WAIT"
        ):

            active_market[
                "direction"
            ] = signal.get(
                "verdict"
            )

        active_market[
            "m1"
        ] = signal.get(
            "m1"
        )

        active_market[
            "m5"
        ] = signal.get(
            "m5"
        )

        active_market[
            "m15"
        ] = signal.get(
            "m15"
        )

        active_market[
            "distance_pct"
        ] = distance_pct

        active_market[
            "yes_mid"
        ] = yes_mid_value

        active_market[
            "structure"
        ] = signal.get(
            "structure"
        )

        active_market[
            "quality"
        ] = signal.get(
            "quality_score"
        )

    # -----------------------------------------------------
    # Close market.
    # -----------------------------------------------------

    if (
        active_market.get(
            "close_ts",
            0
        )
        <= now
    ):

        finalize_memory(
            ticker,
            target,
            active_market.get(
                "last_price"
            )
        )


# =========================================================
# DECISION ENGINE
# =========================================================

def build_signal(
    price,
    market,
    candles,
    quality_score
):

    m1 = momentum(
        candles,
        1
    )

    m5 = momentum(
        candles,
        5
    )

    m15 = momentum(
        candles,
        15
    )

    struct = price_structure(
        candles
    )

    acceleration = (
        acceleration_state(
            m1,
            m5,
            m15
        )
    )

    reversal = (
        reversal_state(
            m1,
            m5,
            m15
        )
    )

    result = {

        "verdict":
            "WAIT",

        "label":
            "WAIT",

        "confidence":
            0,

        "score":
            0,

        "bullish":
            0,

        "bearish":
            0,

        "agreement":
            "NO DATA",

        "structure":
            struct,

        "acceleration":
            acceleration,

        "reversal":
            reversal,

        "pressure":
            None,

        "kalshi_change":
            None,

        "warning":
            None,

        "reasons":
            [],

        "m1":
            m1,

        "m5":
            m5,

        "m15":
            m15,

        "quality_score":
            quality_score
    }

    if (
        price is None
        or
        not market
        or
        market.get(
            "target"
        ) is None
    ):

        result["label"] = (
            "WAIT — BUILDING DATA"
        )

        result["confidence"] = 25

        result["reasons"] = [
            "Waiting for complete BTC and Kalshi data."
        ]

        return result

    if None in (
        m1,
        m5,
        m15
    ):

        result["label"] = (
            "WAIT — BUILDING HISTORY"
        )

        result["confidence"] = 25

        result["reasons"] = [
            "Building complete 1m/5m/15m history."
        ]

        return result

    target = market[
        "target"
    ]

    distance_pct = (
        (price - target)
        / target
    ) * 100

    bullish = 0
    bearish = 0

    bullish_reasons = []
    bearish_reasons = []
    neutral_reasons = []

    # -----------------------------------------------------
    # TARGET
    # -----------------------------------------------------

    if distance_pct > 0.03:

        bullish += 1

        bullish_reasons.append(
            "BTC is above the Kalshi target."
        )

    elif distance_pct < -0.03:

        bearish += 1

        bearish_reasons.append(
            "BTC is below the Kalshi target."
        )

    else:

        neutral_reasons.append(
            "BTC is very close to the Kalshi target."
        )

    # -----------------------------------------------------
    # 1 MINUTE
    # -----------------------------------------------------

    if m1 > 0.01:

        bullish += 1

        bullish_reasons.append(
            "1m momentum is positive."
        )

    elif m1 < -0.01:

        bearish += 1

        bearish_reasons.append(
            "1m momentum is negative."
        )

    else:

        neutral_reasons.append(
            "1m momentum is neutral."
        )

    # -----------------------------------------------------
    # 5 MINUTE
    # -----------------------------------------------------

    if m5 > 0.02:

        bullish += 1

        bullish_reasons.append(
            "5m momentum is positive."
        )

    elif m5 < -0.02:

        bearish += 1

        bearish_reasons.append(
            "5m momentum is negative."
        )

    else:

        neutral_reasons.append(
            "5m momentum is neutral."
        )

    # -----------------------------------------------------
    # 15 MINUTE
    # -----------------------------------------------------

    if m15 > 0.04:

        bullish += 1

        bullish_reasons.append(
            "15m momentum is positive."
        )

    elif m15 < -0.04:

        bearish += 1

        bearish_reasons.append(
            "15m momentum is negative."
        )

    else:

        neutral_reasons.append(
            "15m momentum is neutral."
        )

    # -----------------------------------------------------
    # STRUCTURE
    # -----------------------------------------------------

    if struct == (
        "HIGHER HIGHS / HIGHER LOWS"
    ):

        bullish += 1

        bullish_reasons.append(
            "Price structure is bullish."
        )

    elif struct == (
        "LOWER HIGHS / LOWER LOWS"
    ):

        bearish += 1

        bearish_reasons.append(
            "Price structure is bearish."
        )

    else:

        neutral_reasons.append(
            "Price structure is mixed."
        )

    # -----------------------------------------------------
    # KALSHI YES
    # -----------------------------------------------------

    mid = yes_mid(
        market
    )

    if mid is not None:

        result["pressure"] = (
            mid - 0.50
        ) * 200

        if mid >= 0.60:

            bullish += 1

            bullish_reasons.append(
                "Kalshi YES pricing favors UP."
            )

        elif mid <= 0.40:

            bearish += 1

            bearish_reasons.append(
                "Kalshi YES pricing favors DOWN."
            )

        else:

            neutral_reasons.append(
                "Kalshi YES pricing is near neutral."
            )

    record_kalshi(
        market
    )

    kchange = kalshi_change(
        market.get(
            "ticker"
        ),
        60
    )

    if kchange is not None:

        result[
            "kalshi_change"
        ] = round(
            kchange,
            2
        )

    # -----------------------------------------------------
    # ACCELERATION
    # -----------------------------------------------------

    if acceleration == (
        "ACCELERATING UP"
    ):

        bullish += 1

        bullish_reasons.append(
            "Short-term momentum is accelerating upward."
        )

    elif acceleration == (
        "ACCELERATING DOWN"
    ):

        bearish += 1

        bearish_reasons.append(
            "Short-term momentum is accelerating downward."
        )

    difference = abs(
        bullish - bearish
    )

    remaining = seconds_left(
        market.get(
            "close_time"
        )
    )

    # -----------------------------------------------------
    # CORE DECISION
    # -----------------------------------------------------

    if reversal.startswith(
        "HIGH"
    ):

        result["label"] = (
            "WAIT — REVERSAL RISK"
        )

        result["confidence"] = 58

        result["warning"] = reversal

    elif (
        bullish >= 5
        and
        bullish - bearish >= 3
    ):

        result["verdict"] = "UP"

        result["label"] = (
            "UP — STRONG CONFIRMATION"
        )

        result["confidence"] = 90

    elif (
        bearish >= 5
        and
        bearish - bullish >= 3
    ):

        result["verdict"] = "DOWN"

        result["label"] = (
            "DOWN — STRONG CONFIRMATION"
        )

        result["confidence"] = 90

    elif (
        bullish >= 4
        and
        bullish > bearish
        and
        difference >= 2
    ):

        result["verdict"] = "UP"

        result["label"] = (
            "UP — CONFIRMED"
        )

        result["confidence"] = 72

    elif (
        bearish >= 4
        and
        bearish > bullish
        and
        difference >= 2
    ):

        result["verdict"] = "DOWN"

        result["label"] = (
            "DOWN — CONFIRMED"
        )

        result["confidence"] = 72

    else:

        result["label"] = (
            "WAIT — CONFLICT"
        )

        result["confidence"] = 50

        result["warning"] = (
            "Signals are not aligned enough."
        )

    # -----------------------------------------------------
    # FINAL MINUTE BRAKE
    # -----------------------------------------------------

    if (
        remaining is not None
        and
        remaining <= 60
        and
        difference < 4
    ):

        result["verdict"] = "WAIT"

        result["label"] = (
            "WAIT — FINAL MINUTE"
        )

        result["confidence"] = 55

        result["warning"] = (
            "Final 60 seconds require stronger confirmation."
        )

    # -----------------------------------------------------
    # DATA QUALITY BRAKE
    # -----------------------------------------------------

    if quality_score < 50:

        result["verdict"] = "WAIT"

        result["label"] = (
            "WAIT — LOW DATA QUALITY"
        )

        result["confidence"] = 35

        result["warning"] = (
            "Not enough reliable live data."
        )

    elif quality_score < 70:

        result["confidence"] = min(
            result["confidence"],
            65
        )

        if result["verdict"] != "WAIT":

            result["warning"] = (
                "Data quality is FAIR; confidence reduced."
            )

    # -----------------------------------------------------
    # ACCELERATION BOOST
    # -----------------------------------------------------

    if (
        result["verdict"] == "UP"
        and
        acceleration == "ACCELERATING UP"
    ):

        result["confidence"] += 3

    if (
        result["verdict"] == "DOWN"
        and
        acceleration == "ACCELERATING DOWN"
    ):

        result["confidence"] += 3

    result["confidence"] = min(
        95,
        result["confidence"]
    )

    result["bullish"] = bullish

    result["bearish"] = bearish

    result["score"] = (
        bullish - bearish
    )

    result["agreement"] = (
        f"{bullish} BULLISH / "
        f"{bearish} BEARISH"
    )

    # -----------------------------------------------------
    # REASONS
    # -----------------------------------------------------

    if result["verdict"] == "UP":

        reasons = (
            bullish_reasons[:]
        )

    elif result["verdict"] == "DOWN":

        reasons = (
            bearish_reasons[:]
        )

    else:

        reasons = (
            bullish_reasons[:3]
            +
            bearish_reasons[:3]
        )

        if not reasons:

            reasons = (
                neutral_reasons[:3]
            )

    reasons.append(
        f"Momentum state: {acceleration}."
    )

    reasons.append(
        f"Reversal risk: {reversal}."
    )

    result["reasons"] = (
        reasons[:8]
    )

    return result


# =========================================================
# COLLECT EVERYTHING
# =========================================================

def collect_state():

    btc, feeds = (
        get_spot_feeds()
    )

    candles = (
        get_history()
    )

    market = (
        get_kalshi()
    )

    btc_quality = (
        calculate_feed_quality(
            feeds
        )
    )

    kalshi_quality = (
        calculate_kalshi_quality(
            market
        )
    )

    quality_score, quality_grade = (
        overall_quality(
            btc_quality,
            kalshi_quality,
            len(candles)
        )
    )

    signal = build_signal(
        btc,
        market,
        candles,
        quality_score
    )

    target = (
        market.get(
            "target"
        )
        if market
        else None
    )

    distance = None
    distance_pct = None

    if (
        btc is not None
        and
        target is not None
    ):

        distance = (
            btc - target
        )

        distance_pct = (
            distance
            /
            target
        ) * 100

    mid = yes_mid(
        market
    )

    # -----------------------------------------------------
    # UPDATE MEMORY
    # -----------------------------------------------------

    if (
        market
        and
        btc is not None
    ):

        update_memory(
            market,
            btc,
            signal,
            distance_pct,
            mid
        )

    # -----------------------------------------------------
    # HISTORICAL MATCH
    # -----------------------------------------------------

    current_memory = None

    if (
        signal.get(
            "verdict"
        )
        in (
            "UP",
            "DOWN"
        )
        and
        market
    ):

        current_record = {

            "direction":
                signal["verdict"],

            "m1":
                signal.get(
                    "m1"
                ),

            "m5":
                signal.get(
                    "m5"
                ),

            "m15":
                signal.get(
                    "m15"
                ),

            "distance_pct":
                distance_pct,

            "yes_mid":
                mid,

            "structure":
                signal.get(
                    "structure"
                )
        }

        current_memory = (
            historical_match(
                current_record
            )
        )

        # -------------------------------------------------
        # MEMORY SUPPORT
        # -------------------------------------------------

        if (
            current_memory
            and
            current_memory.get(
                "matches",
                0
            ) >= 3
            and
            current_memory.get(
                "rate"
            ) is not None
        ):

            direction = (
                signal["verdict"]
            )

            rate = (
                current_memory[
                    "rate"
                ]
            )

            if (
                current_memory.get(
                    "direction"
                )
                == direction
            ):

                if rate >= 70:

                    signal["confidence"] = min(
                        97,
                        signal["confidence"]
                        + 5
                    )

                    signal["reasons"].append(
                        "Memory match: "
                        f"{current_memory['wins']}/"
                        f"{current_memory['matches']} "
                        f"similar setups finished "
                        f"{direction}."
                    )

                elif rate <= 40:

                    signal["confidence"] = max(
                        45,
                        signal["confidence"]
                        - 8
                    )

                    signal["warning"] = (
                        "Historical memory conflicts "
                        "with the current direction."
                    )

                    signal["reasons"].append(
                        "Memory warning: "
                        f"only "
                        f"{current_memory['wins']}/"
                        f"{current_memory['matches']} "
                        f"similar setups finished "
                        f"{direction}."
                    )

    # -----------------------------------------------------
    # MEMORY STATS
    # -----------------------------------------------------

    memory_stats = {

        "records":
            len(signal_memory),

        "resolved":
            sum(
                1
                for record
                in signal_memory
                if record.get(
                    "outcome"
                )
                in (
                    "UP",
                    "DOWN"
                )
            ),

        "up":
            sum(
                1
                for record
                in signal_memory
                if record.get(
                    "outcome"
                ) == "UP"
            ),

        "down":
            sum(
                1
                for record
                in signal_memory
                if record.get(
                    "outcome"
                ) == "DOWN"
            ),

        "match":
            current_memory
    }

    return {

        "ok":
            btc is not None,

        "updated":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "btc":
            (
                round(
                    btc,
                    2
                )
                if btc is not None
                else None
            ),

        "feeds": {
            key:
                (
                    round(
                        value,
                        2
                    )
                    if value is not None
                    else None
                )
            for key, value
            in feeds.items()
        },

        "feed_count":
            btc_quality["live"],

        "feed_quality":
            btc_quality,

        "history_points":
            len(candles),

        "data_quality": {

            "score":
                quality_score,

            "grade":
                quality_grade
        },

        "memory":
            memory_stats,

        "kalshi_quality":
            kalshi_quality,

        "kalshi": {

            "ticker":
                (
                    market.get(
                        "ticker"
                    )
                    if market
                    else None
                ),

            "target":
                (
                    round(
                        target,
                        2
                    )
                    if target is not None
                    else None
                ),

            "yes_bid":
                (
                    market.get(
                        "yes_bid"
                    )
                    if market
                    else None
                ),

            "yes_ask":
                (
                    market.get(
                        "yes_ask"
                    )
                    if market
                    else None
                ),

            "yes_mid":
                mid,

            "last":
                (
                    market.get(
                        "last"
                    )
                    if market
                    else None
                ),

            "close_time":
                (
                    market.get(
                        "close_time"
                    )
                    if market
                    else None
                ),

            "countdown":
                (
                    seconds_left(
                        market.get(
                            "close_time"
                        )
                    )
                    if market
                    else None
                )
        },

        "distance": {

            "dollars":
                (
                    round(
                        distance,
                        2
                    )
                    if distance is not None
                    else None
                ),

            "percent":
                (
                    round(
                        distance_pct,
                        4
                    )
                    if distance_pct is not None
                    else None
                )
        },

        "momentum": {

            "m1":
                (
                    round(
                        signal["m1"],
                        4
                    )
                    if signal["m1"] is not None
                    else None
                ),

            "m5":
                (
                    round(
                        signal["m5"],
                        4
                    )
                    if signal["m5"] is not None
                    else None
                ),

            "m15":
                (
                    round(
                        signal["m15"],
                        4
                    )
                    if signal["m15"] is not None
                    else None
                )
        },

        "signal":
            signal
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
        now - cache["time"]
        < CACHE_SECONDS
    ):

        return jsonify(
            cache["state"]
        )

    state = collect_state()

    cache["state"] = state
    cache["time"] = now

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

<title>
BTC Strike AI
</title>

<style>

*{
 box-sizing:border-box
}

body{
 margin:0;
 background:#070a0f;
 color:#f4f7fb;
 font-family:Arial,sans-serif
}

.wrap{
 max-width:1100px;
 margin:auto;
 padding:18px
}

.header{
 display:flex;
 justify-content:space-between;
 align-items:center;
 margin-bottom:15px
}

h1{
 margin:0;
 font-size:25px
}

.sub{
 color:#8994a5;
 font-size:12px;
 margin-top:4px
}

.status{
 padding:8px 12px;
 border-radius:20px;
 background:#111822;
 font-size:12px
}

.dot{
 display:inline-block;
 width:8px;
 height:8px;
 border-radius:50%;
 background:#20d879;
 margin-right:6px
}

.verdict{
 padding:24px;
 text-align:center;
 border-radius:18px;
 background:#211b0b;
 border:1px solid #e8b84d;
 margin-bottom:14px
}

.verdict.up{
 background:#092018;
 border-color:#18d77a
}

.verdict.down{
 background:#250c11;
 border-color:#ff4d5d
}

.verdict.wait{
 background:#211b0b;
 border-color:#e8b84d
}

.verdict-label{
 font-size:39px;
 font-weight:900
}

.conf{
 margin-top:7px;
 color:#aeb8c8
}

.agreement{
 margin-top:6px;
 font-size:16px;
 font-weight:800
}

.warning{
 margin-top:7px;
 color:#ffcf5a;
 font-size:12px;
 font-weight:700
}

.grid{
 display:grid;
 grid-template-columns:repeat(4,1fr);
 gap:12px
}

.card{
 background:#10151e;
 border:1px solid #202a38;
 border-radius:14px;
 padding:15px
}

.card h3{
 margin:0 0 8px;
 color:#8994a5;
 font-size:12px;
 text-transform:uppercase
}

.big{
 font-size:25px;
 font-weight:800
}

.value{
 font-size:17px;
 font-weight:700
}

.green{
 color:#20d879
}

.red{
 color:#ff5262
}

.yellow{
 color:#e8b84d
}

.small{
 color:#7f8998;
 font-size:11px;
 margin-top:5px
}

.wide{
 grid-column:span 2
}

.quality-row{
 display:flex;
 align-items:center;
 gap:15px
}

.quality-score{
 font-size:28px;
 font-weight:900
}

.bar{
 height:7px;
 background:#222b37;
 border-radius:10px;
 overflow:hidden;
 margin-top:10px
}

.bar-fill{
 height:100%;
 width:0%;
 background:#20d879;
 transition:.3s
}

.memory-big{
 font-size:27px;
 font-weight:900
}

.memory-text{
 color:#c8d0dc;
 font-size:13px;
 line-height:1.6
}

.reasons{
 margin:14px 0 0;
 padding-left:20px;
 color:#c8d0dc;
 font-size:13px;
 line-height:1.7
}

.note{
 color:#697585;
 font-size:10px;
 margin-top:6px
}

@media(max-width:800px){

 .grid{
  grid-template-columns:repeat(2,1fr)
 }

 .wide{
  grid-column:span 2
 }

}

@media(max-width:500px){

 .wrap{
  padding:10px
 }

 .verdict-label{
  font-size:28px
 }

 .big{
  font-size:21px
 }

}

</style>

</head>

<body>

<div class="wrap">


<div class="header">

<div>

<h1>
₿ BTC STRIKE AI
</h1>

<div class="sub">
15-minute adaptive confirmation engine
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

<div
 id="agreement"
 class="agreement"
>
--
</div>

<div
 id="warning"
 class="warning"
>
</div>

</div>


<div class="grid">


<div class="card">

<h3>
BTC Reference
</h3>

<div
 id="btc"
 class="big"
>
--
</div>

<div
 id="feedCount"
 class="small"
>
-- feeds
</div>

</div>


<div class="card">

<h3>
Kalshi Target
</h3>

<div
 id="target"
 class="big"
>
--
</div>

<div
 id="ticker"
 class="small"
>
--
</div>

</div>


<div class="card">

<h3>
BTC vs Target
</h3>

<div
 id="distance"
 class="big"
>
--
</div>

<div
 id="distancePct"
 class="small"
>
--
</div>

</div>


<div class="card">

<h3>
Countdown
</h3>

<div
 id="countdown"
 class="big"
>
--
</div>

<div class="small">
until market close
</div>

</div>


<div class="card">

<h3>
1 Minute
</h3>

<div
 id="m1"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>
5 Minutes
</h3>

<div
 id="m5"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>
15 Minutes
</h3>

<div
 id="m15"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>
Structure
</h3>

<div
 id="structure"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>
Momentum State
</h3>

<div
 id="acceleration"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>
Reversal Risk
</h3>

<div
 id="reversal"
 class="value"
>
--
</div>

</div>


<div class="card">

<h3>
Kalshi YES
</h3>

<div
 id="yesMid"
 class="value"
>
--
</div>

<div
 id="kalshiChange"
 class="small"
>
--
</div>

</div>


<div class="card">

<h3>
Signal Score
</h3>

<div
 id="score"
 class="value"
>
--
</div>

</div>


<div class="card wide">

<h3>
DATA QUALITY BRAIN
</h3>

<div class="quality-row">

<div
 id="qualityScore"
 class="quality-score"
>
--
</div>

<div>

<div id="qualityGrade">
--
</div>

<div class="small">
Feeds + freshness + exchange agreement + Kalshi + history
</div>

</div>

</div>

<div class="bar">

<div
 id="qualityBar"
 class="bar-fill"
>
</div>

</div>

<div
 id="qualityDetails"
 class="small"
>
--
</div>

</div>


<div class="card wide">

<h3>
SIGNAL MEMORY 🧠
</h3>

<div class="memory-text">

<div>

<span
 id="memoryRecords"
 class="memory-big"
>
0
</span>

resolved setups stored

</div>

<div
 id="memoryMatch"
 style="margin-top:8px"
>
Building pattern history...
</div>

<div
 id="memoryStats"
 class="small"
>
The engine compares the current setup with previous completed markets.
</div>

</div>

</div>


<div class="card wide">

<h3>
WHY THE ENGINE CHOSE THIS
</h3>

<ul
 id="reasons"
 class="reasons"
>

<li>
Waiting for live data...
</li>

</ul>

</div>


<div class="card wide">

<h3>
FEED HEALTH
</h3>

<div
 id="feeds"
 class="small"
>
--
</div>

<div class="note">
BTC Reference is an exchange composite and is not the official CF Benchmarks BRTI.
</div>

</div>


</div>

</div>


<script>

function money(v){

 if(v==null)
  return "--";

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


function pct(v){

 if(v==null)
  return "--";

 return (
  v>=0 ? "+" : ""
 )
 +
 Number(v).toFixed(3)
 +
 "%";

}


function color(v){

 if(v==null)
  return "";

 if(v>0)
  return "green";

 if(v<0)
  return "red";

 return "yellow";

}


function setValue(
 id,
 text,
 cls
){

 const e =
  document.getElementById(
   id
  );

 e.textContent =
  text;

 e.className =
  "value "
  +
  (cls || "");

}


function clock(
 seconds
){

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
  +
  Date.now(),
  {
   cache:"no-store"
  }
 )

 .then(
  r => {

   if(!r.ok)

    throw new Error(
     "API "
     +
     r.status
    );

   return r.json();

  }
 )

 .then(
  d => {

   document.getElementById(
    "status"
   ).textContent =
    d.ok
     ?
      "LIVE"
     :
      "DATA ERROR";


   document.getElementById(
    "btc"
   ).textContent =
    money(
     d.btc
    );


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
    ||
    "--";


   document.getElementById(
    "feedCount"
   ).textContent =
    (
     d.feed_count
     ||
     0
    )
    +
    " feeds live";


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
    color(
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
    pct(
     d.momentum.m1
    ),
    color(
     d.momentum.m1
    )
   );


   setValue(
    "m5",
    pct(
     d.momentum.m5
    ),
    color(
     d.momentum.m5
    )
   );


   setValue(
    "m15",
    pct(
     d.momentum.m15
    ),
    color(
     d.momentum.m15
    )
   );


   setValue(
    "structure",
    d.signal.structure
    ||
    "--",
    ""
   );


   let accelerationClass =
    "yellow";


   if(
    d.signal.acceleration
    &&
    d.signal.acceleration.includes(
     "UP"
    )
   ){

    accelerationClass =
     "green";

   }


   if(
    d.signal.acceleration
    &&
    d.signal.acceleration.includes(
     "DOWN"
    )
   ){

    accelerationClass =
     "red";

   }


   setValue(
    "acceleration",
    d.signal.acceleration
    ||
    "--",
    accelerationClass
   );


   let reversalClass =
    "yellow";


   if(
    d.signal.reversal
    &&
    d.signal.reversal.includes(
     "BULLISH"
    )
   ){

    reversalClass =
     "green";

   }


   if(
    d.signal.reversal
    &&
    d.signal.reversal.includes(
     "BEARISH"
    )
   ){

    reversalClass =
     "red";

   }


   setValue(
    "reversal",
    d.signal.reversal
    ||
    "--",
    reversalClass
   );


   const mid =
    d.kalshi.yes_mid;


   setValue(

    "yesMid",

    mid == null
     ?
      "--"
     :
      (
       Number(mid)
       * 100
      ).toFixed(1)
      +
      "%",

    mid == null
     ?
      ""
     :
      mid >= 0.50
       ?
        "green"
       :
        "red"

   );


   document.getElementById(
    "kalshiChange"
   ).textContent =

    d.signal.kalshi_change
    == null

     ?

      "Building Kalshi history..."

     :

      "1m YES move: "
      +
      (
       d.signal.kalshi_change >= 0
        ?
         "+"
        :
         ""
      )
      +
      Number(
       d.signal.kalshi_change
      ).toFixed(1)
      +
      "¢";


   setValue(
    "score",

    d.signal.score == null
     ?
      "--"
     :
      Number(
       d.signal.score
      ).toFixed(0),

    color(
     d.signal.score
    )
   );


   const verdict =
    d.signal.verdict
    ||
    "WAIT";


   const box =
    document.getElementById(
     "verdict"
    );


   box.className =
    "verdict "
    +
    (
     verdict === "UP"
      ?
       "up"
      :
     verdict === "DOWN"
      ?
       "down"
      :
       "wait"
    );


   document.getElementById(
    "verdictLabel"
   ).textContent =

    d.signal.label
    ||
    (
     verdict === "UP"
      ?
       "🟢 UP"
      :
     verdict === "DOWN"
      ?
       "🔴 DOWN"
      :
       "🟡 WAIT"
    );


   document.getElementById(
    "confidence"
   ).textContent =
    (
     d.signal.confidence
     ||
     0
    )
    +
    "%";


   document.getElementById(
    "agreement"
   ).textContent =
    d.signal.agreement
    ||
    "--";


   document.getElementById(
    "warning"
   ).textContent =
    d.signal.warning
    ||
    "";


   const reasons =
    document.getElementById(
     "reasons"
    );


   reasons.innerHTML =
    "";


   (
    d.signal.reasons
    ||
    [
     "Waiting for live data..."
    ]
   ).forEach(
    reason => {

     const li =
      document.createElement(
       "li"
      );

     li.textContent =
      reason;

     reasons.appendChild(
      li
     );

    }
   );


   // ------------------------------------------------------
   // DATA QUALITY
   // ------------------------------------------------------

   const quality =
    d.data_quality
    ||
    {};

   const qualityScore =
    quality.score
    ||
    0;


   document.getElementById(
    "qualityScore"
   ).textContent =
    qualityScore
    +
    "/100";


   document.getElementById(
    "qualityGrade"
   ).textContent =
    "QUALITY: "
    +
    (
     quality.grade
     ||
     "--"
    );


   document.getElementById(
    "qualityBar"
   ).style.width =
    qualityScore
    +
    "%";


   const feedQuality =
    d.feed_quality
    ||
    {};


   document.getElementById(
    "qualityDetails"
   ).textContent =

    "BTC feeds: "
    +
    (
     feedQuality.live
     ||
     0
    )
    +
    "/4 live • "
    +
    (
     feedQuality.fresh
     ||
     0
    )
    +
    "/4 fresh • Exchange spread: "
    +
    (
     feedQuality.spread == null
      ?
       "n/a"
      :
       Number(
        feedQuality.spread
       ).toFixed(3)
       +
       "%"
    )
    +
    " • Kalshi: "
    +
    (
     d.kalshi_quality
     &&
     d.kalshi_quality.grade
      ?
       d.kalshi_quality.grade
      :
       "OFFLINE"
    )
    +
    " • History: "
    +
    (
     d.history_points
     ||
     0
    )
    +
    " candles";


   // ------------------------------------------------------
   // SIGNAL MEMORY
   // ------------------------------------------------------

   const memory =
    d.memory
    ||
    {};


   document.getElementById(
    "memoryRecords"
   ).textContent =
    memory.resolved
    ||
    0;


   const match =
    memory.match;


   if(
    match
    &&
    match.matches >= 3
    &&
    match.rate != null
   ){

    document.getElementById(
     "memoryMatch"
    ).textContent =

     "Current setup match: "
     +
     match.wins
     +
     "/"
     +
     match.matches
     +
     " similar setups finished "
     +
     (
      match.direction
      ||
      ""
     )
     +
     " ("
     +
     Number(
      match.rate
     ).toFixed(1)
     +
     "%).";

   }

   else{

    document.getElementById(
     "memoryMatch"
    ).textContent =
     "No strong historical match yet.";

   }


   document.getElementById(
    "memoryStats"
   ).textContent =

    "Stored outcomes: "
    +
    (
     memory.up
     ||
     0
    )
    +
    " UP • "
    +
    (
     memory.down
     ||
     0
    )
    +
    " DOWN • "
    +
    (
     memory.resolved
     ||
     0
    )
    +
    " resolved. Memory is supporting evidence, not a guarantee.";


   // ------------------------------------------------------
   // FEED HEALTH
   // ------------------------------------------------------

   document.getElementById(
    "feeds"
   ).textContent =

    Object.entries(
     d.feeds
     ||
     {}
    )
    .map(
     ([name,value]) =>

      name
      +
      ": "
      +
      (
       value == null
        ?
         "OFFLINE"
        :
         money(
          value
         )
      )
    )
    .join(
     "  •  "
    )
    +
    "  •  History: "
    +
    (
     d.history_points
     ||
     0
    )
    +
    " candles";

  }
 )

 .catch(
  error => {

   document.getElementById(
    "status"
   ).textContent =
    "API ERROR";


   document.getElementById(
    "warning"
   ).textContent =
    error.message;

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
# LOAD MEMORY
# =========================================================

load_memory()


# =========================================================
# LOCAL SERVER
# =========================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        ),
        threaded=True
    )
