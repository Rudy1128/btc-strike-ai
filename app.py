def get_kalshi():
    now = time.time()

    # Use cached result briefly to avoid unnecessary API calls.
    if now - kalshi_cache["time"] < KALSHI_REFRESH_SECONDS:
        return kalshi_cache["market"]

    # ---------------------------------------------------------
    # 1. If a specific ticker was manually supplied, use it.
    # ---------------------------------------------------------
    if KALSHI_TICKER:
        for base in KALSHI_BASES:
            data = get_json(
                f"{base}/markets/{KALSHI_TICKER}"
            )

            if isinstance(data, dict):
                market = normalize_market(
                    data.get("market", data)
                )

                if market:
                    kalshi_cache["time"] = now
                    kalshi_cache["market"] = market
                    return market

    # ---------------------------------------------------------
    # 2. IMPORTANT:
    # Ask Kalshi directly for the BTC 15-minute series.
    #
    # KXBTC15M = BTC 15-minute markets
    # ---------------------------------------------------------
    params = {
        "series_ticker": "KXBTC15M",
        "status": "open",
        "limit": 100,
        "mve_filter": "exclude",
    }

    for base in KALSHI_BASES:

        data = get_json(
            f"{base}/markets",
            params
        )

        if not isinstance(data, dict):
            continue

        markets = data.get(
            "markets",
            []
        )

        candidates = []

        for market in markets:

            ticker = str(
                market.get(
                    "ticker",
                    ""
                )
            ).upper()

            # Only accept the actual BTC 15-minute series.
            if not ticker.startswith("KXBTC15M"):
                continue

            close_time = (
                market.get("close_time")
                or
                market.get("expiration_time")
            )

            close = parse_time(
                close_time
            )

            if not close:
                continue

            # Ignore markets that have already closed.
            if close.timestamp() <= now:
                continue

            candidates.append(
                market
            )

        # -----------------------------------------------------
        # 3. Choose the nearest upcoming KXBTC15M market.
        # -----------------------------------------------------
        if candidates:

            candidates.sort(
                key=lambda market: (
                    parse_time(
                        market.get("close_time")
                        or
                        market.get("expiration_time")
                    ).timestamp()
                )
            )

            selected = normalize_market(
                candidates[0]
            )

            if selected:

                kalshi_cache["time"] = now
                kalshi_cache["market"] = selected

                return selected

    # ---------------------------------------------------------
    # Nothing found.
    # ---------------------------------------------------------
    kalshi_cache["time"] = now
    kalshi_cache["market"] = None

    return None
