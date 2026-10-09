def update_memory(market, price, signal, distance_pct):
    """
    Track one prediction per 15-minute market.

    Resolve and save the previous market before starting
    a new one, so market rollovers do not skip scoring.
    """
    global active_market

    load_memory()

    now = time.time()

    ticker = market.get("ticker")

    close = parse_time(market.get("close_time"))
    close_ts = close.timestamp() if close else now + 900

    def resolve_active_market():
        """
        Score the previously tracked market using its last
        observed price. Save the result before clearing it.
        """
        global active_market

        if not active_market:
            return

        record = dict(active_market)

        target = record.get("target")
        final_price = record.get("last_price")
        direction = record.get("direction", "WAIT")

        # Do not invent an outcome without valid prices.
        if target is None or final_price is None:
            return

        try:
            target = float(target)
            final_price = float(final_price)
        except (TypeError, ValueError):
            return

        if target <= 0 or final_price <= 0:
            return

        if final_price > target:
            outcome = "UP"
        elif final_price < target:
            outcome = "DOWN"
        else:
            outcome = "PUSH"

        if direction in ("UP", "DOWN"):
            result = "WIN" if direction == outcome else "LOSS"
            scored = True
        else:
            result = "UNSCORED"
            scored = False

        record.update({
            "outcome": outcome,
            "result": result,
            "scored": scored,
            "resolved": datetime.now(timezone.utc).isoformat(),
            "scoring_price": final_price,
            "scoring_method": "last_observed_price",
        })

        # Avoid inserting the same market twice.
        already_saved = any(
            item.get("ticker") == record.get("ticker")
            for item in signal_memory
        )

        if not already_saved:
            signal_memory.append(record)
            signal_memory[:] = signal_memory[-500:]
            save_memory()

        # Clear only after successfully handling the record.
        active_market = None

    # If the previous market has ended, resolve it BEFORE
    # replacing it with the next market.
    if active_market is not None:
        old_ticker = active_market.get("ticker")
        old_close_ts = active_market.get("close_ts", 0)

        if old_ticker != ticker or now >= old_close_ts:
            resolve_active_market()

    # Start tracking the current market.
    if active_market is None:
        direction = signal.get("verdict", "WAIT")

        if direction not in ("UP", "DOWN"):
            direction = "WAIT"

        active_market = {
            "ticker": ticker,
            "close_ts": close_ts,
            "target": market.get("target"),
            "direction": direction,
            "confidence": (
                signal.get("confidence")
                if direction in ("UP", "DOWN")
                else None
            ),
            "signal_score": signal.get("score"),
            "prediction_locked": direction in ("UP", "DOWN"),
            "prediction_locked_at": (
                datetime.now(timezone.utc).isoformat()
                if direction in ("UP", "DOWN")
                else None
            ),
            "m1": signal.get("m1"),
            "m5": signal.get("m5"),
            "m15": signal.get("m15"),
            "distance": distance_pct,
            "structure": signal.get("structure"),
            "last_price": price,
        }

    else:
        # Keep the original prediction unchanged.
        # Update the last observed BTC price while the market runs.
        if price is not None:
            active_market["last_price"] = price

        # If the engine initially said WAIT, record its first
        # directional prediction for this market.
        if active_market.get("direction") == "WAIT":
            direction = signal.get("verdict")

            if direction in ("UP", "DOWN"):
                active_market.update({
                    "direction": direction,
                    "confidence": signal.get("confidence"),
                    "signal_score": signal.get("score"),
                    "prediction_locked": True,
                    "prediction_locked_at": (
                        datetime.now(timezone.utc).isoformat()
                    ),
                    "m1": signal.get("m1"),
                    "m5": signal.get("m5"),
                    "m15": signal.get("m15"),
                    "distance": distance_pct,
                    "structure": signal.get("structure"),
                })

        active_market["current_m1"] = signal.get("m1")
        active_market["current_m5"] = signal.get("m5")
        active_market["current_m15"] = signal.get("m15")
        active_market["current_distance"] = distance_pct
        active_market["current_structure"] = signal.get("structure")

    # Resolve a market that has reached its scheduled close.
    if active_market is not None and now >= active_market.get("close_ts", 0):
        resolve_active_market()
