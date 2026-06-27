import asyncio
import json
import random
from collections import deque
from datetime import datetime
import redis.asyncio as aioredis

STOCKS = {
    "AAPL": {"name": "Apple Inc.",      "price": 182.0},
    "GOOGL": {"name": "Alphabet Inc.",  "price": 140.0},
    "MSFT": {"name": "Microsoft Corp.", "price": 375.0},
    "TSLA": {"name": "Tesla Inc.",      "price": 245.0},
    "AMZN": {"name": "Amazon.com Inc.", "price": 178.0},
}

# sliding window per stock for SMA-20
windows = {symbol: deque(maxlen=20) for symbol in STOCKS}

def next_price(last_price: float) -> float:
    """Random walk — Gaussian noise around last price."""
    return round(last_price * (1 + random.gauss(0, 0.002)), 2)

def calc_sma(window: deque) -> float | None:
    """SMA only meaningful once window is full."""
    if len(window) < 2:
        return None
    return round(sum(window) / len(window), 2)

async def run_producer(redis_client: aioredis.Redis):
    """Generates ticks every 0.5s and pushes to Redis list."""
    current_prices = {s: d["price"] for s, d in STOCKS.items()}

    print("[producer] started")

    while True:
        for symbol, last_price in current_prices.items():
            new_price = next_price(last_price)
            current_prices[symbol] = new_price

            windows[symbol].append(new_price)
            sma = calc_sma(windows[symbol])

            tick = {
                "symbol":    symbol,
                "price":     new_price,
                "sma_20":    sma,
                "timestamp": datetime.utcnow().isoformat(),
            }

            # push to Redis list — consumer will BRPOP from this
            await redis_client.lpush("ticks", json.dumps(tick))

            # cache latest price in Redis hash for instant API reads
            await redis_client.hset("prices", symbol, new_price)

        await asyncio.sleep(0.5)