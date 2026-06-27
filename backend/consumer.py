import asyncio
import json
import redis.asyncio as aioredis
from sqlalchemy import select, update
from datetime import datetime
from database import AsyncSessionLocal
from models import Stock, TickHistory

# price alert thresholds — symbol: target price
ALERTS = {
    "AAPL":  190.0,
    "GOOGL": 145.0,
    "MSFT":  385.0,
    "TSLA":  255.0,
    "AMZN":  180.0,
}

# in-memory set of active WebSocket connections (filled by api.py)
active_connections: set = set()

async def broadcast(message: dict):
    """Push tick to all connected WebSocket clients."""
    dead = set()
    for ws in active_connections:
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    active_connections.difference_update(dead)

async def check_alerts(redis_client: aioredis.Redis, tick: dict):
    """Publish to Redis Pub/Sub if price crosses threshold."""
    symbol = tick["symbol"]
    price  = tick["price"]
    target = ALERTS.get(symbol)

    if target and price >= target:
        alert = {
            "type":    "alert",
            "symbol":  symbol,
            "price":   price,
            "target":  target,
            "message": f"{symbol} crossed target {target}!"
        }
        await redis_client.publish(f"alerts:{symbol}", json.dumps(alert))

async def write_tick(tick: dict):
    """Write tick to SQLite — update current price + insert history row."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # update current price on Stock table
            await session.execute(
                update(Stock)
                .where(Stock.symbol == tick["symbol"])
                .values(current_price=tick["price"])
            )

            # insert into tick_history
            session.add(TickHistory(
                stock_id  = await get_stock_id(session, tick["symbol"]),
                price     = tick["price"],
                sma_20    = tick["sma_20"],
                timestamp = datetime.fromisoformat(tick["timestamp"]),
            ))

async def get_stock_id(session, symbol: str) -> int:
    """Fetch stock id by symbol."""
    result = await session.execute(
        select(Stock.id).where(Stock.symbol == symbol)
    )
    return result.scalar_one()

async def run_consumer(redis_client: aioredis.Redis):
    """Drains Redis list, writes to DB, checks alerts, broadcasts to WebSocket."""
    print("[consumer] started")

    while True:
        # BRPOP blocks until a tick is available — no busy waiting
        data = await redis_client.brpop("ticks", timeout=2)

        if data is None:
            continue

        tick = json.loads(data[1])

        await write_tick(tick)
        await check_alerts(redis_client, tick)
        await broadcast(tick)