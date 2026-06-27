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

# tracks whether a symbol is currently "armed" (hasn't fired since last
# dropping back below its target) so we alert once per crossing, not
# once per tick forever
_armed = {symbol: True for symbol in ALERTS}

async def broadcast(message: dict):
    """Push tick (or alert) to all connected WebSocket clients."""
    dead = set()
    for ws in active_connections:
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    active_connections.difference_update(dead)

async def check_alerts(redis_client: aioredis.Redis, tick: dict):
    """Publish to Redis Pub/Sub once per threshold crossing, re-arming
    once price drops back below target so it can fire again later."""
    symbol = tick["symbol"]
    price  = tick["price"]
    target = ALERTS.get(symbol)

    if target is None:
        return

    if price >= target and _armed.get(symbol, True):
        _armed[symbol] = False
        alert = {
            "type":    "alert",
            "symbol":  symbol,
            "price":   price,
            "target":  target,
            "message": f"{symbol} crossed target {target}!"
        }
        await redis_client.publish(f"alerts:{symbol}", json.dumps(alert))
    elif price < target:
        _armed[symbol] = True

async def run_alert_listener(redis_client: aioredis.Redis):
    """Subscribes to all alert channels and forwards them to connected
    WebSocket clients. Without this, alerts were published to Redis
    Pub/Sub but nothing ever consumed them — the frontend's alert feed
    would silently never fire."""
    pubsub = redis_client.pubsub()
    await pubsub.psubscribe("alerts:*")
    print("[alert-listener] started")

    async for message in pubsub.listen():
        if message["type"] != "pmessage":
            continue
        alert = json.loads(message["data"])
        await broadcast(alert)

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