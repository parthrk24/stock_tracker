import asyncio
import json
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta

from database import get_db, create_tables, seed_stocks
from models import Stock, TickHistory, Portfolio
from producer import run_producer
from consumer import run_consumer, active_connections

app = FastAPI(title="Stock Portfolio Tracker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client: aioredis.Redis = None


# startup / shutdown

@app.on_event("startup")
async def startup():
    global redis_client
    await create_tables()
    await seed_stocks()
    redis_client = await aioredis.from_url("redis://localhost:6379")
    asyncio.create_task(run_producer(redis_client))
    asyncio.create_task(run_consumer(redis_client))
    print("[api] server ready")

@app.on_event("shutdown")
async def shutdown():
    await redis_client.aclose()


# REST endpoints 

@app.get("/stocks")
async def get_stocks(db: AsyncSession = Depends(get_db)):
    """All stocks with current price — reads from SQLite."""
    result = await db.execute(select(Stock))
    stocks = result.scalars().all()
    return [
        {
            "symbol":        s.symbol,
            "name":          s.name,
            "current_price": s.current_price,
        }
        for s in stocks
    ]

@app.get("/stocks/{symbol}/history")
async def get_history(
    symbol: str,
    minutes: int = 60,
    db: AsyncSession = Depends(get_db)
):
    """Price history for a stock within the last N minutes."""
    since = datetime.utcnow() - timedelta(minutes=minutes)

    result = await db.execute(
        select(Stock).where(Stock.symbol == symbol.upper())
    )
    stock = result.scalar_one_or_none()
    if not stock:
        return {"error": "stock not found"}

    ticks = await db.execute(
        select(TickHistory)
        .where(
            TickHistory.stock_id  == stock.id,
            TickHistory.timestamp >= since
        )
        .order_by(desc(TickHistory.timestamp))
        .limit(200)
    )
    rows = ticks.scalars().all()
    return [
        {
            "price":     r.price,
            "sma_20":    r.sma_20,
            "timestamp": r.timestamp.isoformat(),
        }
        for r in rows
    ]

@app.get("/portfolio")
async def get_portfolio(db: AsyncSession = Depends(get_db)):
    """Holdings with live P&L computed on the fly."""
    result = await db.execute(
        select(Portfolio, Stock)
        .join(Stock, Portfolio.stock_id == Stock.id)
    )
    rows = result.all()
    return [
        {
            "symbol":        stock.symbol,
            "name":          stock.name,
            "quantity":      holding.quantity,
            "buy_price":     holding.buy_price,
            "current_price": stock.current_price,
            "pnl":           round(
                                (stock.current_price - holding.buy_price)
                                * holding.quantity, 2
                              ),
        }
        for holding, stock in rows
    ]

@app.get("/prices/live")
async def get_live_prices():
    """Reads all current prices directly from Redis — fastest possible."""
    prices = await redis_client.hgetall("prices")
    return {k.decode(): float(v) for k, v in prices.items()}


# WebSocket

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    active_connections.add(ws)
    print(f"[ws] client connected — total: {len(active_connections)}")
    try:
        while True:
            await ws.receive_text()   # keep connection alive
    except WebSocketDisconnect:
        active_connections.discard(ws)
        print(f"[ws] client disconnected — total: {len(active_connections)}")