from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from models import Base
from sqlalchemy import text
DATABASE_URL = "sqlite+aiosqlite:///./stocks.db"

engine = create_async_engine(DATABASE_URL, echo=False)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session



SEED_STOCKS = [
    {"symbol": "AAPL",  "name": "Apple Inc.",       "current_price": 182.0},
    {"symbol": "GOOGL", "name": "Alphabet Inc.",     "current_price": 140.0},
    {"symbol": "MSFT",  "name": "Microsoft Corp.",   "current_price": 375.0},
    {"symbol": "TSLA",  "name": "Tesla Inc.",        "current_price": 245.0},
    {"symbol": "AMZN",  "name": "Amazon.com Inc.",   "current_price": 178.0},
]

async def seed_stocks():
    """Insert stocks only if table is empty."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            result = await session.execute(text("SELECT COUNT(*) FROM stocks"))
            count = result.scalar()
            if count == 0:
                from models import Stock
                for s in SEED_STOCKS:
                    session.add(Stock(**s))
                print("[db] stocks seeded")
            else:
                print("[db] stocks already seeded, skipping")