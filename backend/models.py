from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Index
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class Stock(Base):
    __tablename__ = "stocks"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    symbol     = Column(String(10), unique=True, nullable=False)  # e.g. "AAPL"
    name       = Column(String(100), nullable=False)              # e.g. "Apple Inc."
    current_price = Column(Float, default=0.0)

    ticks      = relationship("TickHistory", back_populates="stock")
    holdings   = relationship("Portfolio", back_populates="stock")


class TickHistory(Base):
    __tablename__ = "tick_history"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    stock_id   = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    price      = Column(Float, nullable=False)
    sma_20     = Column(Float, nullable=True)
    timestamp  = Column(DateTime, default=datetime.utcnow)

    stock      = relationship("Stock", back_populates="ticks")

    __table_args__ = (
        Index("ix_tick_stock_time", "stock_id", "timestamp"),
    )


class Portfolio(Base):
    __tablename__ = "portfolio"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    stock_id   = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    quantity   = Column(Integer, nullable=False)
    buy_price  = Column(Float, nullable=False)

    stock      = relationship("Stock", back_populates="holdings")