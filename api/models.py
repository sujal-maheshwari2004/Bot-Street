"""
API Models — Pydantic request/response schemas for FastAPI

Separate from core/schemas.py (which are Kafka message dataclasses).
These models define what the REST API accepts and returns.
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from time import time


# ── Request models ────────────────────────────────────────────────────────────

class PlaceOrderRequest(BaseModel):
    symbol     : str
    side       : Literal["buy", "sell"]
    order_type : Literal["market", "limit"] = "limit"
    quantity   : int   = Field(gt=0)
    price      : Optional[float] = Field(default=None, gt=0)
    client_id  : str   = "user"

    model_config = {"json_schema_extra": {"example": {
        "symbol": "PEAR", "side": "buy",
        "order_type": "limit", "quantity": 10, "price": 151.00
    }}}


# ── Response models ───────────────────────────────────────────────────────────

class OrderResponse(BaseModel):
    order_id   : str
    client_id  : str
    symbol     : str
    side       : str
    order_type : str
    quantity   : int
    price      : Optional[float]
    timestamp  : float
    expires_at : float
    status     : str = "accepted"


class PriceResponse(BaseModel):
    symbol     : str
    price      : float
    vwap       : Optional[float]
    bid        : Optional[float]
    ask        : Optional[float]
    spread     : Optional[float]
    volume     : int
    rsi        : Optional[float]
    macd       : Optional[float]
    macd_signal: Optional[float]
    bb_upper   : Optional[float]
    bb_lower   : Optional[float]
    ema_short  : Optional[float]
    ema_long   : Optional[float]
    ofi        : Optional[float]
    timestamp  : float


class OrderBookLevel(BaseModel):
    price    : float
    quantity : int


class OrderBookResponse(BaseModel):
    symbol : str
    bids   : list[OrderBookLevel]
    asks   : list[OrderBookLevel]
    spread : Optional[float]
    mid    : Optional[float]


class CandleResponse(BaseModel):
    symbol      : str
    open        : float
    high        : float
    low         : float
    close       : float
    volume      : int
    vwap        : float
    trade_count : int
    interval_s  : int
    open_time   : float
    close_time  : float


class SentimentResponse(BaseModel):
    symbol         : str
    sentiment      : str
    strength       : float
    buy_ratio      : float
    trade_velocity : float
    timestamp      : float


class HoldingResponse(BaseModel):
    symbol      : str
    quantity    : int
    avg_cost    : float
    curr_price  : Optional[float]
    unrealised  : Optional[float]


class PortfolioResponse(BaseModel):
    client_id      : str
    holdings       : list[HoldingResponse]
    cash           : float
    realised_pnl   : float
    unrealised_pnl : float
    sharpe         : Optional[float]
    max_drawdown   : Optional[float]
    var_95         : Optional[float]
    trade_count    : int


class LeaderboardEntry(BaseModel):
    rank          : int
    client_id     : str
    total_pnl     : float
    realised_pnl  : float
    unrealised_pnl: float
    cash          : float
    sharpe        : Optional[float]
    max_drawdown  : Optional[float]
    trade_count   : int


class MarketStatusResponse(BaseModel):
    symbol  : str
    halted  : bool
    name    : str
    price   : Optional[float]
    profile : str


class HealthResponse(BaseModel):
    status  : str
    kafka   : str
    topics  : list[str]
    symbols : list[str]