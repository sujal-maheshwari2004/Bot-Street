from dataclasses import dataclass, field
from typing import Literal
from uuid import uuid4
from time import time

# ── Type aliases ──────────────────────────────────────────────────────────────
OrderSide   = Literal["buy", "sell"]
OrderType   = Literal["market", "limit"]
Sentiment   = Literal["bullish", "bearish", "neutral"]
HaltReason  = Literal["circuit_breaker", "manual"]
HaltStatus  = Literal["halted", "resumed"]

# ── Orders ────────────────────────────────────────────────────────────────────
@dataclass
class Order:
    """
    Placed by any participant (user or bot).
    Market orders set price=None and fill immediately at best available.
    Limit orders sit in the book until matched or expired.
    """
    client_id  : str
    symbol     : str
    side       : OrderSide
    order_type : OrderType
    quantity   : int
    price      : float | None        # None for market orders
    order_id   : str   = field(default_factory=lambda: str(uuid4()))
    timestamp  : float = field(default_factory=time)
    expires_at : float = field(default_factory=lambda: time() + 60)

# ── Trades ────────────────────────────────────────────────────────────────────
@dataclass
class Trade:
    """
    Emitted by the matching engine when two orders cross.
    A single order may produce multiple trades (partial fills).
    """
    symbol     : str
    buyer_id   : str
    seller_id  : str
    quantity   : int
    price      : float
    buy_order_id  : str
    sell_order_id : str
    trade_id   : str   = field(default_factory=lambda: str(uuid4()))
    timestamp  : float = field(default_factory=time)

# ── Price update ──────────────────────────────────────────────────────────────
@dataclass
class PriceUpdate:
    """
    Published by the price feed after every trade.
    Contains current market snapshot including quant indicators.
    """
    symbol     : str
    price      : float          # last traded price
    vwap       : float          # volume-weighted average price
    bid        : float          # best bid in order book
    ask        : float          # best ask in order book
    spread     : float          # ask - bid
    volume     : int            # cumulative volume
    rsi        : float | None   # RSI(14) — None until enough trades
    macd       : float | None   # MACD line
    macd_signal: float | None   # MACD signal line
    bb_upper   : float | None   # Bollinger upper band
    bb_lower   : float | None   # Bollinger lower band
    ema_short  : float | None   # EMA(9)
    ema_long   : float | None   # EMA(21)
    ofi        : float | None   # order flow imbalance [-1, +1]
    timestamp  : float = field(default_factory=time)

# ── Sentiment ─────────────────────────────────────────────────────────────────
@dataclass
class SentimentUpdate:
    """
    Published by the sentiment engine on every trade.
    Bots consume this to adjust their aggression and direction.
    """
    symbol     : str
    sentiment  : Sentiment
    strength   : float          # 0.0 → 1.0
    buy_ratio  : float          # rolling buy / (buy + sell)
    trade_velocity : float      # trades per second in window
    timestamp  : float = field(default_factory=time)

# ── Portfolio ─────────────────────────────────────────────────────────────────
@dataclass
class PortfolioSnapshot:
    """
    Published by the portfolio ledger periodically and on every trade.
    Tracks holdings, cash, P&L and risk metrics per client.
    """
    client_id     : str
    holdings      : dict        # symbol → quantity
    avg_cost      : dict        # symbol → average buy price
    cash          : float
    realised_pnl  : float       # closed position P&L
    unrealised_pnl: float       # open position P&L at current prices
    sharpe        : float | None
    max_drawdown  : float | None
    var_95        : float | None # Value at Risk 95%
    timestamp     : float = field(default_factory=time)

# ── Candles ───────────────────────────────────────────────────────────────────
@dataclass
class Candle:
    """
    OHLCV bucket published by the candle aggregator every N seconds.
    Foundation for candlestick chart in dashboard.
    """
    symbol     : str
    open       : float
    high       : float
    low        : float
    close      : float
    volume     : int
    vwap       : float          # VWAP within this candle
    trade_count: int            # number of trades in window
    interval_s : int            # bucket size in seconds
    open_time  : float          # bucket start timestamp
    close_time : float          # bucket end timestamp

# ── Circuit breaker ───────────────────────────────────────────────────────────
@dataclass
class MarketHaltEvent:
    """
    Published when a circuit breaker triggers or market resumes.
    All producers must check halt status before sending orders.
    """
    symbol     : str
    status     : HaltStatus
    reason     : HaltReason | None
    price_before  : float | None   # price that triggered halt
    price_trigger : float | None   # price at halt
    resume_at     : float | None   # estimated resume timestamp
    timestamp  : float = field(default_factory=time)

# ── Order expiry ──────────────────────────────────────────────────────────────
@dataclass
class OrderExpired:
    """
    Published when a limit order is purged from the book due to TTL.
    """
    order_id   : str
    client_id  : str
    symbol     : str
    side       : OrderSide
    quantity   : int
    price      : float
    placed_at  : float
    expired_at : float = field(default_factory=time)