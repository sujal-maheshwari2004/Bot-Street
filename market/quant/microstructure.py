"""
Market Microstructure — How Markets Actually Work at the Tick Level

Microstructure studies the mechanics of price formation:
how orders interact, how information gets into prices,
and what the cost of trading really is.

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Bid-Ask Spread         The cost of immediacy
  Order Flow Imbalance   Buy/sell pressure predicting price direction
  Market Impact          How your order moves the price
  Trade Arrival Rate     Market activity level
  Amihud Illiquidity     Price impact per unit of volume
  Roll Spread Estimator  Inferring spread from price changes
  Kyle's Lambda          Market depth / price impact coefficient
─────────────────────────────────────────────────────────────────────────────
"""

import math
from dataclasses import dataclass
from typing import Optional
from collections import deque
from time import time


# ── Output dataclasses ────────────────────────────────────────────────────────

@dataclass
class SpreadMetrics:
    """
    All spread-related measurements for a symbol at a point in time.

    The bid-ask spread is THE fundamental cost of trading.
    Every time you buy at the ask and sell at the bid,
    you pay the spread as a transaction cost.
    """
    bid              : Optional[float]   # best bid price
    ask              : Optional[float]   # best ask price
    absolute_spread  : Optional[float]   # ask - bid in dollars
    relative_spread  : Optional[float]   # spread / mid_price (%)
    mid_price        : Optional[float]   # (bid + ask) / 2
    roll_spread      : Optional[float]   # Roll's implied spread estimator


@dataclass
class FlowMetrics:
    """
    Order flow measurements — directional pressure in the market.

    Order flow is the single best short-term predictor of price direction.
    Academic research shows OFI predicts next-minute returns with
    statistical significance across virtually all liquid markets.
    """
    ofi              : float             # order flow imbalance [-1, +1]
    buy_volume       : int               # total buy volume in window
    sell_volume      : int               # total sell volume in window
    total_volume     : int               # total volume
    trade_count      : int               # number of trades in window
    arrival_rate     : Optional[float]   # trades per second


@dataclass
class ImpactMetrics:
    """
    Price impact measurements — how trading moves the market.

    Every trade moves the price slightly.
    Large orders move it more (market impact).
    Understanding impact helps size orders to minimize slippage.
    """
    amihud           : Optional[float]   # illiquidity ratio
    kyle_lambda      : Optional[float]   # price impact coefficient
    avg_trade_size   : Optional[float]   # average shares per trade
    price_volatility : Optional[float]   # short-term realized vol


@dataclass
class MicrostructureSnapshot:
    """Complete microstructure state for one symbol."""
    spread  : SpreadMetrics
    flow    : FlowMetrics
    impact  : ImpactMetrics
    timestamp : float


# ── Bid-Ask Spread ────────────────────────────────────────────────────────────

def compute_spread(bid: float, ask: float) -> SpreadMetrics:
    """
    Compute all spread metrics from current best bid and ask.

    Absolute spread = ask - bid
      Measures raw dollar cost to round-trip trade.
      Tight spread → liquid market (many competing quotes)
      Wide spread  → illiquid market (few quotes, hard to trade)

    Relative spread = (ask - bid) / mid_price
      Normalizes spread across different price levels.
      PEAR at $150 with $0.10 spread = 0.067% relative spread
      TSLA at $90 with $0.10 spread = 0.111% relative spread
      Same absolute spread, different relative cost.

    Market makers EARN the spread:
      They buy at bid ($150.00) and sell at ask ($150.10)
      Profit = $0.10 per share (the spread)
      This is why market_maker.py always posts both sides.
    """
    if bid <= 0 or ask <= 0 or ask < bid:
        return SpreadMetrics(None, None, None, None, None, None)

    mid = (bid + ask) / 2
    abs_spread = round(ask - bid, 4)
    rel_spread = round(abs_spread / mid, 6) if mid > 0 else None

    return SpreadMetrics(
        bid             = round(bid, 4),
        ask             = round(ask, 4),
        absolute_spread = abs_spread,
        relative_spread = rel_spread,
        mid_price       = round(mid, 4),
        roll_spread     = None,  # computed separately from price series
    )


def roll_spread_estimator(prices: list[float]) -> Optional[float]:
    """
    Roll's (1984) implied spread estimator.

    Estimates the bid-ask spread from transaction price changes alone,
    without needing to observe actual bid/ask quotes.

    Formula: spread = 2 * sqrt(-cov(delta_p_t, delta_p_t-1))

    Intuition:
      In a frictionless market, consecutive price changes are uncorrelated.
      But with a bid-ask spread, prices bounce between bid and ask,
      creating NEGATIVE serial correlation in price changes.
      The magnitude of that negative correlation reveals the spread.

    Real world use:
      Used to estimate historical spreads before electronic quotes
      were widely available (e.g. estimating 1980s market liquidity).
      Still used for illiquid assets where quotes are unreliable.

    Returns None if serial correlation is positive
    (positive cov means something else is driving prices).
    """
    if len(prices) < 10:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    if len(deltas) < 2:
        return None

    # compute covariance of consecutive price changes
    n = len(deltas) - 1
    mean_t  = sum(deltas[:-1]) / n
    mean_t1 = sum(deltas[1:])  / n

    cov = sum(
        (deltas[i] - mean_t) * (deltas[i + 1] - mean_t1)
        for i in range(n)
    ) / n

    if cov >= 0:
        return None  # positive covariance → Roll estimator undefined

    return round(2 * math.sqrt(-cov), 4)


# ── Order Flow Imbalance ──────────────────────────────────────────────────────

def order_flow_imbalance(buy_volume: int, sell_volume: int) -> float:
    """
    Order Flow Imbalance (OFI)

    OFI = (buy_volume - sell_volume) / (buy_volume + sell_volume)
    Range: -1.0 to +1.0

    +1.0 → pure buy pressure  → price likely to rise
    -1.0 → pure sell pressure → price likely to fall
     0.0 → balanced flow      → no directional pressure

    Academic basis:
      Chordia, Roll & Subrahmanyam (2002) showed OFI significantly
      predicts short-term returns across NYSE stocks.

    How bots use OFI:
      Market maker: if OFI > 0.5 → skew quotes higher (demand > supply)
      Momentum bot: if OFI > 0.6 → add to long position
    """
    total = buy_volume + sell_volume
    if total == 0:
        return 0.0
    return round((buy_volume - sell_volume) / total, 4)


# ── Market Impact ─────────────────────────────────────────────────────────────

def amihud_illiquidity(prices: list[float],
                       volumes: list[int],
                       period: int = 20) -> Optional[float]:
    """
    Amihud (2002) Illiquidity Ratio

    ILLIQ = mean(|return_t| / volume_t)

    Measures: how much does price move per dollar of volume traded?
    Higher ILLIQ → more illiquid → larger orders move price more.

    Interpretation:
      ILLIQ ≈ 0    → very liquid (like large-cap stocks)
      ILLIQ > 0.01 → illiquid (small-cap, thinly traded)

    Use in simulator:
      Market maker uses ILLIQ to set spread width:
        High ILLIQ → wide spread (compensate for adverse selection risk)
        Low ILLIQ  → tight spread (competitive liquid market)

    Adverse selection:
      If someone trades aggressively against you, they probably
      know something you don't. ILLIQ measures this risk.
    """
    if len(prices) < period + 1 or len(volumes) < period:
        return None

    ratios = []
    for i in range(1, min(period + 1, len(prices))):
        if volumes[i] > 0 and prices[i - 1] > 0:
            ret = abs(prices[i] - prices[i - 1]) / prices[i - 1]
            ratios.append(ret / volumes[i])

    if not ratios:
        return None

    return round(sum(ratios) / len(ratios), 8)


def kyle_lambda(price_changes: list[float],
                signed_volumes: list[float]) -> Optional[float]:
    """
    Kyle's Lambda — price impact coefficient from Kyle (1985).

    From the regression: delta_price = lambda * signed_volume + error

    lambda = cov(price_change, signed_volume) / var(signed_volume)

    signed_volume = +volume for buys, -volume for sells

    Interpretation:
      lambda = $0.01 means every 100 shares traded moves price by $1.00
      Low lambda  → deep market, large orders absorbed easily
      High lambda → shallow market, small orders move price significantly

    This is the theoretical foundation for:
      - Optimal trade execution (VWAP algorithms)
      - Market impact models (used by hedge funds)
      - Optimal market making (how to set bid/ask)

    In our simulator: momentum bot uses kyle_lambda to estimate
    how much its order will move the price before placing it.
    """
    if len(price_changes) < 10 or len(signed_volumes) < 10:
        return None

    n = min(len(price_changes), len(signed_volumes))
    pc = price_changes[:n]
    sv = signed_volumes[:n]

    mean_sv = sum(sv) / n
    mean_pc = sum(pc) / n

    cov = sum((sv[i] - mean_sv) * (pc[i] - mean_pc) for i in range(n)) / n
    var = sum((sv[i] - mean_sv) ** 2 for i in range(n)) / n

    if var == 0:
        return None

    return round(cov / var, 6)


# ── Trade Arrival Rate ────────────────────────────────────────────────────────

def trade_arrival_rate(timestamps: list[float],
                       window: float = 60.0) -> Optional[float]:
    """
    Trade Arrival Rate — trades per second in recent window.

    High arrival rate → active market, price discovery happening fast
    Low arrival rate  → quiet market, wide spreads, low liquidity

    Use in simulator:
      Sentiment engine weights recent trades more heavily
      when arrival rate is high (information-rich period).

      Market maker widens spread when arrival rate spikes
      (high activity often means informed trading).
    """
    if not timestamps:
        return None

    now = timestamps[-1]
    cutoff = now - window
    recent = [t for t in timestamps if t >= cutoff]

    if len(recent) < 2:
        return None

    duration = recent[-1] - recent[0]
    if duration <= 0:
        return None

    return round(len(recent) / duration, 4)


# ── Microstructure Engine ─────────────────────────────────────────────────────

class MicrostructureEngine:
    """
    Stateful microstructure calculator for one symbol.
    Maintains rolling windows of trades and quotes.

    Usage:
        engine = MicrostructureEngine('PEAR')
        engine.update_trade(price=151.0, volume=10, side='buy', ts=time())
        engine.update_quotes(bid=150.90, ask=151.10)
        snapshot = engine.snapshot()
    """

    def __init__(self, symbol: str, window: int = 50):
        self.symbol  = symbol
        self._window = window

        self._prices          : list[float] = []
        self._volumes         : list[int]   = []
        self._sides           : list[str]   = []   # 'buy' or 'sell'
        self._timestamps      : list[float] = []
        self._price_changes   : list[float] = []
        self._signed_volumes  : list[float] = []

        self._buy_volume  = 0
        self._sell_volume = 0
        self._trade_count = 0

        self._current_bid : Optional[float] = None
        self._current_ask : Optional[float] = None

    def update_trade(self, price: float, volume: int,
                     side: str, ts: float):
        """Record a new trade."""
        if self._prices:
            self._price_changes.append(price - self._prices[-1])
            signed = volume if side == 'buy' else -volume
            self._signed_volumes.append(float(signed))

        self._prices.append(price)
        self._volumes.append(volume)
        self._sides.append(side)
        self._timestamps.append(ts)
        self._trade_count += 1

        if side == 'buy':
            self._buy_volume += volume
        else:
            self._sell_volume += volume

        # keep window bounded
        if len(self._prices) > self._window:
            oldest_side = self._sides.pop(0)
            oldest_vol  = self._volumes.pop(0)
            self._prices.pop(0)
            self._timestamps.pop(0)
            if self._price_changes:
                self._price_changes.pop(0)
            if self._signed_volumes:
                self._signed_volumes.pop(0)
            # adjust running volumes
            if oldest_side == 'buy':
                self._buy_volume -= oldest_vol
            else:
                self._sell_volume -= oldest_vol

    def update_quotes(self, bid: float, ask: float):
        """Update current best bid/ask from order book."""
        self._current_bid = bid
        self._current_ask = ask

    def snapshot(self) -> MicrostructureSnapshot:
        bid = self._current_bid
        ask = self._current_ask

        # spread metrics
        if bid and ask:
            spread = compute_spread(bid, ask)
            spread.roll_spread = roll_spread_estimator(self._prices)
        else:
            spread = SpreadMetrics(None, None, None, None, None, None)

        # flow metrics
        flow = FlowMetrics(
            ofi          = order_flow_imbalance(
                               self._buy_volume, self._sell_volume),
            buy_volume   = self._buy_volume,
            sell_volume  = self._sell_volume,
            total_volume = self._buy_volume + self._sell_volume,
            trade_count  = self._trade_count,
            arrival_rate = trade_arrival_rate(self._timestamps),
        )

        # impact metrics
        prices  = self._prices
        volumes = self._volumes
        avg_size = (
            sum(volumes) / len(volumes) if volumes else None
        )

        # short-term volatility from price changes
        price_vol = None
        if len(self._price_changes) >= 5:
            mean_pc = sum(self._price_changes) / len(self._price_changes)
            var_pc  = sum(
                (p - mean_pc) ** 2 for p in self._price_changes
            ) / len(self._price_changes)
            price_vol = round(math.sqrt(var_pc), 4)

        impact = ImpactMetrics(
            amihud         = amihud_illiquidity(prices, volumes),
            kyle_lambda    = kyle_lambda(
                                 self._price_changes, self._signed_volumes),
            avg_trade_size = round(avg_size, 2) if avg_size else None,
            price_volatility = price_vol,
        )

        return MicrostructureSnapshot(
            spread    = spread,
            flow      = flow,
            impact    = impact,
            timestamp = time(),
        )