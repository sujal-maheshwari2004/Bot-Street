"""
Technical Indicators — Pure Math, No Kafka, No Side Effects

All functions operate on plain Python lists of prices/volumes.
No external TA library — implemented from scratch for learning.

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  EMA         Exponential Moving Average
  SMA         Simple Moving Average
  VWAP        Volume Weighted Average Price
  RSI         Relative Strength Index
  MACD        Moving Average Convergence Divergence
  Bollinger   Bollinger Bands
  ATR         Average True Range
─────────────────────────────────────────────────────────────────────────────
"""

from dataclasses import dataclass
from typing import Optional


# ── Output dataclasses ────────────────────────────────────────────────────────

@dataclass
class MACDResult:
    """
    MACD = EMA(fast) - EMA(slow)
    Signal = EMA(MACD, signal_period)
    Histogram = MACD - Signal

    Bullish signal: MACD crosses above Signal (golden cross)
    Bearish signal: MACD crosses below Signal (death cross)
    """
    macd      : float
    signal    : float
    histogram : float


@dataclass
class BollingerResult:
    """
    Middle band = SMA(period)
    Upper band  = SMA + (std_dev * multiplier)
    Lower band  = SMA - (std_dev * multiplier)

    Price near upper band = overbought territory
    Price near lower band = oversold territory
    Bandwidth narrows before big moves (volatility squeeze)
    """
    upper  : float
    middle : float
    lower  : float
    bandwidth : float   # (upper - lower) / middle — volatility measure


@dataclass
class IndicatorSnapshot:
    """
    Complete indicator state for one symbol at one point in time.
    Published inside PriceUpdate messages.
    """
    ema_short  : Optional[float]   # EMA(9)
    ema_long   : Optional[float]   # EMA(21)
    rsi        : Optional[float]   # RSI(14) — 0 to 100
    macd       : Optional[float]   # MACD line
    macd_signal: Optional[float]   # Signal line
    macd_hist  : Optional[float]   # Histogram
    bb_upper   : Optional[float]   # Bollinger upper
    bb_middle  : Optional[float]   # Bollinger middle
    bb_lower   : Optional[float]   # Bollinger lower
    bb_bw      : Optional[float]   # Bollinger bandwidth
    vwap       : Optional[float]   # Session VWAP
    atr        : Optional[float]   # Average True Range


# ── Simple Moving Average ─────────────────────────────────────────────────────

def sma(prices: list[float], period: int) -> Optional[float]:
    """
    Simple Moving Average — equal weight to all prices in window.

    SMA = sum(prices[-period:]) / period

    Weakness: reacts slowly to recent changes.
    EMA fixes this by weighting recent prices more heavily.
    """
    if len(prices) < period:
        return None
    window = prices[-period:]
    return round(sum(window) / period, 4)


# ── Exponential Moving Average ────────────────────────────────────────────────

def ema(prices: list[float], period: int,
        prev_ema: Optional[float] = None) -> Optional[float]:
    """
    Exponential Moving Average — weights recent prices more heavily.

    Smoothing factor k = 2 / (period + 1)
    EMA = price * k + prev_EMA * (1 - k)

    First EMA value = SMA of first N prices (bootstrapping)

    period=9  → fast EMA, reacts quickly, good for entry signals
    period=21 → slow EMA, reacts slowly, good for trend direction

    EMA(9) crossing above EMA(21) → bullish crossover signal
    EMA(9) crossing below EMA(21) → bearish crossover signal
    """
    if len(prices) < period:
        return None

    k = 2.0 / (period + 1)

    if prev_ema is None:
        # bootstrap: first EMA = SMA of first period prices
        return round(sum(prices[:period]) / period, 4)

    return round(prices[-1] * k + prev_ema * (1 - k), 4)


def ema_series(prices: list[float], period: int) -> list[Optional[float]]:
    """
    Compute full EMA series over all prices.
    Returns a list of same length — None until enough data.
    Used internally by MACD.
    """
    result = [None] * len(prices)
    if len(prices) < period:
        return result

    # bootstrap first value
    result[period - 1] = sum(prices[:period]) / period
    k = 2.0 / (period + 1)

    for i in range(period, len(prices)):
        result[i] = round(prices[i] * k + result[i - 1] * (1 - k), 4)

    return result


# ── VWAP ──────────────────────────────────────────────────────────────────────

def vwap(prices: list[float], volumes: list[int]) -> Optional[float]:
    """
    Volume Weighted Average Price

    VWAP = sum(price * volume) / sum(volume)

    The true average price paid across all transactions,
    weighted by how much was traded at each price.

    Institutional traders use VWAP as a benchmark:
      Bought below VWAP = good execution
      Bought above VWAP = paid too much

    Mean reversion bot logic:
      Price >> VWAP → likely to fall back → SELL signal
      Price << VWAP → likely to rise back → BUY signal
    """
    if not prices or not volumes or len(prices) != len(volumes):
        return None
    total_volume = sum(volumes)
    if total_volume == 0:
        return None
    total_cost = sum(p * v for p, v in zip(prices, volumes))
    return round(total_cost / total_volume, 4)


# ── RSI ───────────────────────────────────────────────────────────────────────

def rsi(prices: list[float], period: int = 14) -> Optional[float]:
    """
    Relative Strength Index — measures speed and magnitude of price moves.
    Range: 0 to 100.

    Calculation:
      gains = average of up-moves over period
      losses = average of down-moves over period
      RS = gains / losses
      RSI = 100 - (100 / (1 + RS))

    Interpretation:
      RSI > 70 → overbought → price may reverse down → SELL signal
      RSI < 30 → oversold  → price may reverse up   → BUY signal
      RSI = 50 → neutral momentum

    Mean reversion bot uses RSI directly:
      RSI > 70 → sell (fade the move)
      RSI < 30 → buy  (fade the move)

    Limitation: RSI can stay overbought/oversold for extended
    periods in strong trends — always confirm with other signals.
    """
    if len(prices) < period + 1:
        return None

    deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    recent = deltas[-(period):]

    gains  = [d for d in recent if d > 0]
    losses = [abs(d) for d in recent if d < 0]

    avg_gain = sum(gains)  / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0   # no losses = maximum strength

    rs  = avg_gain / avg_loss
    rsi_value = 100 - (100 / (1 + rs))
    return round(rsi_value, 2)


# ── MACD ──────────────────────────────────────────────────────────────────────

def macd(prices: list[float],
         fast: int = 12,
         slow: int = 26,
         signal: int = 9) -> Optional[MACDResult]:
    """
    Moving Average Convergence Divergence

    MACD line   = EMA(fast) - EMA(slow)       e.g. EMA(12) - EMA(26)
    Signal line = EMA(MACD line, signal)       e.g. EMA(MACD, 9)
    Histogram   = MACD line - Signal line

    Trading signals:
      MACD crosses above Signal → bullish momentum (buy)
      MACD crosses below Signal → bearish momentum (sell)
      Histogram growing         → momentum accelerating
      Histogram shrinking       → momentum fading (watch for reversal)
      MACD above zero           → uptrend
      MACD below zero           → downtrend

    Why two EMAs?
      Fast EMA reacts to recent price changes quickly.
      Slow EMA represents longer-term trend.
      The difference (MACD) shows how far current momentum
      deviates from the trend.
    """
    min_required = slow + signal
    if len(prices) < min_required:
        return None

    fast_series = ema_series(prices, fast)
    slow_series = ema_series(prices, slow)

    # MACD line = difference where both EMAs are available
    macd_line = []
    for f, s in zip(fast_series, slow_series):
        if f is not None and s is not None:
            macd_line.append(round(f - s, 4))

    if len(macd_line) < signal:
        return None

    # Signal line = EMA of MACD line
    signal_series = ema_series(macd_line, signal)
    signal_value  = next(
        (v for v in reversed(signal_series) if v is not None), None
    )
    if signal_value is None:
        return None

    macd_value = macd_line[-1]
    histogram  = round(macd_value - signal_value, 4)

    return MACDResult(
        macd=macd_value,
        signal=round(signal_value, 4),
        histogram=histogram,
    )


# ── Bollinger Bands ───────────────────────────────────────────────────────────

def bollinger_bands(prices: list[float],
                    period: int = 20,
                    std_multiplier: float = 2.0) -> Optional[BollingerResult]:
    """
    Bollinger Bands — volatility envelope around a moving average.

    Middle = SMA(period)
    Upper  = SMA + (std_dev * multiplier)
    Lower  = SMA - (std_dev * multiplier)

    Interpretation:
      ~95% of prices fall within the bands (2 std devs)
      Price touching upper band → strong uptrend OR overbought
      Price touching lower band → strong downtrend OR oversold
      Bands widening  → volatility increasing (big move coming)
      Bands narrowing → volatility squeezing (breakout coming)

    Circuit breaker input:
      Wide bands signal high volatility.
      When bands are unusually wide, circuit breaker threshold
      could be tightened (not implemented in POC but the data is here).

    Bandwidth = (upper - lower) / middle
      Low bandwidth → quiet market
      High bandwidth → volatile market
    """
    if len(prices) < period:
        return None

    window = prices[-period:]
    middle = sum(window) / period

    variance = sum((p - middle) ** 2 for p in window) / period
    std_dev  = variance ** 0.5

    upper = round(middle + std_multiplier * std_dev, 4)
    lower = round(middle - std_multiplier * std_dev, 4)
    mid   = round(middle, 4)
    bw    = round((upper - lower) / mid, 6) if mid != 0 else 0.0

    return BollingerResult(upper=upper, middle=mid, lower=lower, bandwidth=bw)


# ── ATR ───────────────────────────────────────────────────────────────────────

def atr(highs: list[float], lows: list[float],
        closes: list[float], period: int = 14) -> Optional[float]:
    """
    Average True Range — measures market volatility.

    True Range = max of:
      high - low                    (current candle range)
      |high - prev_close|           (gap up scenario)
      |low  - prev_close|           (gap down scenario)

    ATR = SMA(True Range, period)

    Use in simulator:
      High ATR → volatile market → bots widen their spreads
      Low ATR  → quiet market   → bots tighten spreads
      Position sizing: risk = ATR * multiplier (e.g. 2x ATR stop loss)

    Note: requires OHLC data from candle aggregator.
    Only meaningful after enough candles have formed.
    """
    if len(closes) < period + 1:
        return None

    true_ranges = []
    for i in range(1, len(closes)):
        if i >= len(highs) or i >= len(lows):
            break
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return round(sum(true_ranges[-period:]) / period, 4)


# ── Indicator engine ──────────────────────────────────────────────────────────

class IndicatorEngine:
    """
    Stateful indicator calculator for a single symbol.
    Maintains rolling price/volume history and prev EMA values
    for efficient incremental updates on each new trade.

    Usage:
        engine = IndicatorEngine("PEAR")
        engine.update(price=151.0, volume=10)
        snapshot = engine.snapshot()
    """

    def __init__(self, symbol: str):
        self.symbol   = symbol
        self._prices  : list[float] = []
        self._volumes : list[int]   = []

        # cached EMA values for incremental updates
        self._ema9  : Optional[float] = None
        self._ema21 : Optional[float] = None

        # candle OHLC for ATR
        self._highs  : list[float] = []
        self._lows   : list[float] = []
        self._closes : list[float] = []

    def update(self, price: float, volume: int):
        """Add a new trade price and volume, update all indicators."""
        self._prices.append(price)
        self._volumes.append(volume)

        # incremental EMA updates (efficient — no full recompute)
        from config import EMA_SHORT, EMA_LONG
        self._ema9  = ema(self._prices, EMA_SHORT,  self._ema9)
        self._ema21 = ema(self._prices, EMA_LONG,   self._ema21)

        # keep history bounded — 500 ticks is plenty for all indicators
        if len(self._prices) > 500:
            self._prices.pop(0)
            self._volumes.pop(0)

    def update_candle(self, high: float, low: float, close: float):
        """Feed closed candle OHLC data for ATR calculation."""
        self._highs.append(high)
        self._lows.append(low)
        self._closes.append(close)
        if len(self._closes) > 100:
            self._highs.pop(0)
            self._lows.pop(0)
            self._closes.pop(0)

    def snapshot(self) -> IndicatorSnapshot:
        """
        Compute and return all current indicator values.
        Returns None for indicators that need more data.
        """
        from config import RSI_PERIOD, MACD_SIGNAL, BOLLINGER_PERIOD, BOLLINGER_STD

        prices  = self._prices
        volumes = self._volumes

        _macd   = macd(prices)
        _bb     = bollinger_bands(prices, BOLLINGER_PERIOD, BOLLINGER_STD)
        _atr    = atr(self._highs, self._lows, self._closes)

        return IndicatorSnapshot(
            ema_short   = self._ema9,
            ema_long    = self._ema21,
            rsi         = rsi(prices, RSI_PERIOD),
            macd        = _macd.macd      if _macd else None,
            macd_signal = _macd.signal    if _macd else None,
            macd_hist   = _macd.histogram if _macd else None,
            bb_upper    = _bb.upper       if _bb   else None,
            bb_middle   = _bb.middle      if _bb   else None,
            bb_lower    = _bb.lower       if _bb   else None,
            bb_bw       = _bb.bandwidth   if _bb   else None,
            vwap        = vwap(prices, volumes),
            atr         = _atr,
        )

    def has_enough_data(self, min_prices: int = 30) -> bool:
        return len(self._prices) >= min_prices