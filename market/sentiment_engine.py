"""
Sentiment Engine — Reads Trade Flow, Emits Bullish/Bearish Signal

Consumes trade-executed, maintains a rolling window of trades per symbol,
and publishes a SentimentUpdate after every trade.

Bots consume market-sentiment to adjust their aggression and direction.
The feedback loop this creates produces realistic bullish/bearish swings:

  More buys → bullish signal → momentum bot buys more
  → even more buy pressure → price rises → RSI climbs
  → mean reversion bot starts selling → flow balances
  → sentiment flips neutral → momentum bot backs off
  → market maker rebalances quotes → cycle repeats

This self-reinforcing then self-correcting dynamic is what creates
realistic price action without any scripted scenarios.

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Rolling Window Analysis    Recency-weighted signal extraction
  Buy/Sell Ratio             Directional pressure measurement
  Trade Velocity             Activity level as signal amplifier
  Sentiment Strength         Continuous 0→1 signal vs binary flags
  Feedback Loops             How sentiment creates momentum and reversals
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import threading
from collections import deque
from time import time

from config import (
    TOPIC_TRADE_EXECUTED, TOPIC_MARKET_SENTIMENT,
    SENTIMENT_WINDOW, BULLISH_THRESHOLD, BEARISH_THRESHOLD,
    SYMBOL_LIST,
)
from core.schemas import SentimentUpdate
from core.kafka_client import MarketProducer, MarketConsumer

logger = logging.getLogger(__name__)


# ── Per-symbol sentiment state ────────────────────────────────────────────────

class SymbolSentiment:
    """
    Maintains rolling trade window for one symbol.
    Computes sentiment signal on every new trade.

    Rolling window approach:
      We keep the last N trades (not time-based).
      Each trade is (side, volume, timestamp).
      buy_ratio = buy_volume / total_volume in window.

    Why volume-weighted instead of trade-count weighted?
      10 trades of 1 share each ≠ 1 trade of 100 shares.
      A large buy order signals stronger conviction than
      many small ones. Volume weighting captures this.

    Strength calculation:
      Raw strength = distance from neutral (0.5) normalized to 0→1
      bullish: strength = (buy_ratio - 0.5) / 0.5
      bearish: strength = (0.5 - buy_ratio) / 0.5
      neutral: strength = 1 - abs(buy_ratio - 0.5) / 0.5

    Trade velocity as amplifier:
      High velocity (many trades/sec) → sentiment signal is stronger
      Low velocity (few trades/sec)   → signal is weaker / less reliable
      Final strength = raw_strength * velocity_factor (capped at 1.0)
    """

    def __init__(self, symbol: str):
        self.symbol   = symbol
        self._window  : deque = deque(maxlen=SENTIMENT_WINDOW)
        # each entry: (side, volume, timestamp)

    def add_trade(self, side: str, volume: int, ts: float):
        self._window.append((side, volume, ts))

    def compute(self) -> SentimentUpdate:
        if not self._window:
            return SentimentUpdate(
                symbol         = self.symbol,
                sentiment      = "neutral",
                strength       = 0.0,
                buy_ratio      = 0.5,
                trade_velocity = 0.0,
            )

        # volume-weighted buy ratio
        buy_vol   = sum(v for s, v, _ in self._window if s == "buy")
        sell_vol  = sum(v for s, v, _ in self._window if s == "sell")
        total_vol = buy_vol + sell_vol

        buy_ratio = buy_vol / total_vol if total_vol > 0 else 0.5

        # determine sentiment direction
        if buy_ratio >= BULLISH_THRESHOLD:
            sentiment = "bullish"
            raw_strength = (buy_ratio - 0.5) / 0.5
        elif buy_ratio <= BEARISH_THRESHOLD:
            sentiment = "bearish"
            raw_strength = (0.5 - buy_ratio) / 0.5
        else:
            sentiment = "neutral"
            raw_strength = 1.0 - abs(buy_ratio - 0.5) / 0.5

        raw_strength = max(0.0, min(1.0, raw_strength))

        # trade velocity — trades per second in window
        timestamps = [ts for _, _, ts in self._window]
        velocity   = 0.0
        if len(timestamps) >= 2:
            duration = timestamps[-1] - timestamps[0]
            if duration > 0:
                velocity = len(timestamps) / duration

        # velocity factor: normalize to 0→1 using soft cap
        # 10 trades/sec = full amplification, less = proportional
        velocity_factor = min(1.0, velocity / 10.0) if velocity > 0 else 0.1

        # final strength combines direction strength + velocity
        # velocity amplifies conviction but doesn't flip direction
        final_strength = round(raw_strength * (0.7 + 0.3 * velocity_factor), 4)
        final_strength = max(0.0, min(1.0, final_strength))

        return SentimentUpdate(
            symbol         = self.symbol,
            sentiment      = sentiment,
            strength       = final_strength,
            buy_ratio      = round(buy_ratio, 4),
            trade_velocity = round(velocity, 4),
        )


# ── Sentiment Engine service ──────────────────────────────────────────────────

class SentimentEngine:
    """
    Kafka consumer service — one instance handles all symbols.

    Consumes trade-executed, updates per-symbol SymbolSentiment,
    publishes SentimentUpdate after every trade.

    Also exposes get_sentiment() for direct API access.
    """

    def __init__(self):
        self._sentiment : dict[str, SymbolSentiment] = {
            symbol: SymbolSentiment(symbol)
            for symbol in SYMBOL_LIST
        }
        self._latest : dict[str, SentimentUpdate] = {}
        self._lock   = threading.Lock()

        self._consumer = MarketConsumer(
            group_id="sentiment-engine",
            topics=[TOPIC_TRADE_EXECUTED],
            offset="latest",
        )
        self._producer = MarketProducer("sentiment-engine")
        self._running  = False

        logger.info("[sentiment-engine] initialised")

    def start(self):
        self._running = True
        logger.info("[sentiment-engine] started")
        try:
            self._run_loop()
        finally:
            self._running = False
            self._producer.flush()
            logger.info("[sentiment-engine] stopped")

    def stop(self):
        self._running = False

    def get_sentiment(self, symbol: str) -> SentimentUpdate | None:
        """Direct access for API and bots."""
        with self._lock:
            return self._latest.get(symbol)

    def get_all_sentiment(self) -> dict[str, SentimentUpdate]:
        with self._lock:
            return dict(self._latest)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _run_loop(self):
        while self._running:
            msg = self._consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            self._process_trade(msg)

    def _process_trade(self, msg: dict):
        symbol    = msg.get("symbol")
        buyer_id  = msg.get("buyer_id", "")
        seller_id = msg.get("seller_id", "")
        quantity  = msg.get("quantity", 0)
        ts        = msg.get("timestamp", time())

        if not symbol or symbol not in self._sentiment:
            return

        # infer aggressor side (same logic as price_feed)
        if "market-maker" in seller_id:
            side = "buy"
        elif "market-maker" in buyer_id:
            side = "sell"
        elif buyer_id == "user":
            side = "buy"
        elif seller_id == "user":
            side = "sell"
        else:
            side = "buy"

        with self._lock:
            self._sentiment[symbol].add_trade(side, quantity, ts)
            update = self._sentiment[symbol].compute()
            self._latest[symbol] = update

        self._producer.send(TOPIC_MARKET_SENTIMENT, update)
        self._producer.flush()

        logger.debug(
            f"[sentiment] {symbol} {update.sentiment} "
            f"strength={update.strength} buy_ratio={update.buy_ratio}"
        )