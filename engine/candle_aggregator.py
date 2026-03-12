"""
Candle Aggregator — Builds OHLCV candles from trade stream

Concept:
─────────────────────────────────────────────────────────────────────────────
OHLCV Candlestick:
  Open   = first trade price in the time window
  High   = highest trade price in the window
  Low    = lowest trade price in the window
  Close  = last trade price in the window
  Volume = total shares traded in the window

Why candles matter:
  Raw tick data is noisy. Candles compress it into digestible
  price action. Traders use candle patterns to read momentum,
  reversals, and volatility at a glance.

  A tall candle body = strong directional move
  A small body with long wicks = indecision / rejection

Our candles: 10-second buckets (configurable via CANDLE_INTERVAL_SECONDS)
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import threading
from collections import defaultdict
from time import time, sleep

from config import (
    TOPIC_TRADE_EXECUTED, TOPIC_CANDLES,
    CANDLE_INTERVAL_SECONDS, SYMBOL_LIST,
)
from core.schemas import Candle
from core.kafka_client import MarketProducer, MarketConsumer

logger = logging.getLogger(__name__)


class CandleAggregator:
    """
    Buckets trades into fixed time windows per symbol.
    Publishes a Candle to the `candles` topic on window close.
    """

    def __init__(self):
        self._buckets  : dict[str, dict] = {}   # symbol → current open bucket
        self._history  : dict[str, list] = defaultdict(list)  # symbol → last N candles
        self._lock     = threading.Lock()

        self._consumer = MarketConsumer(
            group_id="candle-aggregator",
            topics=[TOPIC_TRADE_EXECUTED],
            offset="latest",
        )
        self._producer = MarketProducer("candle-aggregator")
        self._running  = False

        # initialise empty buckets for all symbols
        now = time()
        for symbol in SYMBOL_LIST:
            self._buckets[symbol] = self._new_bucket(now)

        logger.info("[candle-aggregator] initialised")

    def start(self):
        self._running = True

        # background thread flushes buckets on interval
        flush_thread = threading.Thread(
            target=self._flush_loop,
            name="candle-flush",
            daemon=True,
        )
        flush_thread.start()

        logger.info("[candle-aggregator] started")
        try:
            self._trade_loop()
        finally:
            self._running = False
            self._producer.flush()
            logger.info("[candle-aggregator] stopped")

    def stop(self):
        self._running = False

    def get_history(self, symbol: str, n: int = 20) -> list[Candle]:
        """Returns last N closed candles for a symbol. Used by API + dashboard."""
        with self._lock:
            return list(self._history[symbol][-n:])

    # ── Internal ──────────────────────────────────────────────────────────────

    def _trade_loop(self):
        while self._running:
            msg = self._consumer.poll_once(timeout=0.5)
            if msg is None:
                continue

            symbol   = msg.get("symbol")
            price    = msg.get("price")
            quantity = msg.get("quantity")
            ts       = msg.get("timestamp", time())

            if not symbol or not price or not quantity:
                continue

            with self._lock:
                bucket = self._buckets[symbol]
                if bucket["open"] is None:
                    bucket["open"] = price
                bucket["high"]   = max(bucket["high"] or price, price)
                bucket["low"]    = min(bucket["low"]  or price, price)
                bucket["close"]  = price
                bucket["volume"] += quantity
                bucket["trades"] += 1
                # running VWAP within candle
                bucket["_total_cost"] += price * quantity
                bucket["vwap"] = bucket["_total_cost"] / bucket["volume"]

    def _flush_loop(self):
        """
        Closes current candle bucket every CANDLE_INTERVAL_SECONDS
        and starts a fresh one.
        """
        while self._running:
            sleep(CANDLE_INTERVAL_SECONDS)
            now = time()

            with self._lock:
                for symbol in SYMBOL_LIST:
                    bucket = self._buckets[symbol]

                    # skip empty buckets (no trades this window)
                    if bucket["open"] is None:
                        bucket["open_time"] = now
                        continue

                    candle = Candle(
                        symbol      = symbol,
                        open        = bucket["open"],
                        high        = bucket["high"],
                        low         = bucket["low"],
                        close       = bucket["close"],
                        volume      = bucket["volume"],
                        vwap        = round(bucket["vwap"], 4),
                        trade_count = bucket["trades"],
                        interval_s  = CANDLE_INTERVAL_SECONDS,
                        open_time   = bucket["open_time"],
                        close_time  = now,
                    )

                    self._history[symbol].append(candle)
                    # keep only last 100 candles in memory
                    if len(self._history[symbol]) > 100:
                        self._history[symbol].pop(0)

                    self._producer.send(TOPIC_CANDLES, candle)

                    logger.debug(
                        f"[candle] {symbol} O={candle.open} H={candle.high} "
                        f"L={candle.low} C={candle.close} V={candle.volume}"
                    )

                    # reset bucket
                    self._buckets[symbol] = self._new_bucket(now)

            self._producer.flush()

    def _new_bucket(self, open_time: float) -> dict:
        return {
            "open"       : None,
            "high"       : None,
            "low"        : None,
            "close"      : None,
            "volume"     : 0,
            "trades"     : 0,
            "vwap"       : 0.0,
            "_total_cost": 0.0,
            "open_time"  : open_time,
        }