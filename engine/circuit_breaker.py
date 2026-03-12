"""
Circuit Breaker — Halts trading on extreme price moves

Concept:
─────────────────────────────────────────────────────────────────────────────
Real exchanges use circuit breakers to prevent flash crashes.
NYSE Rule 80B: market-wide halt if S&P 500 drops 7%, 13%, or 20%.
Individual stocks halt if they move 5-10% in a 5-minute window.

Our implementation:
  - Watches price-update topic per symbol
  - If |current_price - price_N_seconds_ago| / price_N_seconds_ago > threshold
    → publish market-halt event
    → automatically resume after CIRCUIT_BREAKER_DURATION seconds
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import threading
from collections import deque
from time import time, sleep

from config import (
    TOPIC_PRICE_UPDATE, TOPIC_MARKET_HALT,
    CIRCUIT_BREAKER_THRESHOLD, CIRCUIT_BREAKER_WINDOW,
    CIRCUIT_BREAKER_DURATION, SYMBOL_LIST,
)
from core.schemas import MarketHaltEvent
from core.kafka_client import MarketProducer, MarketConsumer

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """
    Monitors price feed for all symbols.
    Fires halt events when price moves exceed threshold.
    One instance handles all symbols.
    """

    def __init__(self):
        # per symbol: deque of (timestamp, price) within window
        self._price_history : dict[str, deque] = {
            s: deque() for s in SYMBOL_LIST
        }
        self._halted        : dict[str, bool]  = {s: False for s in SYMBOL_LIST}
        self._resume_timers : dict[str, threading.Timer] = {}

        self._consumer = MarketConsumer(
            group_id="circuit-breaker",
            topics=[TOPIC_PRICE_UPDATE],
            offset="latest",
        )
        self._producer = MarketProducer("circuit-breaker")
        self._running  = False

        logger.info("[circuit-breaker] initialised")

    def start(self):
        self._running = True
        logger.info("[circuit-breaker] started — monitoring price feed")
        try:
            self._run_loop()
        finally:
            self._running = False
            self._producer.flush()
            logger.info("[circuit-breaker] stopped")

    def stop(self):
        self._running = False

    def is_halted(self, symbol: str) -> bool:
        return self._halted.get(symbol, False)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run_loop(self):
        while self._running:
            msg = self._consumer.poll_once(timeout=0.5)
            if msg is None:
                continue

            symbol = msg.get("symbol")
            price  = msg.get("price")
            ts     = msg.get("timestamp", time())

            if not symbol or not price:
                continue
            if symbol not in SYMBOL_LIST:
                continue
            if self._halted[symbol]:
                continue

            self._check(symbol, price, ts)

    def _check(self, symbol: str, price: float, ts: float):
        """
        Maintain a sliding window of prices.
        Trigger halt if move exceeds threshold within window.
        """
        history = self._price_history[symbol]

        # add current price
        history.append((ts, price))

        # remove entries outside the time window
        cutoff = ts - CIRCUIT_BREAKER_WINDOW
        while history and history[0][0] < cutoff:
            history.popleft()

        if len(history) < 2:
            return

        oldest_price = history[0][1]
        move = abs(price - oldest_price) / oldest_price

        if move >= CIRCUIT_BREAKER_THRESHOLD:
            self._trigger_halt(symbol, oldest_price, price)

    def _trigger_halt(self, symbol: str, price_before: float, price_now: float):
        self._halted[symbol] = True
        resume_at = time() + CIRCUIT_BREAKER_DURATION

        halt_event = MarketHaltEvent(
            symbol        = symbol,
            status        = "halted",
            reason        = "circuit_breaker",
            price_before  = price_before,
            price_trigger = price_now,
            resume_at     = resume_at,
        )
        self._producer.send(TOPIC_MARKET_HALT, halt_event)
        self._producer.flush()

        logger.warning(
            f"[circuit-breaker] ⛔ {symbol} HALTED "
            f"move={abs(price_now - price_before)/price_before*100:.1f}% "
            f"resuming in {CIRCUIT_BREAKER_DURATION}s"
        )

        # auto-resume after duration
        timer = threading.Timer(
            CIRCUIT_BREAKER_DURATION,
            self._resume,
            args=[symbol],
        )
        timer.daemon = True
        self._resume_timers[symbol] = timer
        timer.start()

    def _resume(self, symbol: str):
        self._halted[symbol] = False
        self._price_history[symbol].clear()

        resume_event = MarketHaltEvent(
            symbol        = symbol,
            status        = "resumed",
            reason        = None,
            price_before  = None,
            price_trigger = None,
            resume_at     = None,
        )
        self._producer.send(TOPIC_MARKET_HALT, resume_event)
        self._producer.flush()

        logger.info(f"[circuit-breaker] ✅ {symbol} trading resumed")