"""
Matching Engine — Kafka-wired Order Book

Consumes from `market-orders`, runs each order through the OrderBook,
then publishes resulting trades to `trade-executed`.

One matching engine instance per symbol — assigned to its own partition
on the market-orders topic so order sequencing is guaranteed per symbol.

Also runs a background TTL sweep thread that purges expired limit orders
and publishes them to `order-expired`.

Circuit breaker awareness — checks market-halt topic state before
processing orders. Rejects orders for halted symbols.
"""

import logging
import threading
from time import time, sleep

from config import (
    TOPIC_MARKET_ORDERS, TOPIC_TRADE_EXECUTED, TOPIC_ORDER_EXPIRED,
    TOPIC_MARKET_HALT, TTL_SWEEP_INTERVAL, SYMBOL_LIST,
)
from core.schemas import Trade, OrderExpired, MarketHaltEvent
from core.kafka_client import MarketProducer, MarketConsumer, symbol_to_partition
from engine.order_book import OrderBook

logger = logging.getLogger(__name__)


class MatchingEngine:
    """
    One instance handles one symbol's order book.

    Lifecycle:
        engine = MatchingEngine("PEAR")
        engine.start()   # blocks — runs consumer loop + TTL thread
    """

    def __init__(self, symbol: str):
        self.symbol    = symbol
        self.book      = OrderBook(symbol)
        self._running  = False
        self._halted   = False  # circuit breaker state

        partition = symbol_to_partition(symbol)

        # consumes only this symbol's partition
        self._consumer = MarketConsumer(
            group_id=f"matching-engine-{symbol}",
            topics=[TOPIC_MARKET_ORDERS],
            offset="latest",
            partitions=[partition],
        )

        # consumes halt events for this symbol
        self._halt_consumer = MarketConsumer(
            group_id=f"matching-engine-halt-{symbol}",
            topics=[TOPIC_MARKET_HALT],
            offset="latest",
        )

        self._producer = MarketProducer(f"matching-engine-{symbol}")

        logger.info(f"[matching-engine:{symbol}] initialised on partition {partition}")

    def start(self):
        """Start the matching engine — blocks the calling thread."""
        self._running = True

        # TTL sweep runs in background thread
        ttl_thread = threading.Thread(
            target=self._ttl_sweep_loop,
            name=f"ttl-sweep-{self.symbol}",
            daemon=True,
        )
        ttl_thread.start()

        # Halt monitor runs in background thread
        halt_thread = threading.Thread(
            target=self._halt_monitor_loop,
            name=f"halt-monitor-{self.symbol}",
            daemon=True,
        )
        halt_thread.start()

        logger.info(f"[matching-engine:{self.symbol}] started — listening for orders")

        try:
            self._order_loop()
        finally:
            self._running = False
            self._producer.flush()
            self._consumer.close()
            logger.info(f"[matching-engine:{self.symbol}] stopped")

    def stop(self):
        self._running = False

    # ── Main order processing loop ────────────────────────────────────────────

    def _order_loop(self):
        """
        Core loop — poll market-orders, run matching, emit trades.
        """
        while self._running:
            msg = self._consumer.poll_once(timeout=0.5)
            if msg is None:
                continue

            # only process orders for our symbol
            if msg.get("symbol") != self.symbol:
                continue

            if self._halted:
                logger.warning(
                    f"[matching-engine:{self.symbol}] "
                    f"HALTED — rejecting order {msg.get('order_id')}"
                )
                continue

            self._process_order(msg)

    def _process_order(self, msg: dict):
        """
        Run a single order through the book and emit resulting trades.
        """
        try:
            result = self.book.add_order(
                order_id   = msg["order_id"],
                client_id  = msg["client_id"],
                side       = msg["side"],
                order_type = msg["order_type"],
                price      = msg.get("price"),
                quantity   = msg["quantity"],
                timestamp  = msg["timestamp"],
            )
        except Exception as e:
            logger.error(f"[matching-engine:{self.symbol}] order error: {e} msg={msg}")
            return

        if not result.trades:
            return  # no match — order resting in book or rejected

        for event in result.trades:
            trade = Trade(
                symbol        = self.symbol,
                buyer_id      = event.buyer_id,
                seller_id     = event.seller_id,
                quantity      = event.quantity,
                price         = event.price,
                buy_order_id  = event.buy_order_id,
                sell_order_id = event.sell_order_id,
                timestamp     = event.timestamp,
            )
            self._producer.send(TOPIC_TRADE_EXECUTED, trade)

            logger.debug(
                f"[matching-engine:{self.symbol}] trade "
                f"buyer={trade.buyer_id} seller={trade.seller_id} "
                f"qty={trade.quantity} price={trade.price}"
            )

        self._producer.flush()

    # ── TTL sweep loop ────────────────────────────────────────────────────────

    def _ttl_sweep_loop(self):
        """
        Background thread — purges expired limit orders every N seconds.

        Why TTL matters:
          Without expiry, bots posting aggressive limit orders would
          leave stale quotes in the book forever, distorting the price
          and making the spread artificially tight or wide.
        """
        while self._running:
            sleep(TTL_SWEEP_INTERVAL)
            now = time()

            expired_orders = self.book.expire_orders(now)
            if not expired_orders:
                continue

            logger.info(
                f"[matching-engine:{self.symbol}] "
                f"TTL sweep: {len(expired_orders)} orders expired"
            )

            for order in expired_orders:
                expired_event = OrderExpired(
                    order_id   = order.order_id,
                    client_id  = order.client_id,
                    symbol     = self.symbol,
                    side       = order.side,
                    quantity   = order.quantity,
                    price      = order.price,
                    placed_at  = order.timestamp,
                )
                self._producer.send(TOPIC_ORDER_EXPIRED, expired_event)

            self._producer.flush()

    # ── Halt monitor loop ─────────────────────────────────────────────────────

    def _halt_monitor_loop(self):
        """
        Background thread — watches market-halt topic.
        Flips self._halted flag when circuit breaker fires or clears.

        Circuit breaker concept:
          When price moves too fast (e.g. 5% in 5 seconds), trading halts
          to prevent runaway cascades. Real exchanges (NYSE, NASDAQ) use
          this mechanism — called a "trading curb" or "limit up/limit down".
        """
        while self._running:
            msg = self._halt_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            if msg.get("symbol") != self.symbol:
                continue

            status = msg.get("status")
            if status == "halted":
                self._halted = True
                logger.warning(
                    f"[matching-engine:{self.symbol}] "
                    f"CIRCUIT BREAKER — trading halted. "
                    f"reason={msg.get('reason')}"
                )
            elif status == "resumed":
                self._halted = False
                logger.info(
                    f"[matching-engine:{self.symbol}] "
                    f"trading resumed"
                )

    # ── Introspection helpers (used by API + dashboard) ───────────────────────

    def get_depth(self, levels: int = 10) -> dict:
        return self.book.get_depth(levels)

    def get_mid_price(self) -> float | None:
        return self.book.get_mid_price()

    def get_spread(self) -> float | None:
        return self.book.get_spread()

    def get_ofi(self) -> float:
        return self.book.get_order_flow_imbalance()

    def is_halted(self) -> bool:
        return self._halted


# ── Multi-symbol runner ───────────────────────────────────────────────────────

def run_all_engines():
    """
    Spawns one MatchingEngine per symbol, each in its own thread.
    Called from main runner script.

    Threading model:
      Each engine owns one partition on market-orders.
      No shared state between engines — thread safe by design.
    """
    threads = []
    engines = {}

    for symbol in SYMBOL_LIST:
        engine = MatchingEngine(symbol)
        engines[symbol] = engine
        t = threading.Thread(
            target=engine.start,
            name=f"engine-{symbol}",
            daemon=True,
        )
        threads.append(t)
        t.start()
        logger.info(f"[runner] started matching engine for {symbol}")

    return engines, threads