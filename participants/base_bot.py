"""
Base Bot — Shared Interface All Bots Inherit

Every bot in the simulator shares the same lifecycle:
  1. Subscribe to price-update and market-sentiment
  2. On each tick, decide whether to place an order
  3. Check circuit breaker before placing
  4. Place order via MarketProducer → market-orders topic

This base class handles all the plumbing so each bot
only needs to implement one method: _on_tick()

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Bot Architecture       How algorithmic traders are structured
  Event-driven vs Timed  Two approaches to bot decision making
  Position Awareness     Bots tracking their own state
  Risk Controls          Pre-trade checks every bot must pass
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import threading
from abc import ABC, abstractmethod
from time import time, sleep
from typing import Optional

from config import (
    TOPIC_MARKET_ORDERS, TOPIC_PRICE_UPDATE, TOPIC_MARKET_SENTIMENT,
    TOPIC_MARKET_HALT, TOPIC_PORTFOLIO_SNAP,
    BOT_TICK_SECONDS, SYMBOL_LIST, INITIAL_CASH, SYMBOLS,
)
from core.schemas import Order
from core.kafka_client import MarketProducer, MarketConsumer

logger = logging.getLogger(__name__)


# ── Per-symbol state ──────────────────────────────────────────────────────────

class BotSymbolState:
    """
    Everything a bot knows about one symbol right now.
    Updated on every price-update and sentiment message received.
    """
    def __init__(self, symbol: str, initial_price: float):
        self.symbol    = symbol
        self.price     = initial_price
        self.vwap      : Optional[float] = None
        self.rsi       : Optional[float] = None
        self.ema_short : Optional[float] = None
        self.ema_long  : Optional[float] = None
        self.bb_upper  : Optional[float] = None
        self.bb_lower  : Optional[float] = None
        self.ofi       : Optional[float] = None
        self.sentiment : Optional[str]   = None  # bullish/bearish/neutral
        self.strength  : float           = 0.0
        self.buy_ratio : float           = 0.5
        self.halted    : bool            = False
        self.last_updated : float        = time()

    def update_from_price(self, msg: dict):
        self.price     = msg.get("price", self.price)
        self.vwap      = msg.get("vwap")
        self.rsi       = msg.get("rsi")
        self.ema_short = msg.get("ema_short")
        self.ema_long  = msg.get("ema_long")
        self.bb_upper  = msg.get("bb_upper")
        self.bb_lower  = msg.get("bb_lower")
        self.ofi       = msg.get("ofi")
        self.last_updated = time()

    def update_from_sentiment(self, msg: dict):
        self.sentiment = msg.get("sentiment")
        self.strength  = msg.get("strength", 0.0)
        self.buy_ratio = msg.get("buy_ratio", 0.5)


# ── Bot portfolio state ───────────────────────────────────────────────────────

class BotPortfolio:
    """
    Lightweight in-memory portfolio tracker for a bot.
    Mirrors what portfolio_ledger tracks but locally
    so bots can make position-aware decisions without
    making a Kafka round trip on every tick.
    """
    def __init__(self):
        self.cash     : float            = INITIAL_CASH
        self.holdings : dict[str, int]   = {}   # symbol → qty
        self.avg_cost : dict[str, float] = {}   # symbol → avg price

    def apply_buy(self, symbol: str, qty: int, price: float):
        cost = qty * price
        if cost > self.cash:
            return False
        prev_qty  = self.holdings.get(symbol, 0)
        prev_cost = self.avg_cost.get(symbol, 0.0)
        new_qty   = prev_qty + qty
        self.avg_cost[symbol] = (
            (prev_qty * prev_cost + qty * price) / new_qty
        )
        self.holdings[symbol] = new_qty
        self.cash -= cost
        return True

    def apply_sell(self, symbol: str, qty: int, price: float):
        held = self.holdings.get(symbol, 0)
        if qty > held:
            return False
        self.holdings[symbol] -= qty
        self.cash += qty * price
        if self.holdings[symbol] == 0:
            del self.holdings[symbol]
            del self.avg_cost[symbol]
        return True

    def position(self, symbol: str) -> int:
        return self.holdings.get(symbol, 0)

    def can_afford(self, qty: int, price: float) -> bool:
        return self.cash >= qty * price


# ── Base Bot ──────────────────────────────────────────────────────────────────

class BaseBot(ABC):
    """
    Abstract base class for all market participants.

    Subclasses implement:
      _on_tick(symbol, state) → list[Order] | None

    The base class handles:
      - Kafka producer/consumer setup
      - Price + sentiment state updates (background threads)
      - Halt state monitoring
      - Timed tick loop
      - Order placement with pre-trade risk checks
      - Local portfolio tracking
    """

    def __init__(self, client_id: str, symbols: list[str] = None):
        self.client_id = client_id
        self.symbols   = symbols or SYMBOL_LIST
        self._running  = False

        # per-symbol state — updated by background threads
        self._states : dict[str, BotSymbolState] = {
            s: BotSymbolState(s, SYMBOLS[s][1])
            for s in self.symbols
        }
        self._portfolio = BotPortfolio()
        self._lock      = threading.Lock()

        # Kafka
        self._producer = MarketProducer(client_id)

        self._price_consumer = MarketConsumer(
            group_id=f"{client_id}-prices",
            topics=[TOPIC_PRICE_UPDATE],
            offset="latest",
        )
        self._sentiment_consumer = MarketConsumer(
            group_id=f"{client_id}-sentiment",
            topics=[TOPIC_MARKET_SENTIMENT],
            offset="latest",
        )
        self._halt_consumer = MarketConsumer(
            group_id=f"{client_id}-halts",
            topics=[TOPIC_MARKET_HALT],
            offset="latest",
        )

        logger.info(f"[{client_id}] initialised")

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        """Start bot — spawns listener threads, runs tick loop."""
        self._running = True

        for target, name in [
            (self._price_loop,     f"{self.client_id}-price-listener"),
            (self._sentiment_loop, f"{self.client_id}-sentiment-listener"),
            (self._halt_loop,      f"{self.client_id}-halt-listener"),
        ]:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()

        logger.info(f"[{self.client_id}] started")
        try:
            self._tick_loop()
        finally:
            self._running = False
            self._producer.flush()
            logger.info(f"[{self.client_id}] stopped")

    def stop(self):
        self._running = False

    # ── Tick loop ─────────────────────────────────────────────────────────────

    def _tick_loop(self):
        """
        Main bot loop — fires every BOT_TICK_SECONDS.

        Each tick:
          1. For each symbol → call _on_tick(symbol, state)
          2. _on_tick returns list of Orders or None
          3. Each order passes pre-trade checks
          4. Valid orders sent to market-orders topic

        Timed approach (vs event-driven):
          Bots fire on a fixed interval rather than reacting
          to every single trade. This is more realistic —
          real algorithms have minimum decision intervals
          to avoid overtrading on noise.
        """
        while self._running:
            tick_start = time()

            for symbol in self.symbols:
                with self._lock:
                    state = self._states[symbol]

                if state.halted:
                    continue

                try:
                    orders = self._on_tick(symbol, state)
                except Exception as e:
                    logger.error(
                        f"[{self.client_id}] tick error "
                        f"symbol={symbol} err={e}"
                    )
                    continue

                if not orders:
                    continue

                for order in orders:
                    self._place_order(order, state)

            # sleep remainder of tick interval
            elapsed = time() - tick_start
            sleep_time = max(0.0, BOT_TICK_SECONDS - elapsed)
            sleep(sleep_time)

    # ── Abstract method ───────────────────────────────────────────────────────

    @abstractmethod
    def _on_tick(self, symbol: str,
                 state: BotSymbolState) -> list[Order] | None:
        """
        Called every BOT_TICK_SECONDS for each symbol.
        Return a list of Orders to place, or None/[] to skip.

        Each subclass implements its own strategy here:
          MarketMaker    → always returns 2 orders (bid + ask)
          MomentumBot    → returns 1 order if sentiment is strong
          RandomBot      → returns 1 random order
          MeanRevBot     → returns 1 order if RSI is extreme
        """
        ...

    # ── Order placement ───────────────────────────────────────────────────────

    def _place_order(self, order: Order, state: BotSymbolState):
        """
        Pre-trade risk checks then publish to market-orders.

        Checks:
          1. Symbol not halted
          2. Price is valid (> 0)
          3. Quantity is valid (> 0)
          4. Bot can afford buy orders
          5. Bot holds enough shares for sell orders
        """
        if state.halted:
            return

        if not order.price or order.price <= 0:
            if order.order_type == "limit":
                return

        if order.quantity <= 0:
            return

        # position checks
        if order.side == "buy":
            if not self._portfolio.can_afford(order.quantity, order.price or state.price):
                logger.debug(
                    f"[{self.client_id}] insufficient cash "
                    f"symbol={order.symbol}"
                )
                return
        else:
            if self._portfolio.position(order.symbol) < order.quantity:
                logger.debug(
                    f"[{self.client_id}] insufficient position "
                    f"symbol={order.symbol}"
                )
                return

        # send to Kafka
        self._producer.send_order(TOPIC_MARKET_ORDERS, order)
        self._producer.flush()

        # update local portfolio optimistically
        # (actual update comes from portfolio_ledger via trade-executed)
        if order.side == "buy" and order.price:
            self._portfolio.apply_buy(
                order.symbol, order.quantity, order.price
            )
        elif order.side == "sell" and order.price:
            self._portfolio.apply_sell(
                order.symbol, order.quantity, order.price
            )

        logger.debug(
            f"[{self.client_id}] placed {order.side} {order.order_type} "
            f"{order.quantity}x{order.symbol} @ {order.price}"
        )

    def _make_order(self, symbol: str, side: str,
                    order_type: str, quantity: int,
                    price: float | None = None) -> Order:
        """Convenience factory — builds an Order with client_id pre-filled."""
        return Order(
            client_id  = self.client_id,
            symbol     = symbol,
            side       = side,
            order_type = order_type,
            quantity   = quantity,
            price      = round(price, 2) if price else None,
        )

    # ── Background listeners ──────────────────────────────────────────────────

    def _price_loop(self):
        while self._running:
            msg = self._price_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            symbol = msg.get("symbol")
            if symbol in self._states:
                with self._lock:
                    self._states[symbol].update_from_price(msg)

    def _sentiment_loop(self):
        while self._running:
            msg = self._sentiment_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            symbol = msg.get("symbol")
            if symbol in self._states:
                with self._lock:
                    self._states[symbol].update_from_sentiment(msg)

    def _halt_loop(self):
        while self._running:
            msg = self._halt_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            symbol = msg.get("symbol")
            status = msg.get("status")
            if symbol in self._states:
                with self._lock:
                    self._states[symbol].halted = (status == "halted")
                logger.warning(
                    f"[{self.client_id}] {symbol} "
                    f"{'HALTED' if status == 'halted' else 'RESUMED'}"
                )