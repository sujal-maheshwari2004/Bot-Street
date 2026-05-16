"""
Price Feed — Combines Trade Stream + All Quant Modules

Consumes trade-executed, runs all indicators and microstructure
calculations, then publishes a rich PriceUpdate message.

This is the single source of truth for current market state.
Every bot and the dashboard read from price-update — not directly
from trade-executed. This keeps all indicator logic in one place.

Flow:
  trade-executed
      → IndicatorEngine.update()
      → MicrostructureEngine.update_trade()
      → RiskEngine.record_price()
      → build PriceUpdate
      → publish price-update
"""

import logging
import threading
from time import time
from collections import defaultdict

from config import (
    TOPIC_TRADE_EXECUTED, TOPIC_PRICE_UPDATE, TOPIC_CANDLES,
    SYMBOLS, SYMBOL_LIST,
)
from core.schemas import PriceUpdate
from core.kafka_client import MarketProducer, MarketConsumer
from market.quant.indicators import IndicatorEngine
from market.quant.microstructure import MicrostructureEngine
from market.quant.risk import RiskEngine

logger = logging.getLogger(__name__)


class PriceFeed:
    def __init__(self):
        self._indicators     : dict[str, IndicatorEngine]      = {}
        self._microstructure : dict[str, MicrostructureEngine] = {}
        self._risk           : dict[str, RiskEngine]           = {}

        self._last_price  : dict[str, float] = {}
        self._total_volume: dict[str, int]   = defaultdict(int)
        self._session_start = time()

        for symbol, (_, initial_price, _) in SYMBOLS.items():
            self._indicators[symbol]     = IndicatorEngine(symbol)
            self._microstructure[symbol] = MicrostructureEngine(symbol)
            self._risk[symbol]           = RiskEngine(symbol)
            self._last_price[symbol]     = initial_price

        self._trade_consumer = MarketConsumer(
            group_id="price-feed-trades",
            topics=[TOPIC_TRADE_EXECUTED],
            offset="latest",
        )
        self._candle_consumer = MarketConsumer(
            group_id="price-feed-candles",
            topics=[TOPIC_CANDLES],
            offset="latest",
        )
        self._producer = MarketProducer("price-feed")
        self._running  = False

        logger.info("[price-feed] initialised")

    def start(self):
        self._running = True

        # ── Seed prices ──────────────────────────────────────────────────────
        # Publish initial prices for every symbol so bots have a reference
        # price on cold start. Without this, bots wait for a price-update
        # that never arrives (because no trades have executed yet), creating
        # a deadlock where nothing ever trades.
        logger.info("[price-feed] publishing seed prices")
        for symbol, (_, initial_price, _) in SYMBOLS.items():
            half_spread = 0.05
            seed = PriceUpdate(
                symbol      = symbol,
                price       = initial_price,
                vwap        = initial_price,
                bid         = round(initial_price - half_spread, 2),
                ask         = round(initial_price + half_spread, 2),
                spread      = round(half_spread * 2, 2),
                volume      = 0,
                rsi         = None,
                macd        = None,
                macd_signal = None,
                bb_upper    = None,
                bb_lower    = None,
                ema_short   = None,
                ema_long    = None,
                ofi         = 0.0,
            )
            self._producer.send(TOPIC_PRICE_UPDATE, seed)
            logger.info(f"[price-feed] seed {symbol} @ {initial_price}")
        self._producer.flush()
        # ─────────────────────────────────────────────────────────────────────

        candle_thread = threading.Thread(
            target=self._candle_loop,
            name="price-feed-candles",
            daemon=True,
        )
        candle_thread.start()

        logger.info("[price-feed] started")
        try:
            self._trade_loop()
        finally:
            self._running = False
            self._producer.flush()
            logger.info("[price-feed] stopped")

    def stop(self):
        self._running = False

    def get_price(self, symbol: str) -> float | None:
        return self._last_price.get(symbol)

    def get_all_prices(self) -> dict[str, float]:
        return dict(self._last_price)

    # ── Internal loops ────────────────────────────────────────────────────────

    def _trade_loop(self):
        while self._running:
            msg = self._trade_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            self._process_trade(msg)

    def _candle_loop(self):
        while self._running:
            msg = self._candle_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            symbol = msg.get("symbol")
            if symbol not in self._indicators:
                continue
            self._indicators[symbol].update_candle(
                high  = msg["high"],
                low   = msg["low"],
                close = msg["close"],
            )

    def _process_trade(self, msg: dict):
        symbol   = msg.get("symbol")
        price    = msg.get("price")
        quantity = msg.get("quantity")
        side     = self._infer_side(msg)
        ts       = msg.get("timestamp", time())

        if not symbol or not price or symbol not in SYMBOL_LIST:
            return

        ind   = self._indicators[symbol]
        micro = self._microstructure[symbol]
        risk  = self._risk[symbol]

        ind.update(price, quantity)
        micro.update_trade(price, quantity, side, ts)
        risk.record_price(price)

        self._last_price[symbol]   = price
        self._total_volume[symbol] += quantity

        micro_snap = micro.snapshot()
        ind_snap   = ind.snapshot()

        update = PriceUpdate(
            symbol      = symbol,
            price       = price,
            vwap        = ind_snap.vwap or price,
            bid         = micro_snap.spread.bid or price,
            ask         = micro_snap.spread.ask or price,
            spread      = micro_snap.spread.absolute_spread or 0.0,
            volume      = self._total_volume[symbol],
            rsi         = ind_snap.rsi,
            macd        = ind_snap.macd,
            macd_signal = ind_snap.macd_signal,
            bb_upper    = ind_snap.bb_upper,
            bb_lower    = ind_snap.bb_lower,
            ema_short   = ind_snap.ema_short,
            ema_long    = ind_snap.ema_long,
            ofi         = micro_snap.flow.ofi,
            timestamp   = ts,
        )

        self._producer.send(TOPIC_PRICE_UPDATE, update)
        self._producer.flush()

        logger.debug(
            f"[price-feed] {symbol} price={price} "
            f"rsi={ind_snap.rsi} ofi={micro_snap.flow.ofi}"
        )

    def _infer_side(self, trade: dict) -> str:
        buyer_id  = trade.get("buyer_id", "")
        seller_id = trade.get("seller_id", "")

        if "market-maker" in seller_id:
            return "buy"
        if "market-maker" in buyer_id:
            return "sell"
        if buyer_id == "user":
            return "buy"
        if seller_id == "user":
            return "sell"

        return "buy"

    def update_order_book_quotes(self, symbol: str, bid: float, ask: float):
        if symbol in self._microstructure:
            self._microstructure[symbol].update_quotes(bid, ask)