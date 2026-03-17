"""
Momentum Bot — Trend Follower

Buys into bullish sentiment, sells into bearish sentiment.
Scales position size with sentiment strength.

Strategy:
  sentiment=bullish AND strength > threshold → BUY
  sentiment=bearish AND strength > threshold → SELL
  neutral or weak signal → do nothing

Also uses EMA crossover as confirmation:
  EMA(9) > EMA(21) → uptrend confirmed → only take buy signals
  EMA(9) < EMA(21) → downtrend confirmed → only take sell signals

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Momentum Trading       Buying strength, selling weakness
  Signal Confirmation    Requiring multiple indicators to agree
  Position Sizing        Scaling size with conviction level
  Trend Following        EMA crossover as trend filter
─────────────────────────────────────────────────────────────────────────────
"""

import logging
from participants.base_bot import BaseBot, BotSymbolState
from core.schemas import Order
from config import (
    MOMENTUM_CLIENT_ID, MOMENTUM_QTY, MOMENTUM_THRESHOLD,
)

logger = logging.getLogger(__name__)


class MomentumBot(BaseBot):

    def __init__(self):
        super().__init__(client_id=MOMENTUM_CLIENT_ID)

    def _on_tick(self, symbol: str,
                 state: BotSymbolState) -> list[Order] | None:
        """
        Only trade when sentiment is strong AND EMA confirms trend.

        Position sizing:
          base_qty = MOMENTUM_QTY
          scaled_qty = base_qty * strength  (more conviction = bigger size)
          minimum 1 share
        """
        if state.sentiment is None:
            return None
        if state.strength < MOMENTUM_THRESHOLD:
            return None

        # EMA crossover confirmation
        # if indicators not yet available → skip confirmation check
        ema_bullish = None
        if state.ema_short and state.ema_long:
            ema_bullish = state.ema_short > state.ema_long

        scaled_qty = max(1, int(MOMENTUM_QTY * state.strength))

        if state.sentiment == "bullish":
            # skip if EMA says downtrend
            if ema_bullish is False:
                return None
            # buy slightly above mid to ensure fill
            price = round(state.price * 1.001, 2)
            return [self._make_order(symbol, "buy", "limit",
                                     scaled_qty, price)]

        elif state.sentiment == "bearish":
            # skip if EMA says uptrend
            if ema_bullish is True:
                return None
            # check we have shares to sell
            if self._portfolio.position(symbol) < scaled_qty:
                return None
            # sell slightly below mid to ensure fill
            price = round(state.price * 0.999, 2)
            return [self._make_order(symbol, "sell", "limit",
                                     scaled_qty, price)]

        return None