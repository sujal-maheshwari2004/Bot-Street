"""
Random Bot — Noise Trader

Places random buy/sell orders at random prices and sizes.
Ensures there is always baseline order flow even when
other bots are inactive (neutral sentiment, no signals).

Role in the simulation:
  Without noise, the market would freeze whenever sentiment
  is neutral — no trades, no price discovery, no signals.
  The random bot keeps the market alive and prevents
  artificial flatlines in the price feed.

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Noise Trading      Uninformed trading that adds randomness
  Market Liquidity   Why markets need constant order flow
  Price Discovery    How random flow helps reveal true price
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import random
from participants.base_bot import BaseBot, BotSymbolState
from core.schemas import Order
from config import RANDOM_CLIENT_ID, RANDOM_QTY_MAX, TICK_SIZE

logger = logging.getLogger(__name__)


class RandomBot(BaseBot):

    def __init__(self, seed: int = None):
        super().__init__(client_id=RANDOM_CLIENT_ID)
        if seed:
            random.seed(seed)

    def _on_tick(self, symbol: str,
                 state: BotSymbolState) -> list[Order] | None:
        """
        Random order every tick.

        Price: current price ± random noise up to 1%
        Size:  random between 1 and RANDOM_QTY_MAX
        Side:  random buy or sell (60/40 slight buy bias
               to match typical retail flow)
        Type:  always limit (market orders would be too aggressive
               for a noise trader and would widen spreads too much)
        """
        price = state.price
        if price <= 0:
            return None

        # random price within 1% of current price
        noise    = random.uniform(-0.01, 0.01)
        order_price = round(max(TICK_SIZE, price * (1 + noise)), 2)

        # random quantity
        qty = random.randint(1, RANDOM_QTY_MAX)

        # slight buy bias (60% buy, 40% sell) — mimics retail behaviour
        side = "buy" if random.random() < 0.6 else "sell"

        # sell check — can't sell what we don't have
        if side == "sell":
            held = self._portfolio.position(symbol)
            if held == 0:
                side = "buy"   # flip to buy if no position
            else:
                qty = min(qty, held)

        return [self._make_order(symbol, side, "limit", qty, order_price)]