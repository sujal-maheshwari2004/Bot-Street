"""
Market Maker Bot — Liquidity Provider

The market maker's job is to always have both a buy and sell
order in the book, profiting from the bid-ask spread.

Strategy:
  Every tick → post one bid below mid, one ask above mid
  Spread width adjusts based on:
    - Sentiment (bullish → skew quotes higher)
    - Volatility (high vol → widen spread for protection)
    - OFI (strong buy pressure → raise both bid and ask)

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Market Making          Providing liquidity, earning the spread
  Quote Skewing          Adjusting quotes based on inventory/sentiment
  Adverse Selection      Risk of trading against informed participants
  Inventory Risk         Danger of accumulating too much one-sided position
─────────────────────────────────────────────────────────────────────────────

P&L model:
  Every time someone buys from us (hits our ask) → we earn ask - avg_cost
  Every time someone sells to us (hits our bid)  → we earn avg_cost - bid
  Net profit per round trip = full spread

Risk:
  If market moves strongly one way, we accumulate a losing position.
  Skewing quotes is how market makers manage inventory risk:
    Long too much PEAR? → lower bid (buy less) + lower ask (sell faster)
    Short too much PEAR? → raise bid (buy faster) + raise ask (sell less)
"""

import logging
from participants.base_bot import BaseBot, BotSymbolState
from core.schemas import Order
from config import (
    MARKET_MAKER_SPREAD, MARKET_MAKER_QTY,
    MARKET_MAKER_CLIENT_ID, TICK_SIZE,
)

logger = logging.getLogger(__name__)


class MarketMakerBot(BaseBot):

    def __init__(self):
        super().__init__(client_id=MARKET_MAKER_CLIENT_ID)
        # track inventory skew per symbol
        # positive = long (bought more than sold)
        # negative = short (sold more than bought)
        self._inventory : dict[str, int] = {}

    def _on_tick(self, symbol: str,
                 state: BotSymbolState) -> list[Order] | None:
        """
        Post one bid and one ask every tick.

        Base spread = MARKET_MAKER_SPREAD ($0.10 each side of mid)
        Adjustments:
          Sentiment bullish → shift both quotes up by strength * 0.05
          Sentiment bearish → shift both quotes down by strength * 0.05
          OFI > 0.5 → raise quotes (buy pressure detected)
          OFI < -0.5 → lower quotes (sell pressure detected)
          Inventory long → lower bid, raise ask (slow buying, push selling)
          Inventory short → raise bid, lower ask (push buying, slow selling)
        """
        mid = state.price
        if mid <= 0:
            return None

        half_spread = MARKET_MAKER_SPREAD / 2

        # ── Sentiment adjustment ──────────────────────────────────────
        # Bullish: shift quotes up so we sell higher, buy higher
        # Bearish: shift quotes down
        sentiment_shift = 0.0
        if state.sentiment == "bullish":
            sentiment_shift = +state.strength * 0.05
        elif state.sentiment == "bearish":
            sentiment_shift = -state.strength * 0.05

        # ── OFI adjustment ────────────────────────────────────────────
        # Strong buy OFI → raise quotes (demand > supply)
        ofi_shift = 0.0
        if state.ofi is not None:
            ofi_shift = state.ofi * 0.03

        # ── Inventory skew ────────────────────────────────────────────
        # Too long → widen ask side (want to sell), tighten bid
        # Too short → widen bid side (want to buy), tighten ask
        inventory  = self._inventory.get(symbol, 0)
        inv_skew   = -inventory * 0.001   # small nudge per share held

        # ── Final quote calculation ───────────────────────────────────
        total_shift = sentiment_shift + ofi_shift + inv_skew
        bid_price   = mid - half_spread + total_shift
        ask_price   = mid + half_spread + total_shift

        # enforce minimum spread of 1 tick
        if ask_price - bid_price < TICK_SIZE:
            ask_price = bid_price + TICK_SIZE

        # enforce positive prices
        bid_price = max(TICK_SIZE, round(bid_price, 2))
        ask_price = max(bid_price + TICK_SIZE, round(ask_price, 2))

        # ── Quantity adjustment ───────────────────────────────────────
        # Post smaller size when inventory is extreme
        qty = max(1, MARKET_MAKER_QTY - abs(inventory) // 10)

        bid = self._make_order(symbol, "buy",  "limit", qty, bid_price)
        ask = self._make_order(symbol, "sell", "limit", qty, ask_price)

        # update inventory tracking
        # (approximate — real fills come from portfolio_ledger)
        self._inventory[symbol] = inventory

        logger.debug(
            f"[market-maker] {symbol} "
            f"bid={bid_price} ask={ask_price} "
            f"shift={total_shift:+.4f} inv={inventory}"
        )

        return [bid, ask]