"""
Mean Reversion Bot — Fades Extremes Back to Fair Value

Sells when price is overbought (RSI > 70, price > VWAP + threshold)
Buys  when price is oversold  (RSI < 30, price < VWAP - threshold)

The core belief: prices deviate from fair value temporarily,
then revert. The bot profits from the reversion.

Strategy:
  Primary signal:  RSI extreme (overbought or oversold)
  Confirmation:    Price deviation from VWAP
  Both must agree before placing an order.

─────────────────────────────────────────────────────────────────────────────
CONCEPTS COVERED:
  Mean Reversion     Prices return to average over time
  RSI Signals        Overbought/oversold thresholds
  VWAP as Fair Value Institutional benchmark price
  Signal Confluence  Requiring 2 indicators to agree (reduces false signals)
  Contrarian Trading Going against the crowd at extremes
─────────────────────────────────────────────────────────────────────────────
"""

import logging
from participants.base_bot import BaseBot, BotSymbolState
from core.schemas import Order
from config import (
    MEAN_REV_CLIENT_ID, MEAN_REVERSION_QTY,
    RSI_OVERBOUGHT, RSI_OVERSOLD, VWAP_DEVIATION_PCT,
)

logger = logging.getLogger(__name__)


class MeanReversionBot(BaseBot):

    def __init__(self):
        super().__init__(client_id=MEAN_REV_CLIENT_ID)

    def _on_tick(self, symbol: str,
                 state: BotSymbolState) -> list[Order] | None:
        """
        Wait for RSI extreme + VWAP deviation, then fade the move.

        Both conditions must be true simultaneously:
          Overbought: RSI > 70 AND price > VWAP * (1 + threshold)
          Oversold:   RSI < 30 AND price < VWAP * (1 - threshold)

        Why require both?
          RSI alone gives many false signals in strong trends.
          VWAP deviation confirms the price is genuinely stretched.
          Requiring confluence reduces false signals significantly.

        Exit: mean reversion bots exit when price returns to VWAP.
          (not implemented in POC — orders have TTL so they expire)
        """
        rsi  = state.rsi
        vwap = state.vwap
        price = state.price

        # need both indicators to be available
        if rsi is None or vwap is None or price <= 0:
            return None

        vwap_upper = vwap * (1 + VWAP_DEVIATION_PCT)
        vwap_lower = vwap * (1 - VWAP_DEVIATION_PCT)

        # ── Overbought → SELL ─────────────────────────────────────────
        if rsi > RSI_OVERBOUGHT and price > vwap_upper:
            held = self._portfolio.position(symbol)
            if held < MEAN_REVERSION_QTY:
                # no position to sell — skip
                # (short selling not implemented in POC)
                return None
            qty   = min(MEAN_REVERSION_QTY, held)
            # sell slightly below current price for quick fill
            sell_price = round(price * 0.999, 2)

            logger.debug(
                f"[mean-rev] {symbol} SELL signal "
                f"rsi={rsi:.1f} price={price} vwap={vwap:.2f}"
            )
            return [self._make_order(symbol, "sell", "limit",
                                     qty, sell_price)]

        # ── Oversold → BUY ────────────────────────────────────────────
        if rsi < RSI_OVERSOLD and price < vwap_lower:
            if not self._portfolio.can_afford(MEAN_REVERSION_QTY, price):
                return None
            # buy slightly above current price for quick fill
            buy_price = round(price * 1.001, 2)

            logger.debug(
                f"[mean-rev] {symbol} BUY signal "
                f"rsi={rsi:.1f} price={price} vwap={vwap:.2f}"
            )
            return [self._make_order(symbol, "buy", "limit",
                                     MEAN_REVERSION_QTY, buy_price)]

        return None