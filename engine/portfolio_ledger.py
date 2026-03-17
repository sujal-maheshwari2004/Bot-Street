"""
Portfolio Ledger — Tracks holdings, cash, and P&L per participant

Consumes `trade-executed` and updates each client's portfolio in real time.
Publishes `portfolio-snapshot` after every update.

Concepts implemented:
─────────────────────────────────────────────────────────────────────────────
AVERAGE COST BASIS
  When you buy shares at different prices, your cost basis is the
  weighted average of all purchase prices.
  avg_cost = total_cost_paid / total_shares_held

  Example:
    Buy 10 @ $100 → avg_cost = $100
    Buy 10 @ $120 → avg_cost = ($1000 + $1200) / 20 = $110

REALISED P&L
  Profit/loss locked in when you CLOSE a position (sell shares).
  realised_pnl += (sell_price - avg_cost) * quantity_sold

UNREALISED P&L
  Profit/loss on positions you still HOLD, marked to current market price.
  unrealised_pnl = (current_price - avg_cost) * quantity_held
  Fluctuates with market — not real until you sell.

POSITION LIMITS
  Prevents any single participant from:
  - Holding more than MAX_POSITION_SHARES of one symbol
  - Allocating more than MAX_POSITION_PCT of portfolio to one symbol
  Keeps the simulation balanced and prevents bot runaway.

SHARPE RATIO
  risk_adjusted_return = (mean_return - risk_free_rate) / std_dev_returns
  Higher is better. >1 is good, >2 is excellent, <0 means losing money
  risk-adjusted vs just holding cash.

MAX DRAWDOWN
  Largest peak-to-trough decline in portfolio value.
  Measures worst-case loss experienced. Key risk metric.

VALUE AT RISK (VaR 95%)
  The maximum loss you'd expect 95% of the time.
  "With 95% confidence, I won't lose more than $X in the next period."
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import threading
from collections import defaultdict
from time import time

from config import (
    TOPIC_TRADE_EXECUTED, TOPIC_PORTFOLIO_SNAP, TOPIC_PRICE_UPDATE,
    INITIAL_CASH, MAX_POSITION_PCT, MAX_POSITION_SHARES,
    VOLATILITY_WINDOW, SHARPE_RISK_FREE, VAR_CONFIDENCE,
    ALL_CLIENT_IDS,
)
from core.schemas import PortfolioSnapshot
from core.kafka_client import MarketProducer, MarketConsumer

logger = logging.getLogger(__name__)


# ── Per-client portfolio state ────────────────────────────────────────────────

class ClientPortfolio:
    """
    Holds the complete financial state for one participant.
    """

    def __init__(self, client_id: str):
        self.client_id     = client_id
        self.cash          = INITIAL_CASH
        self.holdings      : dict[str, int]   = defaultdict(int)    # symbol → qty
        self.avg_cost      : dict[str, float] = {}                   # symbol → avg price
        self.realised_pnl  = 0.0
        self._pnl_history  : list[float] = []   # for Sharpe + VaR
        self._peak_value   = INITIAL_CASH        # for max drawdown
        self.max_drawdown  = 0.0
        self._trade_count  = 0

    def can_buy(self, symbol: str, quantity: int, price: float,
                current_prices: dict) -> tuple[bool, str]:
        """
        Check position limits before allowing a buy.
        Returns (allowed, reason).
        """
        cost = quantity * price
        if cost > self.cash:
            return False, f"insufficient cash (have ${self.cash:.2f}, need ${cost:.2f})"

        new_qty = self.holdings[symbol] + quantity
        if new_qty > MAX_POSITION_SHARES:
            return False, f"position limit: max {MAX_POSITION_SHARES} shares per symbol"

        # check portfolio concentration
        total_value = self._total_value(current_prices) + cost
        position_value = new_qty * price
        if total_value > 0 and (position_value / total_value) > MAX_POSITION_PCT:
            return False, f"concentration limit: max {MAX_POSITION_PCT*100:.0f}% in one symbol"

        return True, ""

    def can_sell(self, symbol: str, quantity: int) -> tuple[bool, str]:
        """Check we actually hold enough shares to sell."""
        held = self.holdings.get(symbol, 0)
        if quantity > held:
            return False, f"insufficient shares (have {held}, selling {quantity})"
        return True, ""

    def apply_buy(self, symbol: str, quantity: int, price: float):
        """
        Update state when a buy trade executes.
        Recalculates average cost basis using weighted average.
        """
        cost = quantity * price
        self.cash -= cost

        prev_qty  = self.holdings[symbol]
        prev_cost = self.avg_cost.get(symbol, 0.0)

        new_qty = prev_qty + quantity
        if new_qty > 0:
            self.avg_cost[symbol] = (
                (prev_qty * prev_cost + quantity * price) / new_qty
            )
        self.holdings[symbol] = new_qty
        self._trade_count += 1

    def apply_sell(self, symbol: str, quantity: int, price: float):
        """
        Update state when a sell trade executes.
        Locks in realised P&L.
        """
        avg = self.avg_cost.get(symbol, price)
        pnl = (price - avg) * quantity
        self.realised_pnl      += pnl
        self.cash              += quantity * price
        self.holdings[symbol]  -= quantity

        if self.holdings[symbol] == 0:
            del self.avg_cost[symbol]
            del self.holdings[symbol]

        self._pnl_history.append(pnl)
        if len(self._pnl_history) > VOLATILITY_WINDOW:
            self._pnl_history.pop(0)

        self._trade_count += 1

    def unrealised_pnl(self, current_prices: dict) -> float:
        """Mark open positions to current market price."""
        total = 0.0
        for symbol, qty in self.holdings.items():
            if symbol in current_prices and symbol in self.avg_cost:
                total += (current_prices[symbol] - self.avg_cost[symbol]) * qty
        return round(total, 2)

    def update_drawdown(self, current_prices: dict):
        """
        Track peak portfolio value and compute max drawdown.
        Called after every trade.
        """
        value = self._total_value(current_prices)
        if value > self._peak_value:
            self._peak_value = value
        drawdown = (self._peak_value - value) / self._peak_value if self._peak_value > 0 else 0.0
        self.max_drawdown = max(self.max_drawdown, drawdown)

    def sharpe_ratio(self) -> float | None:
        """
        Sharpe = (mean_pnl - risk_free) / std_pnl
        Requires at least 5 closed trades to be meaningful.
        """
        if len(self._pnl_history) < 5:
            return None
        import statistics
        mean = statistics.mean(self._pnl_history)
        std  = statistics.stdev(self._pnl_history)
        if std == 0:
            return None
        return round((mean - SHARPE_RISK_FREE) / std, 3)

    def var_95(self) -> float | None:
        """
        Historical VaR at 95% confidence.
        Sort past P&L, take 5th percentile (worst 5% of outcomes).
        Negative value = expected loss.
        """
        if len(self._pnl_history) < 10:
            return None
        sorted_pnl = sorted(self._pnl_history)
        idx = int(len(sorted_pnl) * (1 - VAR_CONFIDENCE))
        return round(sorted_pnl[idx], 2)

    def _total_value(self, current_prices: dict) -> float:
        """Cash + mark-to-market value of all holdings."""
        holdings_value = sum(
            qty * current_prices.get(sym, self.avg_cost.get(sym, 0))
            for sym, qty in self.holdings.items()
        )
        return self.cash + holdings_value

    def to_snapshot(self, current_prices: dict) -> PortfolioSnapshot:
        self.update_drawdown(current_prices)
        return PortfolioSnapshot(
            client_id      = self.client_id,
            holdings       = dict(self.holdings),
            avg_cost       = dict(self.avg_cost),
            cash           = round(self.cash, 2),
            realised_pnl   = round(self.realised_pnl, 2),
            unrealised_pnl = self.unrealised_pnl(current_prices),
            sharpe         = self.sharpe_ratio(),
            max_drawdown   = round(self.max_drawdown, 4),
            var_95         = self.var_95(),
        )


# ── Portfolio Ledger service ──────────────────────────────────────────────────

class PortfolioLedger:
    """
    Kafka consumer service — one instance manages all client portfolios.

    Listens to:
      - trade-executed  → update buyer + seller portfolios
      - price-update    → keep current_prices fresh for unrealised P&L

    Publishes:
      - portfolio-snapshot → after every trade
    """

    def __init__(self):
        self._portfolios     : dict[str, ClientPortfolio] = {}
        self._current_prices : dict[str, float] = {}
        self._lock = threading.Lock()

        # pre-create portfolios for all known participants
        for client_id in ALL_CLIENT_IDS:
            self._portfolios[client_id] = ClientPortfolio(client_id)

        self._trade_consumer = MarketConsumer(
            group_id="portfolio-ledger-trades",
            topics=[TOPIC_TRADE_EXECUTED],
            offset="latest",
        )
        self._price_consumer = MarketConsumer(
            group_id="portfolio-ledger-prices",
            topics=[TOPIC_PRICE_UPDATE],
            offset="latest",
        )
        self._producer = MarketProducer("portfolio-ledger")

        self._running = False
        logger.info("[portfolio-ledger] initialised")

    def start(self):
        """Start ledger — spawns price listener thread, runs trade loop."""
        self._running = True

        price_thread = threading.Thread(
            target=self._price_loop,
            name="ledger-price-listener",
            daemon=True,
        )
        price_thread.start()

        logger.info("[portfolio-ledger] started")
        try:
            self._trade_loop()
        finally:
            self._running = False
            self._producer.flush()
            logger.info("[portfolio-ledger] stopped")

    def stop(self):
        self._running = False

    def get_portfolio(self, client_id: str) -> ClientPortfolio | None:
        """Direct access for API — returns live portfolio object."""
        return self._portfolios.get(client_id)

    def get_snapshot(self, client_id: str) -> PortfolioSnapshot | None:
        portfolio = self._portfolios.get(client_id)
        if portfolio is None:
            return None
        with self._lock:
            return portfolio.to_snapshot(self._current_prices)

    def get_leaderboard(self) -> list[dict]:
        """
        Returns all bots + user ranked by total P&L.
        Used by dashboard bot leaderboard panel.
        """
        rows = []
        with self._lock:
            for client_id, p in self._portfolios.items():
                snap = p.to_snapshot(self._current_prices)
                total_pnl = snap.realised_pnl + snap.unrealised_pnl
                rows.append({
                    "client_id"    : client_id,
                    "total_pnl"    : total_pnl,
                    "realised_pnl" : snap.realised_pnl,
                    "unrealised_pnl": snap.unrealised_pnl,
                    "cash"         : snap.cash,
                    "sharpe"       : snap.sharpe,
                    "max_drawdown" : snap.max_drawdown,
                    "var_95"       : snap.var_95,
                    "trade_count"  : p._trade_count,
                })
        return sorted(rows, key=lambda r: r["total_pnl"], reverse=True)

    # ── Internal loops ────────────────────────────────────────────────────────

    def _trade_loop(self):
        while self._running:
            msg = self._trade_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            self._apply_trade(msg)

    def _price_loop(self):
        while self._running:
            msg = self._price_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            symbol = msg.get("symbol")
            price  = msg.get("price")
            if symbol and price:
                with self._lock:
                    self._current_prices[symbol] = price

    def _apply_trade(self, msg: dict):
        """
        Apply a trade to both buyer and seller portfolios.
        Creates portfolio on-the-fly for unknown client IDs
        (handles bots that register after startup).
        """
        symbol    = msg["symbol"]
        buyer_id  = msg["buyer_id"]
        seller_id = msg["seller_id"]
        quantity  = msg["quantity"]
        price     = msg["price"]

        with self._lock:
            # ensure portfolios exist
            if buyer_id not in self._portfolios:
                self._portfolios[buyer_id] = ClientPortfolio(buyer_id)
            if seller_id not in self._portfolios:
                self._portfolios[seller_id] = ClientPortfolio(seller_id)

            buyer  = self._portfolios[buyer_id]
            seller = self._portfolios[seller_id]

            buyer.apply_buy(symbol, quantity, price)
            seller.apply_sell(symbol, quantity, price)

            logger.debug(
                f"[ledger] trade {symbol} qty={quantity} price={price} "
                f"buyer={buyer_id} seller={seller_id}"
            )

            # publish snapshots for both sides
            for portfolio in (buyer, seller):
                snap = portfolio.to_snapshot(self._current_prices)
                self._producer.send(TOPIC_PORTFOLIO_SNAP, snap)

        self._producer.flush()