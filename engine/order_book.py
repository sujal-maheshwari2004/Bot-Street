"""
Limit Order Book — Price-Time Priority Matching Engine

Core concepts implemented here:
─────────────────────────────────────────────────────────────────────────────
LIMIT ORDER BOOK
  Two sorted sides:
  - Bids (buy orders)  → sorted HIGH to LOW  (best bid = highest price)
  - Asks (sell orders) → sorted LOW to HIGH  (best ask = lowest price)

  A match occurs when best_bid >= best_ask (prices cross)

PRICE-TIME PRIORITY
  Orders at the same price level are filled oldest-first (FIFO).
  This rewards participants who post orders early.

PARTIAL FILLS
  If an incoming order is larger than the best resting order,
  the resting order fills completely and the remainder of the
  incoming order continues matching down the book.

MARKET ORDERS
  No price specified — fills immediately at whatever price is available.
  Walks the book until filled or liquidity runs out.

SPREAD
  The gap between best ask and best bid.
  Tight spread = liquid market. Wide spread = illiquid market.

ORDER FLOW IMBALANCE (OFI)
  (buy_volume - sell_volume) / total_volume
  Positive OFI → more buying pressure → price likely to rise
  Negative OFI → more selling pressure → price likely to fall
─────────────────────────────────────────────────────────────────────────────
"""

from collections import deque
from dataclasses import dataclass, field
from time import time
from typing import Literal

# ── Internal order representation ────────────────────────────────────────────
@dataclass
class BookOrder:
    """
    Resting order sitting in the book waiting to be matched.
    Tracks remaining quantity for partial fill support.
    """
    order_id   : str
    client_id  : str
    side       : Literal["buy", "sell"]
    price      : float
    quantity   : int                          # original quantity
    remaining  : int                          # quantity left after partial fill
    timestamp  : float = field(default_factory=time)

    @property
    def is_filled(self) -> bool:
        return self.remaining <= 0


# ── Match result ──────────────────────────────────────────────────────────────
@dataclass
class MatchResult:
    """
    Returned by the order book after attempting to match an order.
    Contains all trades generated + any unfilled remainder.
    """
    trades     : list        # list of TradeEvent
    remaining  : int         # quantity that could not be filled
    filled     : int         # total quantity filled


@dataclass
class TradeEvent:
    """
    A single fill generated inside the order book.
    The matching engine converts these into Trade schema objects.
    """
    buyer_id      : str
    seller_id     : str
    buy_order_id  : str
    sell_order_id : str
    price         : float
    quantity      : int
    timestamp     : float = field(default_factory=time)


# ── Order Book ────────────────────────────────────────────────────────────────
class OrderBook:
    """
    In-memory limit order book for a single symbol.

    Data structures:
    ─────────────────────────────────────────────────────────────────────────
    _bids: dict[price → deque[BookOrder]]
      Keyed by price level. Each level holds a FIFO queue of orders.
      Best bid = max(self._bids.keys())

    _asks: dict[price → deque[BookOrder]]
      Keyed by price level. Each level holds a FIFO queue of orders.
      Best ask = min(self._asks.keys())

    _order_index: dict[order_id → BookOrder]
      Fast lookup for cancellation and TTL expiry.
    ─────────────────────────────────────────────────────────────────────────

    Why dicts of deques?
      - O(1) access to a price level
      - O(1) FIFO ordering within a level (time priority)
      - O(1) cancel via order index
    """

    def __init__(self, symbol: str):
        self.symbol       = symbol
        self._bids        : dict[float, deque[BookOrder]] = {}
        self._asks        : dict[float, deque[BookOrder]] = {}
        self._order_index : dict[str, BookOrder] = {}

        # Volume tracking for OFI calculation
        self._buy_volume  = 0
        self._sell_volume = 0

    # ── Public interface ──────────────────────────────────────────────────────

    def add_order(self, order_id: str, client_id: str, side: str,
                  order_type: str, price: float | None, quantity: int,
                  timestamp: float) -> MatchResult:
        """
        Entry point for all new orders.
        - Market orders match immediately, no resting
        - Limit orders attempt to match, remainder rests in book
        """
        if order_type == "market":
            return self._match_market(order_id, client_id, side, quantity, timestamp)
        else:
            return self._match_limit(order_id, client_id, side, price, quantity, timestamp)

    def cancel_order(self, order_id: str) -> BookOrder | None:
        """
        Remove a resting order from the book.
        Returns the cancelled order or None if not found.
        """
        order = self._order_index.pop(order_id, None)
        if order is None:
            return None

        book = self._bids if order.side == "buy" else self._asks
        level = book.get(order.price)
        if level:
            # rebuild deque without this order (rare operation)
            book[order.price] = deque(o for o in level if o.order_id != order_id)
            if not book[order.price]:
                del book[order.price]
        return order

    def expire_orders(self, now: float) -> list[BookOrder]:
        """
        Remove all orders where expires_at <= now.
        Called periodically by the matching engine (TTL sweep).
        Returns list of expired orders for publishing to order-expired topic.
        """
        expired = []
        for order_id, order in list(self._order_index.items()):
            # expires_at stored on BookOrder via timestamp + TTL
            # We tag it when adding — see _add_to_book
            if hasattr(order, 'expires_at') and order.expires_at <= now:
                removed = self.cancel_order(order_id)
                if removed:
                    expired.append(removed)
        return expired

    def get_best_bid(self) -> float | None:
        """Highest buy price currently in the book."""
        return max(self._bids.keys()) if self._bids else None

    def get_best_ask(self) -> float | None:
        """Lowest sell price currently in the book."""
        return min(self._asks.keys()) if self._asks else None

    def get_spread(self) -> float | None:
        """
        Bid-ask spread = best ask - best bid.
        Tight spread signals high liquidity.
        None if either side of book is empty.
        """
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid is None or ask is None:
            return None
        return round(ask - bid, 4)

    def get_mid_price(self) -> float | None:
        """
        Mid price = (best bid + best ask) / 2
        Used by market makers to anchor their quotes.
        """
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        if bid is None or ask is None:
            return None
        return round((bid + ask) / 2, 4)

    def get_order_flow_imbalance(self) -> float:
        """
        OFI = (buy_volume - sell_volume) / total_volume
        Range: -1.0 (pure sell pressure) to +1.0 (pure buy pressure)
        0.0 = balanced flow

        Strong predictor of short-term price direction.
        Positive OFI → price likely to tick up
        Negative OFI → price likely to tick down
        """
        total = self._buy_volume + self._sell_volume
        if total == 0:
            return 0.0
        return round((self._buy_volume - self._sell_volume) / total, 4)

    def get_depth(self, levels: int = 5) -> dict:
        """
        Order book depth snapshot — top N price levels each side.
        Used by dashboard and API.

        Returns:
            {
                "bids": [(price, total_qty), ...],  # high → low
                "asks": [(price, total_qty), ...],  # low → high
            }
        """
        def aggregate(book: dict, reverse: bool) -> list[tuple]:
            levels_data = []
            for price in sorted(book.keys(), reverse=reverse):
                total_qty = sum(o.remaining for o in book[price])
                if total_qty > 0:
                    levels_data.append((price, total_qty))
                if len(levels_data) >= levels:
                    break
            return levels_data

        return {
            "bids": aggregate(self._bids, reverse=True),
            "asks": aggregate(self._asks, reverse=False),
        }

    def get_market_impact(self, side: str, quantity: int) -> float | None:
        """
        Estimate price impact of a market order of given size.
        Simulates walking the book without actually executing.

        Market impact = (avg fill price - mid price) / mid price
        Larger orders → higher impact → more slippage
        """
        mid = self.get_mid_price()
        if mid is None:
            return None

        book = self._asks if side == "buy" else self._bids
        reverse = side == "sell"

        remaining = quantity
        total_cost = 0.0

        for price in sorted(book.keys(), reverse=reverse):
            for order in book[price]:
                fill_qty = min(remaining, order.remaining)
                total_cost += fill_qty * price
                remaining -= fill_qty
                if remaining <= 0:
                    break
            if remaining <= 0:
                break

        if remaining > 0:
            return None  # not enough liquidity to fill

        avg_fill = total_cost / quantity
        return round((avg_fill - mid) / mid, 6)

    # ── Matching logic ────────────────────────────────────────────────────────

    def _match_market(self, order_id: str, client_id: str, side: str,
                      quantity: int, timestamp: float) -> MatchResult:
        """
        Market order — fill at best available price, walk book if needed.
        No remainder rests in book if unfilled (market orders are immediate-or-cancel).
        """
        trades = []
        remaining = quantity

        # buy market order hits the asks (lowest ask first)
        # sell market order hits the bids (highest bid first)
        book   = self._asks if side == "buy" else self._bids
        reverse = side == "sell"

        for price in sorted(book.keys(), reverse=reverse):
            if remaining <= 0:
                break
            level = book[price]
            while level and remaining > 0:
                resting = level[0]
                fill_qty = min(remaining, resting.remaining)

                trade = self._create_trade(
                    side, client_id, order_id, resting, fill_qty, price
                )
                trades.append(trade)

                resting.remaining -= fill_qty
                remaining -= fill_qty

                if resting.is_filled:
                    level.popleft()
                    self._order_index.pop(resting.order_id, None)

            if not level:
                del book[price]

        filled = quantity - remaining
        self._update_volume(side, filled)

        return MatchResult(trades=trades, remaining=remaining, filled=filled)

    def _match_limit(self, order_id: str, client_id: str, side: str,
                     price: float, quantity: int, timestamp: float) -> MatchResult:
        """
        Limit order — match aggressively first, then rest remainder in book.

        A limit buy at $100 will:
          1. Fill against any asks <= $100 (aggressive matching)
          2. Rest remaining quantity at $100 in the bid side

        A limit sell at $100 will:
          1. Fill against any bids >= $100
          2. Rest remaining quantity at $100 in the ask side
        """
        trades = []
        remaining = quantity

        # aggressive matching phase
        book    = self._asks if side == "buy" else self._bids
        reverse = side == "sell"

        for level_price in sorted(book.keys(), reverse=reverse):
            if remaining <= 0:
                break

            # stop if prices no longer cross
            if side == "buy"  and level_price > price:
                break
            if side == "sell" and level_price < price:
                break

            level = book[level_price]
            while level and remaining > 0:
                resting = level[0]
                fill_qty = min(remaining, resting.remaining)

                trade = self._create_trade(
                    side, client_id, order_id, resting, fill_qty, level_price
                )
                trades.append(trade)

                resting.remaining -= fill_qty
                remaining -= fill_qty

                if resting.is_filled:
                    level.popleft()
                    self._order_index.pop(resting.order_id, None)

            if not level:
                del book[level_price]

        # rest unfilled remainder in book
        if remaining > 0:
            self._add_to_book(
                order_id, client_id, side, price, quantity, remaining, timestamp
            )

        filled = quantity - remaining
        self._update_volume(side, filled)

        return MatchResult(trades=trades, remaining=remaining, filled=filled)

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _add_to_book(self, order_id: str, client_id: str, side: str,
                     price: float, quantity: int, remaining: int,
                     timestamp: float):
        """Add a resting order to the appropriate side of the book."""
        from config import ORDER_TTL_SECONDS
        book = self._bids if side == "buy" else self._asks

        book_order = BookOrder(
            order_id=order_id,
            client_id=client_id,
            side=side,
            price=price,
            quantity=quantity,
            remaining=remaining,
            timestamp=timestamp,
        )
        # tag expiry for TTL sweep
        book_order.expires_at = timestamp + ORDER_TTL_SECONDS

        if price not in book:
            book[price] = deque()
        book[price].append(book_order)
        self._order_index[order_id] = book_order

    def _create_trade(self, aggressor_side: str, aggressor_id: str,
                      aggressor_order_id: str, resting: BookOrder,
                      quantity: int, price: float) -> TradeEvent:
        """Build a TradeEvent from aggressor + resting order."""
        if aggressor_side == "buy":
            buyer_id, seller_id       = aggressor_id, resting.client_id
            buy_order_id, sell_order_id = aggressor_order_id, resting.order_id
        else:
            buyer_id, seller_id       = resting.client_id, aggressor_id
            buy_order_id, sell_order_id = resting.order_id, aggressor_order_id

        return TradeEvent(
            buyer_id=buyer_id,
            seller_id=seller_id,
            buy_order_id=buy_order_id,
            sell_order_id=sell_order_id,
            price=price,
            quantity=quantity,
        )

    def _update_volume(self, side: str, quantity: int):
        """Track directional volume for OFI calculation."""
        if side == "buy":
            self._buy_volume += quantity
        else:
            self._sell_volume += quantity

    def __repr__(self) -> str:
        bid = self.get_best_bid()
        ask = self.get_best_ask()
        spread = self.get_spread()
        return (
            f"OrderBook({self.symbol}) "
            f"bid={bid} ask={ask} spread={spread} "
            f"ofi={self.get_order_flow_imbalance()}"
        )