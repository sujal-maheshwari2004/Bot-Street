"""
MCP Server — mounted at /mcp inside the API pod

No longer a separate process. Calls Python objects directly
instead of making HTTP calls to itself.

Architecture (K8s edition):
  LLM Agent
      │ MCP protocol (streamable-http)
      ▼
  /mcp  (this module, mounted on FastAPI app)
      │ direct Python calls
      ▼
  APICache (in-memory, fed by Kafka consumers)
      │ Kafka
      ▼
  Market pods (engine, bots, ledger, etc.)
"""

import logging
from mcp.server.fastmcp import FastMCP
from core.kafka_client import MarketProducer
from core.schemas import Order
from config import TOPIC_MARKET_ORDERS, SYMBOL_LIST

logger = logging.getLogger(__name__)

MCP_NAME         = "market-simulator"
DEFAULT_AGENT_ID = "agent-claude"

mcp      = FastMCP(MCP_NAME)
_producer = MarketProducer("mcp-server")

# cache reference — injected at startup via set_cache()
_cache = None


def set_cache(cache):
    global _cache
    _cache = cache


# ── Market Data Tools ─────────────────────────────────────────────────────────

@mcp.tool()
def get_prices() -> str:
    """
    Get current prices for all symbols in the market.
    Returns a dict of symbol → current price.
    """
    if _cache is None:
        return "Cache not ready yet."
    lines = ["Current Prices:"]
    for symbol, price in _cache.prices.items():
        lines.append(f"  {symbol}: ${price:.2f}")
    return "\n".join(lines)


@mcp.tool()
def get_order_book(symbol: str, levels: int = 5) -> str:
    """
    Get the current order book depth for a symbol.

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD
        levels: Number of price levels to show (default 5)
    """
    # Order book lives in the engine pod — not available in API pod memory.
    # Return the spread/mid derived from cached price as a fallback.
    if _cache is None:
        return "Cache not ready yet."
    symbol = symbol.upper()
    price  = _cache.prices.get(symbol)
    if price is None:
        return f"No price data for {symbol}."
    return (
        f"Order Book — {symbol}\n"
        f"  Last price: ${price:.2f}\n"
        f"  (Full depth requires direct engine access — not available in API pod)"
    )


@mcp.tool()
def get_sentiment(symbol: str) -> str:
    """
    Get current market sentiment for a symbol.

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD
    """
    if _cache is None:
        return "Cache not ready yet."
    symbol = symbol.upper()
    sent   = _cache.sentiment.get(symbol)
    if sent is None:
        return f"No sentiment data for {symbol} yet."

    direction = sent["sentiment"].upper()
    strength  = sent["strength"]
    ratio     = sent["buy_ratio"]
    velocity  = sent["trade_velocity"]
    bar       = "█" * int(strength * 20) + "░" * (20 - int(strength * 20))

    return (
        f"Sentiment — {symbol}\n"
        f"  Direction:  {direction}\n"
        f"  Strength:   [{bar}] {strength:.2f}\n"
        f"  Buy Ratio:  {ratio*100:.1f}%  Sell Ratio: {(1-ratio)*100:.1f}%\n"
        f"  Velocity:   {velocity:.2f} trades/sec"
    )


@mcp.tool()
def get_candles(symbol: str, n: int = 10) -> str:
    """
    Get the last N closed OHLCV candlestick bars for a symbol.

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD
        n:      Number of candles to return (default 10, max 100)
    """
    if _cache is None:
        return "Cache not ready yet."
    symbol  = symbol.upper()
    candles = _cache.candles.get(symbol, [])[-n:]
    if not candles:
        return f"No candles yet for {symbol}."

    lines = [f"Candles — {symbol} (last {len(candles)} x 10s bars)"]
    lines.append(f"  {'#':3}  {'Open':8} {'High':8} {'Low':8} {'Close':8} {'Vol':6} {'Dir':5}")
    lines.append("  " + "─" * 52)
    for i, c in enumerate(candles):
        bull  = c["close"] >= c["open"]
        arrow = "▲" if bull else "▼"
        body  = abs(c["close"] - c["open"])
        lines.append(
            f"  {i+1:3}  "
            f"{c['open']:8.2f} {c['high']:8.2f} "
            f"{c['low']:8.2f} {c['close']:8.2f} "
            f"{c['volume']:6}  {arrow} {body:.2f}"
        )
    return "\n".join(lines)


@mcp.tool()
def get_portfolio(client_id: str = DEFAULT_AGENT_ID) -> str:
    """
    Get portfolio state for a participant.

    Args:
        client_id: Participant ID. Valid: bot-market-maker, bot-momentum,
                   bot-random, bot-mean-reversion, user, agent-claude
    """
    if _cache is None:
        return "Cache not ready yet."
    port = _cache.portfolios.get(client_id)
    if port is None:
        return f"No portfolio data for '{client_id}' yet."

    prices   = dict(_cache.prices)
    holdings = port.get("holdings", {})
    avg_cost = port.get("avg_cost", {})
    cash     = port.get("cash", 0.0)
    r_pnl    = port.get("realised_pnl", 0.0)
    u_pnl    = port.get("unrealised_pnl", 0.0)

    lines = [f"Portfolio — {client_id}", ""]
    if not holdings:
        lines.append("  No open positions.")
    else:
        lines.append(f"  {'Symbol':8} {'Qty':6} {'Avg Cost':10} {'Curr':10} {'Unrealised':12}")
        lines.append("  " + "─" * 48)
        for sym, qty in holdings.items():
            avg  = avg_cost.get(sym, 0.0)
            curr = prices.get(sym, avg)
            unr  = (curr - avg) * qty
            col  = "+" if unr >= 0 else ""
            lines.append(
                f"  {sym:8} {qty:6} ${avg:9.2f} ${curr:9.2f}  {col}{unr:.2f}"
            )

    lines += [
        "",
        f"  Cash:           ${cash:,.2f}",
        f"  Realised P&L:   {'+' if r_pnl >= 0 else ''}{r_pnl:.2f}",
        f"  Unrealised P&L: {'+' if u_pnl >= 0 else ''}{u_pnl:.2f}",
        f"  Total P&L:      {'+' if r_pnl+u_pnl >= 0 else ''}{r_pnl+u_pnl:.2f}",
    ]
    if port.get("sharpe"):
        lines.append(f"  Sharpe Ratio:   {port['sharpe']:.3f}")
    if port.get("max_drawdown"):
        lines.append(f"  Max Drawdown:   {port['max_drawdown']*100:.1f}%")
    return "\n".join(lines)


@mcp.tool()
def place_order(
    symbol   : str,
    side     : str,
    quantity : int,
    price    : float | None = None,
    client_id: str = DEFAULT_AGENT_ID,
) -> str:
    """
    Place a buy or sell order in the market.

    Args:
        symbol:    Trading symbol: PEAR, TSLA, LBRY, RNFR, MHRD
        side:      'buy' or 'sell'
        quantity:  Number of shares (must be > 0)
        price:     Limit price. If None → market order.
        client_id: Your agent's unique client ID
    """
    symbol = symbol.upper()
    if symbol not in SYMBOL_LIST:
        return f"Invalid symbol '{symbol}'. Valid: {', '.join(SYMBOL_LIST)}"
    if side not in ("buy", "sell"):
        return "Invalid side. Must be 'buy' or 'sell'."
    if quantity <= 0:
        return "Quantity must be > 0."

    order = Order(
        client_id  = client_id,
        symbol     = symbol,
        side       = side,
        order_type = "limit" if price is not None else "market",
        quantity   = quantity,
        price      = round(float(price), 2) if price is not None else None,
    )

    try:
        _producer.send_order(TOPIC_MARKET_ORDERS, order)
        _producer.flush()
        price_str = f"@ ${order.price:.2f}" if order.price else "@ MARKET"
        return (
            f"Order placed:\n"
            f"  Order ID: {order.order_id}\n"
            f"  {side.upper()} {quantity}x {symbol} {price_str}\n"
            f"  Type: {order.order_type}  Status: ACCEPTED"
        )
    except Exception as e:
        return f"Order failed: {e}"


@mcp.tool()
def get_market_status() -> str:
    """Get the current status of all symbols — active or halted."""
    if _cache is None:
        return "Cache not ready yet."
    lines = ["Market Status:", ""]
    for symbol in SYMBOL_LIST:
        halted = _cache.halted.get(symbol, False)
        price  = _cache.prices.get(symbol)
        status = "⛔ HALTED" if halted else "✅ ACTIVE"
        price_str = f"${price:.2f}" if price else "—"
        lines.append(f"  {symbol:6}  {status:12}  {price_str}")
    return "\n".join(lines)


@mcp.tool()
def get_leaderboard() -> str:
    """Get all participants ranked by total P&L."""
    if _cache is None:
        return "Cache not ready yet."

    rows = []
    for client_id, port in _cache.portfolios.items():
        r = port.get("realised_pnl", 0.0)
        u = port.get("unrealised_pnl", 0.0)
        rows.append((client_id, r + u, r, port.get("cash", 0.0)))

    rows.sort(key=lambda x: x[1], reverse=True)
    medals = ["🥇", "🥈", "🥉"]
    lines  = ["Leaderboard — Ranked by Total P&L", ""]
    lines.append(f"  {'#':4} {'Participant':22} {'Total P&L':12} {'Realised':10} {'Cash':10}")
    lines.append("  " + "─" * 60)
    for i, (cid, total, realised, cash) in enumerate(rows):
        medal = medals[i] if i < 3 else f"  {i+1}."
        col   = "+" if total >= 0 else ""
        name  = cid.replace("bot-", "")
        lines.append(f"  {medal:4} {name:22} {col}{total:10.2f}   {realised:8.2f}   ${cash:8,.0f}")
    return "\n".join(lines)