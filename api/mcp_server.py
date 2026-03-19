"""
MCP Server — Exposes Market Simulator as Tools for LLM Agents

Wraps the FastAPI REST endpoints as MCP tools so any
MCP-compatible agent (Claude, GPT, etc.) can trade in the market.

An agent connected to this server can:
  - Read live market data (prices, order book, candles, indicators)
  - Analyse sentiment and microstructure
  - Place and monitor orders
  - Track its own portfolio and P&L
  - Compete against the bots on the leaderboard

Architecture:
  LLM Agent
      │ MCP protocol
      ▼
  MCP Server (this file)
      │ HTTP
      ▼
  FastAPI (api/main.py)
      │ Kafka
      ▼
  Market Simulator

Usage:
  1. Start the simulation:
       uv run python main.py api

  2. Start the MCP server:
       uv run python api/mcp_server.py

  3. Connect your MCP client to:
       http://localhost:8001/mcp

  4. The agent can now trade using the tools below.
"""

import json
import logging
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
API_BASE    = "http://localhost:8000"
MCP_PORT    = 8001
MCP_NAME    = "market-simulator"

# default agent client_id — each agent should use a unique one
DEFAULT_AGENT_ID = "agent-claude"

# ── MCP Server ────────────────────────────────────────────────────────────────
mcp = FastMCP(MCP_NAME)
http = httpx.Client(base_url=API_BASE, timeout=5.0)


def _get(path: str) -> Any:
    """GET from FastAPI — raises on error."""
    resp = http.get(path)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: dict) -> Any:
    """POST to FastAPI — raises on error."""
    resp = http.post(path, json=body)
    resp.raise_for_status()
    return resp.json()


# ── Market Data Tools ─────────────────────────────────────────────────────────

@mcp.tool()
def get_prices() -> str:
    """
    Get current prices for all symbols in the market.

    Returns a dict of symbol → current price.
    Use this to get a quick overview of where all symbols are trading.

    Example response:
      {"PEAR": 151.23, "TSLA": 89.50, "LBRY": 312.10, ...}
    """
    try:
        data = _get("/market/all/prices")
        lines = [f"Current Prices:"]
        for symbol, price in data.items():
            lines.append(f"  {symbol}: ${price:.2f}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching prices: {e}"


@mcp.tool()
def get_order_book(symbol: str, levels: int = 5) -> str:
    """
    Get the current order book depth for a symbol.

    Shows the top N bids (buyers) and asks (sellers) with quantities.
    The spread between best bid and best ask is the cost of trading.

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD
        levels: Number of price levels to show (default 5, max 20)

    The order book tells you:
      - Where buyers are willing to buy (bids)
      - Where sellers are willing to sell (asks)
      - How much liquidity exists at each level
      - The current spread (ask - bid)
    """
    try:
        data = _get(f"/market/{symbol}/orderbook?levels={levels}")
        lines = [f"Order Book — {symbol}"]
        lines.append(f"  Mid: ${data.get('mid', 0):.2f}  "
                     f"Spread: ${data.get('spread', 0):.3f}")
        lines.append("")
        lines.append("  ASKS (sellers):")
        for ask in reversed(data.get("asks", [])):
            bar = "█" * min(ask["quantity"], 20)
            lines.append(f"    ${ask['price']:.2f}  {bar} {ask['quantity']}")
        lines.append("  ─────────────────")
        lines.append("  BIDS (buyers):")
        for bid in data.get("bids", []):
            bar = "█" * min(bid["quantity"], 20)
            lines.append(f"    ${bid['price']:.2f}  {bar} {bid['quantity']}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching order book for {symbol}: {e}"


@mcp.tool()
def get_indicators(symbol: str) -> str:
    """
    Get all technical indicators for a symbol.

    Returns current values of:
      RSI        Relative Strength Index (0-100)
                   >70 = overbought, <30 = oversold
      MACD       Moving Average Convergence Divergence
                   MACD > Signal = bullish momentum
      BB         Bollinger Bands upper and lower
                   Price near upper = overbought territory
      VWAP       Volume Weighted Average Price
                   Price above VWAP = bullish, below = bearish
      EMA 9/21   Short and long exponential moving averages
                   EMA9 > EMA21 = uptrend (golden cross)
      OFI        Order Flow Imbalance (-1 to +1)
                   Positive = buy pressure, negative = sell pressure
      Spread     Current bid-ask spread in dollars

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD
    """
    try:
        data = _get(f"/market/{symbol}/price")
        lines = [f"Indicators — {symbol}  (Price: ${data['price']:.2f})"]
        lines.append("")

        def fmt(val):
            return f"{val:.3f}" if val is not None else "—"

        rsi = data.get("rsi")
        rsi_signal = ""
        if rsi:
            if rsi > 70:   rsi_signal = " ⚠ OVERBOUGHT"
            elif rsi < 30: rsi_signal = " ⚠ OVERSOLD"

        lines.append(f"  RSI (14):      {fmt(rsi)}{rsi_signal}")
        lines.append(f"  MACD:          {fmt(data.get('macd'))}"
                     f"  Signal: {fmt(data.get('macd_signal'))}")

        macd = data.get("macd")
        sig  = data.get("macd_signal")
        if macd and sig:
            lines.append(f"  MACD Signal:   "
                         f"{'BULLISH ▲' if macd > sig else 'BEARISH ▼'}")

        lines.append(f"  BB Upper:      {fmt(data.get('bb_upper'))}")
        lines.append(f"  BB Lower:      {fmt(data.get('bb_lower'))}")
        lines.append(f"  VWAP:          {fmt(data.get('vwap'))}")

        ema_s = data.get("ema_short")
        ema_l = data.get("ema_long")
        lines.append(f"  EMA 9:         {fmt(ema_s)}")
        lines.append(f"  EMA 21:        {fmt(ema_l)}")
        if ema_s and ema_l:
            lines.append(f"  EMA Cross:     "
                         f"{'UPTREND ▲' if ema_s > ema_l else 'DOWNTREND ▼'}")

        ofi = data.get("ofi")
        lines.append(f"  OFI:           {fmt(ofi)}"
                     f"{'  (buy pressure)' if ofi and ofi > 0.3 else ''}"
                     f"{'  (sell pressure)' if ofi and ofi < -0.3 else ''}")
        lines.append(f"  Spread:        ${fmt(data.get('spread'))}")
        lines.append(f"  Volume:        {data.get('volume', 0)}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching indicators for {symbol}: {e}"


@mcp.tool()
def get_sentiment(symbol: str) -> str:
    """
    Get current market sentiment for a symbol.

    Sentiment is derived from the rolling buy/sell ratio over
    the last 20 trades. It tells you which direction the market
    is leaning and how strongly.

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD

    Returns:
      sentiment:  bullish / bearish / neutral
      strength:   0.0 (weak) to 1.0 (very strong)
      buy_ratio:  fraction of volume that was buying (0.0 to 1.0)
      velocity:   trades per second (high = active market)

    Trading interpretation:
      bullish + strength > 0.7 → strong upward momentum
      bearish + strength > 0.7 → strong downward momentum
      neutral → no clear direction, market is balanced
    """
    try:
        data = _get(f"/market/{symbol}/sentiment")
        sent     = data["sentiment"].upper()
        strength = data["strength"]
        ratio    = data["buy_ratio"]
        vel      = data["trade_velocity"]

        bars   = int(strength * 20)
        bar    = "█" * bars + "░" * (20 - bars)

        lines = [f"Sentiment — {symbol}"]
        lines.append(f"  Direction:  {sent}")
        lines.append(f"  Strength:   [{bar}] {strength:.2f}")
        lines.append(f"  Buy Ratio:  {ratio*100:.1f}%  "
                     f"Sell Ratio: {(1-ratio)*100:.1f}%")
        lines.append(f"  Velocity:   {vel:.2f} trades/sec")
        lines.append("")

        if data["sentiment"] == "bullish" and strength > 0.6:
            lines.append("  Signal: Strong buy pressure — momentum bots buying")
        elif data["sentiment"] == "bearish" and strength > 0.6:
            lines.append("  Signal: Strong sell pressure — momentum bots selling")
        elif data["sentiment"] == "neutral":
            lines.append("  Signal: Balanced market — no clear directional bias")

        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching sentiment for {symbol}: {e}"


@mcp.tool()
def get_candles(symbol: str, n: int = 10) -> str:
    """
    Get the last N closed OHLCV candlestick bars for a symbol.

    Each candle covers a 10-second window and shows:
      O = Open  (first trade price in window)
      H = High  (highest trade price)
      L = Low   (lowest trade price)
      C = Close (last trade price)
      V = Volume (total shares traded)

    Reading candles:
      Close > Open = bullish candle (green)
      Close < Open = bearish candle (red)
      Large body = strong move with conviction
      Small body = indecision or consolidation
      High volume candle = conviction behind the move

    Args:
        symbol: Trading symbol e.g. PEAR, TSLA, LBRY, RNFR, MHRD
        n:      Number of candles to return (default 10, max 100)
    """
    try:
        data = _get(f"/market/{symbol}/candles?n={n}")
        if not data:
            return f"No candles yet for {symbol}. Wait for trades to occur."

        lines = [f"Candles — {symbol} (last {len(data)} x 10s bars)"]
        lines.append(f"  {'#':3}  {'Open':8} {'High':8} {'Low':8} "
                     f"{'Close':8} {'Vol':6} {'Dir':5}")
        lines.append("  " + "─" * 52)

        for i, c in enumerate(data):
            bull  = c["close"] >= c["open"]
            arrow = "▲" if bull else "▼"
            color = "+" if bull else "-"
            body  = abs(c["close"] - c["open"])
            lines.append(
                f"  {i+1:3}  "
                f"{c['open']:8.2f} "
                f"{c['high']:8.2f} "
                f"{c['low']:8.2f} "
                f"{c['close']:8.2f} "
                f"{c['volume']:6}  "
                f"{arrow} {color}{body:.2f}"
            )

        # summary
        if len(data) >= 3:
            recent_closes = [c["close"] for c in data[-3:]]
            trend = "UP" if recent_closes[-1] > recent_closes[0] else "DOWN"
            lines.append(f"\n  Recent trend (last 3 candles): {trend}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching candles for {symbol}: {e}"


# ── Portfolio Tools ───────────────────────────────────────────────────────────

@mcp.tool()
def get_portfolio(client_id: str = DEFAULT_AGENT_ID) -> str:
    """
    Get portfolio state for a participant.

    Shows holdings, cash, P&L and risk metrics.

    Args:
        client_id: Participant ID. Use your agent's unique client_id.
                   Other valid IDs: bot-market-maker, bot-momentum,
                   bot-random, bot-mean-reversion, user

    Returns:
      holdings:      shares held per symbol
      avg_cost:      average purchase price per symbol
      cash:          available cash
      realised_pnl:  locked-in profit/loss from closed positions
      unrealised_pnl: floating profit/loss on open positions
      sharpe:        risk-adjusted return ratio
      max_drawdown:  worst peak-to-trough loss fraction
      var_95:        Value at Risk at 95% confidence
    """
    try:
        data = _get(f"/portfolio/{client_id}")
        lines = [f"Portfolio — {client_id}"]
        lines.append("")

        holdings = data.get("holdings", [])
        if not holdings:
            lines.append("  No open positions.")
        else:
            lines.append(f"  {'Symbol':8} {'Qty':6} {'Avg Cost':10} "
                         f"{'Curr':10} {'Unrealised':12}")
            lines.append("  " + "─" * 48)
            for h in holdings:
                pnl = h.get("unrealised", 0) or 0
                col = "+" if pnl >= 0 else ""
                lines.append(
                    f"  {h['symbol']:8} "
                    f"{h['quantity']:6} "
                    f"${h['avg_cost']:9.2f} "
                    f"${h.get('curr_price', 0) or 0:9.2f} "
                    f"  {col}{pnl:.2f}"
                )

        lines.append("")
        lines.append(f"  Cash:           ${data['cash']:,.2f}")
        r = data["realised_pnl"]
        u = data["unrealised_pnl"]
        lines.append(f"  Realised P&L:   {'+' if r >= 0 else ''}{r:.2f}")
        lines.append(f"  Unrealised P&L: {'+' if u >= 0 else ''}{u:.2f}")
        lines.append(f"  Total P&L:      {'+' if r+u >= 0 else ''}{r+u:.2f}")
        lines.append("")

        if data.get("sharpe"):
            lines.append(f"  Sharpe Ratio:   {data['sharpe']:.3f}")
        if data.get("max_drawdown"):
            lines.append(f"  Max Drawdown:   {data['max_drawdown']*100:.1f}%")
        if data.get("var_95"):
            lines.append(f"  VaR 95%:        {data['var_95']:.2f}")

        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching portfolio for {client_id}: {e}"


# ── Order Tools ───────────────────────────────────────────────────────────────

@mcp.tool()
def place_order(
    symbol     : str,
    side       : str,
    quantity   : int,
    price      : float | None = None,
    client_id  : str = DEFAULT_AGENT_ID,
) -> str:
    """
    Place a buy or sell order in the market.

    Args:
        symbol:    Trading symbol: PEAR, TSLA, LBRY, RNFR, MHRD
        side:      'buy' or 'sell'
        quantity:  Number of shares (must be > 0)
        price:     Limit price in dollars. If None → market order.
        client_id: Your agent's unique client ID (default: agent-claude)

    Order types:
      Limit order (price provided):
        Rests in the order book until matched or expires (60s TTL).
        You control the price but execution is not guaranteed.
        Better for: entering at a specific price level.

      Market order (no price):
        Fills immediately at best available price.
        Execution guaranteed but price may slip on large orders.
        Better for: urgent entries when you need to be filled.

    Risk checks applied:
      - Insufficient cash → order rejected
      - Insufficient shares for sell → order rejected
      - Symbol halted → order rejected by matching engine

    Returns order_id which you can use to track the order.
    """
    valid_symbols = ["PEAR", "TSLA", "LBRY", "RNFR", "MHRD"]
    symbol = symbol.upper()

    if symbol not in valid_symbols:
        return (f"Invalid symbol '{symbol}'. "
                f"Valid symbols: {', '.join(valid_symbols)}")

    if side not in ("buy", "sell"):
        return "Invalid side. Must be 'buy' or 'sell'."

    if quantity <= 0:
        return "Quantity must be greater than 0."

    order_type = "limit" if price is not None else "market"

    try:
        body = {
            "symbol"    : symbol,
            "side"      : side,
            "order_type": order_type,
            "quantity"  : quantity,
            "client_id" : client_id,
        }
        if price is not None:
            body["price"] = round(float(price), 2)

        data = _post("/orders", body)

        price_str = (f"@ ${data['price']:.2f}" 
                     if data.get("price") else "@ MARKET")
        lines = [
            f"Order placed successfully:",
            f"  Order ID:   {data['order_id']}",
            f"  Symbol:     {data['symbol']}",
            f"  Side:       {data['side'].upper()}",
            f"  Type:       {data['order_type']}",
            f"  Quantity:   {data['quantity']}",
            f"  Price:      {price_str}",
            f"  Status:     {data['status'].upper()}",
            f"  Expires at: {data['expires_at']:.0f} (unix timestamp)",
        ]
        return "\n".join(lines)
    except httpx.HTTPStatusError as e:
        detail = e.response.json().get("detail", str(e))
        return f"Order rejected: {detail}"
    except Exception as e:
        return f"Error placing order: {e}"


# ── System Tools ──────────────────────────────────────────────────────────────

@mcp.tool()
def get_market_status() -> str:
    """
    Get the current status of all symbols — active or halted.

    A symbol is halted when its price moves more than 5% in 5 seconds
    (circuit breaker). Halted symbols reject all orders until
    automatically resumed after 30 seconds.

    Returns status for all symbols including current price and
    volatility profile.
    """
    try:
        data = _get("/system/status")
        lines = ["Market Status:"]
        lines.append("")
        for s in data:
            status = "⛔ HALTED" if s["halted"] else "✅ ACTIVE"
            price  = f"${s['price']:.2f}" if s.get("price") else "—"
            lines.append(
                f"  {s['symbol']:6}  {status:12}  "
                f"{price:10}  {s['name']}  ({s['profile']} vol)"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching market status: {e}"


@mcp.tool()
def get_leaderboard() -> str:
    """
    Get all participants ranked by total P&L.

    Shows realised + unrealised P&L, cash remaining,
    Sharpe ratio and max drawdown for each participant.

    Use this to:
      - See how your agent compares to the bots
      - Identify which strategies are working
      - Monitor bot behaviour over time

    Participants:
      user               Human trader via CLI
      bot-market-maker   Liquidity provider
      bot-momentum       Trend follower
      bot-random         Noise trader
      bot-mean-reversion Contrarian/RSI fader
      agent-*            LLM agents
    """
    try:
        data = _get("/system/leaderboard")
        medals = ["🥇", "🥈", "🥉"]
        lines  = ["Leaderboard — Ranked by Total P&L"]
        lines.append("")
        lines.append(f"  {'#':4} {'Participant':22} {'Total P&L':12} "
                     f"{'Realised':10} {'Cash':10} {'Sharpe':8} {'Drawdown'}")
        lines.append("  " + "─" * 75)

        for i, r in enumerate(data):
            medal = medals[i] if i < 3 else f"  {i+1}."
            pnl   = r["total_pnl"]
            col   = "+" if pnl >= 0 else ""
            name  = r["client_id"].replace("bot-", "")
            sharpe = f"{r['sharpe']:.2f}" if r.get("sharpe") else "—"
            dd     = (f"{r['max_drawdown']*100:.1f}%"
                      if r.get("max_drawdown") else "—")
            lines.append(
                f"  {medal:4} {name:22} "
                f"{col}{pnl:10.2f}   "
                f"{r['realised_pnl']:8.2f}   "
                f"${r['cash']:8,.0f}   "
                f"{sharpe:6}   {dd}"
            )

        return "\n".join(lines)
    except Exception as e:
        return f"Error fetching leaderboard: {e}"


@mcp.tool()
def get_all_indicators() -> str:
    """
    Get a summary of key indicators across all symbols at once.

    Useful for scanning the market to find trading opportunities
    without calling get_indicators for each symbol individually.

    Returns RSI, sentiment, VWAP deviation and EMA trend
    for all 5 symbols in one response.
    """
    try:
        lines = ["Market Scan — All Symbols"]
        lines.append("")
        lines.append(f"  {'SYM':6} {'Price':8} {'RSI':6} "
                     f"{'vs VWAP':9} {'EMA Trend':12} {'Sentiment':12} {'OFI':6}")
        lines.append("  " + "─" * 65)

        symbols = ["PEAR", "TSLA", "LBRY", "RNFR", "MHRD"]
        for symbol in symbols:
            try:
                d = _get(f"/market/{symbol}/price")
                s = _get(f"/market/{symbol}/sentiment")

                price    = d.get("price", 0)
                rsi      = d.get("rsi")
                vwap     = d.get("vwap")
                ema_s    = d.get("ema_short")
                ema_l    = d.get("ema_long")
                ofi      = d.get("ofi")
                sent     = s.get("sentiment", "neutral")[:4].upper()
                strength = s.get("strength", 0)

                rsi_str  = f"{rsi:.1f}" if rsi else "—"
                vwap_dev = ""
                if vwap and price:
                    dev = (price - vwap) / vwap * 100
                    vwap_dev = f"{dev:+.1f}%"

                ema_trend = "—"
                if ema_s and ema_l:
                    ema_trend = "UP ▲" if ema_s > ema_l else "DN ▼"

                ofi_str = f"{ofi:+.2f}" if ofi is not None else "—"

                lines.append(
                    f"  {symbol:6} ${price:7.2f} "
                    f"{rsi_str:6} "
                    f"{vwap_dev:9} "
                    f"{ema_trend:12} "
                    f"{sent:4} {strength:.2f}   "
                    f"{ofi_str}"
                )
            except Exception:
                lines.append(f"  {symbol:6} — data unavailable")

        return "\n".join(lines)
    except Exception as e:
        return f"Error scanning market: {e}"


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    logging.basicConfig(level=logging.INFO)

    try:
        http.get("/system/health", timeout=3.0)
        logger.info("[mcp] API reachable at %s", API_BASE)
    except Exception:
        print(
            f"ERROR: Cannot reach API at {API_BASE}\n"
            f"Start the simulation first:\n"
            f"  uv run python main.py api\n"
        )
        sys.exit(1)

    print(f"\nMCP Server starting on port {MCP_PORT}")
    print(f"Connect your MCP client to: http://localhost:{MCP_PORT}/mcp")
    print()

    os.environ["FASTMCP_PORT"] = str(MCP_PORT)
    mcp.run(transport="streamable-http")