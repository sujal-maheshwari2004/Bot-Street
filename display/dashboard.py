"""
Dashboard — Live Terminal UI using Rich

Full layout:
┌─ ticker ──────────────────────────────────────────────────────┐
├─ orderbook depth ─────┬─ candlestick chart ───────────────────┤
├─ indicators ──────────┴─ sentiment gauge ─────────────────────┤
├─ P&L line chart ──────────────────────────────────────────────┤
└─ leaderboard ─────────────────────────────────────────────────┘
"""

import logging
import threading
from collections import defaultdict
from time import time, sleep
import copy

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from rich.console import Group

import sys
from threading import Event

from config import (
    SYMBOL_LIST, SYMBOLS,
    TOPIC_PRICE_UPDATE, TOPIC_CANDLES, TOPIC_MARKET_SENTIMENT,
    TOPIC_PORTFOLIO_SNAP, TOPIC_MARKET_HALT, TOPIC_ORDER_EXPIRED,
    TOPIC_TRADE_EXECUTED,
    USER_CLIENT_ID, CANDLE_DISPLAY_COUNT, ALL_CLIENT_IDS,
)
from core.kafka_client import MarketConsumer

logger = logging.getLogger(__name__)

REFRESH_RATE = 2


# ── Dashboard state ───────────────────────────────────────────────────────────

class DashboardState:

    def __init__(self):
        self._lock = threading.Lock()

        self.prices      : dict[str, float] = {s: SYMBOLS[s][1] for s in SYMBOL_LIST}
        self.prev_prices : dict[str, float] = {s: SYMBOLS[s][1] for s in SYMBOL_LIST}
        self.sentiment   : dict[str, dict]  = {}
        self.indicators  : dict[str, dict]  = {}
        self.halted      : dict[str, bool]  = {s: False for s in SYMBOL_LIST}
        self.candles     : dict[str, list]  = defaultdict(list)
        self.volume      : dict[str, int]   = defaultdict(int)
        self.orderbook   : dict[str, dict]  = {}   # symbol → {bids, asks}

        # portfolio per client_id
        self.portfolios  : dict[str, dict]  = {}

        # P&L history per client for line chart — last 60 ticks
        self.pnl_history : dict[str, list]  = defaultdict(list)

        # trade count per client
        self.trade_counts: dict[str, int]   = defaultdict(int)

        self.events      : list[str]        = []

    # ── Update methods ────────────────────────────────────────────────────────

    def update_price(self, msg: dict):
        with self._lock:
            symbol = msg.get("symbol")
            if not symbol:
                return
            self.prev_prices[symbol] = self.prices.get(symbol, msg["price"])
            self.prices[symbol]      = msg["price"]
            self.volume[symbol]      = msg.get("volume", 0)
            self.indicators[symbol]  = {
                "rsi"        : msg.get("rsi"),
                "macd"       : msg.get("macd"),
                "macd_signal": msg.get("macd_signal"),
                "bb_upper"   : msg.get("bb_upper"),
                "bb_lower"   : msg.get("bb_lower"),
                "ema_short"  : msg.get("ema_short"),
                "ema_long"   : msg.get("ema_long"),
                "ofi"        : msg.get("ofi"),
                "vwap"       : msg.get("vwap"),
                "spread"     : msg.get("spread"),
                "bid"        : msg.get("bid"),
                "ask"        : msg.get("ask"),
            }

    def update_sentiment(self, msg: dict):
        with self._lock:
            symbol = msg.get("symbol")
            if symbol:
                self.sentiment[symbol] = msg

    def update_candle(self, msg: dict):
        with self._lock:
            symbol = msg.get("symbol")
            if symbol:
                self.candles[symbol].append(msg)
                if len(self.candles[symbol]) > CANDLE_DISPLAY_COUNT:
                    self.candles[symbol].pop(0)

    def update_portfolio(self, msg: dict):
        with self._lock:
            client_id = msg.get("client_id")
            if not client_id:
                return
            self.portfolios[client_id] = msg
            # track P&L history for line chart
            r_pnl  = msg.get("realised_pnl", 0)
            ur_pnl = msg.get("unrealised_pnl", 0)
            total  = round(r_pnl + ur_pnl, 2)
            self.pnl_history[client_id].append(total)
            if len(self.pnl_history[client_id]) > 60:
                self.pnl_history[client_id].pop(0)

    def update_halt(self, msg: dict):
        with self._lock:
            symbol = msg.get("symbol")
            if symbol:
                self.halted[symbol] = msg.get("status") == "halted"
                status = "HALTED" if self.halted[symbol] else "RESUMED"
                self._add_event(f"[red]{symbol} {status}[/red]")

    def add_expired(self, msg: dict):
        with self._lock:
            self._add_event(
                f"[yellow]Expired: {msg.get('symbol')} "
                f"{msg.get('side')} {msg.get('quantity')}@"
                f"{msg.get('price')}[/yellow]"
            )

    def _add_event(self, text: str):
        self.events.insert(0, text)
        if len(self.events) > 6:
            self.events.pop()

    def snapshot(self) -> dict:
        with self._lock:
            return copy.deepcopy({
                "prices"      : self.prices,
                "prev_prices" : self.prev_prices,
                "sentiment"   : self.sentiment,
                "indicators"  : self.indicators,
                "halted"      : self.halted,
                "candles"     : self.candles,
                "volume"      : self.volume,
                "portfolios"  : self.portfolios,
                "pnl_history" : self.pnl_history,
                "events"      : self.events,
            })


# ── Render helpers ────────────────────────────────────────────────────────────

def _ticker_panel(snap: dict) -> Panel:
    text = Text()
    for i, symbol in enumerate(SYMBOL_LIST):
        price = snap["prices"].get(symbol, 0)
        prev  = snap["prev_prices"].get(symbol, price)
        halt  = snap["halted"].get(symbol, False)

        if halt:
            arrow, style = "⛔", "bold red"
        elif price > prev:
            arrow, style = "▲", "bold green"
        elif price < prev:
            arrow, style = "▼", "bold red"
        else:
            arrow, style = "─", "bold yellow"

        vol = snap["volume"].get(symbol, 0)
        text.append(f" {symbol} ", style="bold cyan")
        text.append(f"${price:.2f}{arrow} ", style=style)
        text.append(f"vol:{vol} ", style="dim")
        if i < len(SYMBOL_LIST) - 1:
            text.append("│ ", style="dim white")

    return Panel(text, title="[bold]⚡ MARKET SIMULATOR[/bold]",
                 box=box.HEAVY)


def _orderbook_panel(snap: dict, symbol: str) -> Panel:
    """
    Horizontal depth bars — visualizes liquidity at each price level.

    Each bar's width = quantity at that level relative to max level.
    Bids shown in green (buyers), asks in red (sellers).
    The gap between them IS the spread.
    """
    ind = snap["indicators"].get(symbol, {})
    bid = ind.get("bid")
    ask = ind.get("ask")
    spread = ind.get("spread")

    text = Text()

    # synthetic depth from bid/ask — real depth needs engine injection
    # for POC we show the best bid/ask with spread visualization
    if bid and ask:
        mid = (bid + ask) / 2

        text.append("  ASKS\n", style="dim red")

        # show 5 synthetic ask levels above best ask
        for i in range(4, -1, -1):
            level_price = round(ask + i * 0.05, 2)
            qty = max(1, 10 - i * 2)
            bar = "█" * qty
            text.append(f"  {level_price:.2f} ", style="dim")
            text.append(f"{bar:10} {qty:3}\n", style="red")

        text.append(f"\n  spread: ${spread:.3f}  mid: ${mid:.2f}\n\n",
                    style="bold yellow")

        # show 5 synthetic bid levels below best bid
        for i in range(5):
            level_price = round(bid - i * 0.05, 2)
            qty = max(1, 10 - i * 2)
            bar = "█" * qty
            text.append(f"  {level_price:.2f} ", style="dim")
            text.append(f"{bar:10} {qty:3}\n", style="green")

        text.append("  BIDS\n", style="dim green")
    else:
        text.append("  Waiting for quotes...", style="dim")

    return Panel(text, title=f"[cyan]Order Book — {symbol}[/cyan]",
                 box=box.SIMPLE)


def _candlestick_panel(snap: dict, symbol: str) -> Panel:
    """
    ASCII candlestick chart.

    Each candle:
      █ = body (open to close)
      │ = wick (high to low)
    Green candle = close > open (bullish)
    Red candle   = close < open (bearish)
    Doji (─)     = open ≈ close (indecision)
    """
    candles = snap["candles"].get(symbol, [])

    if not candles:
        return Panel(
            Text("  Waiting for candles...\n  (10s intervals)", style="dim"),
            title=f"[cyan]Candles — {symbol} (10s)[/cyan]",
            box=box.SIMPLE,
        )

    display = candles[-20:]
    all_highs = [c["high"] for c in display]
    all_lows  = [c["low"]  for c in display]
    max_p = max(all_highs)
    min_p = min(all_lows)
    rng   = max_p - min_p or 1.0
    height = 10

    text = Text()

    for row in range(height, -1, -1):
        threshold = min_p + (row / height) * rng
        price_label = f"{threshold:7.2f} │"
        line_chars  = []
        line_styles = []

        for c in display:
            o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
            is_bull = cl >= o
            color   = "green" if is_bull else "red"

            body_hi = max(o, cl)
            body_lo = min(o, cl)

            if body_lo <= threshold <= body_hi:
                line_chars.append("█")
                line_styles.append(color)
            elif l <= threshold <= h:
                line_chars.append("│")
                line_styles.append("dim " + color)
            else:
                line_chars.append(" ")
                line_styles.append("white")

        text.append(price_label, style="dim")
        for ch, st in zip(line_chars, line_styles):
            text.append(ch, style=st)
        text.append("\n")

    # volume bars below chart
    max_vol = max((c["volume"] for c in display), default=1)
    text.append("   vol  │", style="dim")
    for c in display:
        bars = max(1, int((c["volume"] / max_vol) * 4))
        text.append("▄" * bars, style="cyan")
        text.append(" " * (2 - bars), style="white")
    text.append("\n")

    return Panel(text, title=f"[cyan]Candles — {symbol} (10s)[/cyan]",
                 box=box.SIMPLE)


def _sentiment_gauge_panel(snap: dict) -> Panel:
    """
    Sentiment strength gauge for all symbols.
    Visual bar from bearish (-) to bullish (+).
    """
    text = Text()
    text.append(f"{'SYM':6} {'BEARISH◄':8}{'':12}{'►BULLISH':8} STR   RATIO\n",
                style="dim")
    text.append("─" * 52 + "\n", style="dim")

    for symbol in SYMBOL_LIST:
        sent     = snap["sentiment"].get(symbol, {})
        sentiment = sent.get("sentiment", "neutral")
        strength  = sent.get("strength", 0.0)
        ratio     = sent.get("buy_ratio", 0.5)
        halt      = snap["halted"].get(symbol, False)

        total_bars = 20
        mid        = total_bars // 2

        if sentiment == "bullish":
            filled = int(strength * mid)
            bar    = " " * mid + "█" * filled + "░" * (mid - filled)
            color  = "green"
        elif sentiment == "bearish":
            filled = int(strength * mid)
            bar    = "░" * (mid - filled) + "█" * filled + " " * mid
            color  = "red"
        else:
            bar   = "░" * mid + "░" * mid
            color = "yellow"

        if halt:
            text.append(f"{symbol:6} ", style="bold cyan")
            text.append("⛔ HALTED\n", style="bold red")
        else:
            text.append(f"{symbol:6} ", style="bold cyan")
            text.append(f"│{bar}│ ", style=color)
            text.append(f"{strength:.2f}  {ratio*100:.0f}%\n", style=f"bold {color}")

    return Panel(text, title="[bold]Sentiment Gauge[/bold]", box=box.SIMPLE)


def _pnl_chart_panel(snap: dict) -> Panel:
    """
    ASCII line chart showing P&L over time for all participants.
    Each participant gets a different character marker.
    """
    histories = snap["pnl_history"]

    if not histories:
        return Panel(
            Text("  Waiting for trades...", style="dim"),
            title="[bold]P&L Over Time[/bold]",
            box=box.SIMPLE,
        )

    # find global min/max for scaling
    all_vals = [v for hist in histories.values() for v in hist]
    if not all_vals:
        return Panel(Text("  No data yet.", style="dim"),
                     title="[bold]P&L Over Time[/bold]", box=box.SIMPLE)

    max_v = max(all_vals) or 1
    min_v = min(all_vals) or -1
    rng   = max_v - min_v or 1.0
    height = 8
    width  = 40

    markers = {
        "user"              : ("U", "bold white"),
        "bot-market-maker"  : ("M", "bold cyan"),
        "bot-momentum"      : ("↑", "bold green"),
        "bot-random"        : ("R", "bold yellow"),
        "bot-mean-reversion": ("~", "bold magenta"),
    }

    # build grid
    grid = [[" " for _ in range(width)] for _ in range(height + 1)]

    for client_id, hist in histories.items():
        if not hist:
            continue
        marker, _ = markers.get(client_id, ("·", "white"))
        # sample to width
        step = max(1, len(hist) // width)
        sampled = hist[::step][-width:]
        for x, val in enumerate(sampled):
            if x >= width:
                break
            row = int((val - min_v) / rng * height)
            row = max(0, min(height, row))
            y   = height - row
            grid[y][x] = marker

    text = Text()
    # y-axis labels + grid
    for row_idx, row in enumerate(grid):
        val = max_v - (row_idx / height) * rng
        text.append(f"{val:+7.1f} │", style="dim")
        line = "".join(row)
        for char in line:
            if char == " ":
                text.append(char)
            else:
                client = next(
                    (c for c, (m, _) in markers.items() if m == char),
                    None
                )
                style = markers.get(client, ("·", "white"))[1] if client else "white"
                text.append(char, style=style)
        text.append("\n")

    text.append("        └" + "─" * width + "\n", style="dim")

    # legend
    text.append("\n  ", style="dim")
    for client_id, (marker, style) in markers.items():
        short = client_id.replace("bot-", "").replace("mean-reversion", "MeanRev")
        text.append(f"{marker}:{short}  ", style=style)

    return Panel(text, title="[bold]P&L Over Time[/bold]", box=box.SIMPLE)


def _leaderboard_panel(snap: dict) -> Panel:
    """Live leaderboard — all participants ranked by total P&L."""
    portfolios = snap["portfolios"]

    if not portfolios:
        return Panel(
            Text("  Waiting for trades...", style="dim"),
            title="[bold]🏆 Leaderboard[/bold]",
            box=box.SIMPLE,
        )

    rows = []
    for client_id, port in portfolios.items():
        r_pnl  = port.get("realised_pnl", 0)
        ur_pnl = port.get("unrealised_pnl", 0)
        total  = r_pnl + ur_pnl
        rows.append({
            "client_id"   : client_id,
            "total_pnl"   : total,
            "realised"    : r_pnl,
            "unrealised"  : ur_pnl,
            "cash"        : port.get("cash", 0),
            "sharpe"      : port.get("sharpe"),
            "max_drawdown": port.get("max_drawdown"),
        })

    rows.sort(key=lambda r: r["total_pnl"], reverse=True)

    table = Table(box=box.SIMPLE, show_header=True,
                  header_style="bold cyan", padding=(0, 1))
    table.add_column("#",          justify="right",  style="dim", width=3)
    table.add_column("Participant",                   width=20)
    table.add_column("Total P&L",  justify="right",  width=10)
    table.add_column("Realised",   justify="right",  width=10)
    table.add_column("Unrealised", justify="right",  width=10)
    table.add_column("Cash",       justify="right",  width=10)
    table.add_column("Sharpe",     justify="right",  width=7)
    table.add_column("Drawdown",   justify="right",  width=9)

    medals = ["🥇", "🥈", "🥉"]

    for i, r in enumerate(rows):
        pnl   = r["total_pnl"]
        col   = "green" if pnl >= 0 else "red"
        medal = medals[i] if i < 3 else f"  {i+1}"
        sharpe = f"{r['sharpe']:.2f}" if r["sharpe"] else "—"
        dd     = f"{r['max_drawdown']*100:.1f}%" if r["max_drawdown"] else "—"

        name = r["client_id"].replace("bot-", "")
        if r["client_id"] == USER_CLIENT_ID:
            name = "👤 " + name

        table.add_row(
            str(medal),
            name,
            f"[{col}]{pnl:+.2f}[/{col}]",
            f"{r['realised']:+.2f}",
            f"{r['unrealised']:+.2f}",
            f"${r['cash']:,.0f}",
            sharpe,
            dd,
        )

    return Panel(table, title="[bold]🏆 Leaderboard[/bold]", box=box.SIMPLE)


def _indicators_panel(snap: dict, symbol: str) -> Panel:
    ind = snap["indicators"].get(symbol, {})

    def fmt(val, d=2):
        return f"{val:.{d}f}" if val is not None else "—"

    def rsi_style(val):
        if val is None: return "white"
        if val > 70:    return "bold red"
        if val < 30:    return "bold green"
        return "yellow"

    text = Text()
    rsi_val = ind.get("rsi")
    text.append("RSI    ", style="dim")
    text.append(f"{fmt(rsi_val)}", style=rsi_style(rsi_val))

    # RSI bar
    if rsi_val:
        bar_pos = int(rsi_val / 100 * 20)
        bar = "░" * bar_pos + "█" + "░" * (20 - bar_pos)
        text.append(f"  [{bar}]\n", style=rsi_style(rsi_val))
    else:
        text.append("\n")

    macd = ind.get("macd")
    sig  = ind.get("macd_signal")
    text.append("MACD   ", style="dim")
    if macd and sig:
        col = "green" if macd > sig else "red"
        text.append(f"{macd:+.3f}  sig:{sig:+.3f}\n", style=col)
    else:
        text.append("—\n")

    bb_u = ind.get("bb_upper")
    bb_l = ind.get("bb_lower")
    price = snap["prices"].get(symbol, 0)
    text.append("BB     ", style="dim")
    if bb_u and bb_l:
        pct = (price - bb_l) / (bb_u - bb_l) if bb_u != bb_l else 0.5
        pos = int(pct * 20)
        bar = "░" * pos + "●" + "░" * (20 - pos)
        text.append(f"[{bar}]\n", style="cyan")
        text.append(f"       {bb_l:.2f} ──────── {bb_u:.2f}\n", style="dim cyan")
    else:
        text.append("—\n")

    vwap = ind.get("vwap")
    text.append("VWAP   ", style="dim")
    if vwap:
        col  = "green" if price >= vwap else "red"
        diff = price - vwap
        text.append(f"{vwap:.2f}  ({diff:+.2f})\n", style=col)
    else:
        text.append("—\n")

    ema_s = ind.get("ema_short")
    ema_l = ind.get("ema_long")
    text.append("EMA    ", style="dim")
    if ema_s and ema_l:
        col = "green" if ema_s > ema_l else "red"
        cross = "↑ CROSS UP" if ema_s > ema_l else "↓ CROSS DN"
        text.append(f"9:{ema_s:.2f}  21:{ema_l:.2f}  {cross}\n", style=col)
    else:
        text.append("—\n")

    ofi = ind.get("ofi")
    text.append("OFI    ", style="dim")
    if ofi is not None:
        col     = "green" if ofi > 0 else "red"
        filled  = int(abs(ofi) * 10)
        if ofi > 0:
            bar = "░" * 10 + "█" * filled + "░" * (10 - filled)
        else:
            bar = "░" * (10 - filled) + "█" * filled + "░" * 10
        text.append(f"{ofi:+.3f} [{bar}]\n", style=col)
    else:
        text.append("—\n")

    return Panel(text, title=f"[cyan]Indicators — {symbol}[/cyan]",
                 box=box.SIMPLE)


def _events_panel(snap: dict) -> Panel:
    text = Text()
    events = snap["events"]
    if not events:
        text.append("No events yet.", style="dim")
    for ev in events:
        text.append(f"• {ev}\n")
    return Panel(text, title="[bold]Events[/bold]", box=box.SIMPLE)


def _build_layout(snap: dict, focus_symbol: str) -> Layout:
    layout = Layout()

    layout.split_column(
        Layout(name="ticker",   size=3),
        Layout(name="row1",     size=18),
        Layout(name="row2",     size=10),
        Layout(name="row3",     size=12),
    )

    layout["row1"].split_row(
        Layout(name="orderbook",   ratio=1),
        Layout(name="candles",     ratio=2),
        Layout(name="col3",        ratio=1),
    )

    layout["col3"].split_column(
        Layout(name="indicators", ratio=3),
        Layout(name="events",     ratio=1),
    )

    layout["row2"].split_row(
        Layout(name="sentiment",  ratio=2),
        Layout(name="pnl_chart",  ratio=3),
    )

    layout["row3"].update(_leaderboard_panel(snap))

    layout["ticker"].update(_ticker_panel(snap))
    layout["orderbook"].update(_orderbook_panel(snap, focus_symbol))
    layout["candles"].update(_candlestick_panel(snap, focus_symbol))
    layout["indicators"].update(_indicators_panel(snap, focus_symbol))
    layout["events"].update(_events_panel(snap))
    layout["sentiment"].update(_sentiment_gauge_panel(snap))
    layout["pnl_chart"].update(_pnl_chart_panel(snap))

    return layout


# ── Dashboard service ─────────────────────────────────────────────────────────

# ── Commands ──────────────────────────────────────────────────────────────────

COMMANDS = """
[bold cyan]Commands:[/bold cyan]
  [green]sym <SYMBOL>[/green]          switch focus symbol  (e.g. sym TSLA)
  [green]port <client_id>[/green]      view participant portfolio
  [green]book <SYMBOL>[/green]         switch order book symbol
  [green]prices[/green]               show all current prices
  [green]leaders[/green]              print full leaderboard snapshot
  [green]candles <SYMBOL>[/green]      switch candle chart symbol
  [green]clear[/green]                clear command output
  [green]help[/green]                 show this help
  [green]quit[/green]                 exit dashboard
"""


class Dashboard:

    def __init__(self, focus_symbol: str = "PEAR"):
        self.focus_symbol  = focus_symbol.upper()
        self.book_symbol   = focus_symbol.upper()
        self.candle_symbol = focus_symbol.upper()
        self._state        = DashboardState()
        self._running      = False
        self._cmd_output   : list[str] = []   # lines shown in command panel
        self._cmd_lock     = threading.Lock()

        self._consumers = {
            "price"    : MarketConsumer("dashboard-price",
                             [TOPIC_PRICE_UPDATE],    offset="latest"),
            "candle"   : MarketConsumer("dashboard-candle",
                             [TOPIC_CANDLES],          offset="latest"),
            "sentiment": MarketConsumer("dashboard-sentiment",
                             [TOPIC_MARKET_SENTIMENT], offset="latest"),
            "portfolio": MarketConsumer("dashboard-portfolio",
                             [TOPIC_PORTFOLIO_SNAP],   offset="latest"),
            "halt"     : MarketConsumer("dashboard-halt",
                             [TOPIC_MARKET_HALT],      offset="latest"),
            "expired"  : MarketConsumer("dashboard-expired",
                             [TOPIC_ORDER_EXPIRED],    offset="latest"),
        }

    # ── Command output helpers ────────────────────────────────────────────────

    def _print_cmd(self, text: str):
        with self._cmd_lock:
            self._cmd_output.append(text)
            if len(self._cmd_output) > 12:
                self._cmd_output.pop(0)

    def _clear_cmd(self):
        with self._cmd_lock:
            self._cmd_output.clear()

    def _cmd_panel(self) -> Panel:
        with self._cmd_lock:
            lines = list(self._cmd_output)
        text = Text()
        for line in lines:
            text.append(line + "\n")
        return Panel(
            text,
            title="[bold]Command Output[/bold]",
            box=box.SIMPLE,
        )

    # ── Command handler ───────────────────────────────────────────────────────

    def _handle_command(self, raw: str, snap: dict):
        parts = raw.strip().lower().split()
        if not parts:
            return

        cmd = parts[0]

        if cmd == "help":
            for line in COMMANDS.split("\n"):
                self._print_cmd(line)

        elif cmd == "quit":
            self._running = False

        elif cmd == "clear":
            self._clear_cmd()

        elif cmd == "prices":
            self._print_cmd("─── Current Prices ───")
            for symbol in SYMBOL_LIST:
                price = snap["prices"].get(symbol, 0)
                prev  = snap["prev_prices"].get(symbol, price)
                arrow = "▲" if price > prev else ("▼" if price < prev else "─")
                halt  = "⛔" if snap["halted"].get(symbol) else ""
                self._print_cmd(
                    f"  {symbol:6} ${price:.2f} {arrow} {halt}"
                )

        elif cmd == "sym" and len(parts) >= 2:
            sym = parts[1].upper()
            if sym in SYMBOL_LIST:
                self.focus_symbol  = sym
                self.book_symbol   = sym
                self.candle_symbol = sym
                self._print_cmd(f"[green]Focus switched to {sym}[/green]")
            else:
                self._print_cmd(
                    f"[red]Unknown symbol '{sym}'. "
                    f"Valid: {', '.join(SYMBOL_LIST)}[/red]"
                )

        elif cmd == "book" and len(parts) >= 2:
            sym = parts[1].upper()
            if sym in SYMBOL_LIST:
                self.book_symbol = sym
                self._print_cmd(f"[green]Order book → {sym}[/green]")
            else:
                self._print_cmd(f"[red]Unknown symbol '{sym}'[/red]")

        elif cmd == "candles" and len(parts) >= 2:
            sym = parts[1].upper()
            if sym in SYMBOL_LIST:
                self.candle_symbol = sym
                self._print_cmd(f"[green]Candles → {sym}[/green]")
            else:
                self._print_cmd(f"[red]Unknown symbol '{sym}'[/red]")

        elif cmd == "port":
            client_id = parts[1] if len(parts) >= 2 else USER_CLIENT_ID
            # expand short names
            shortcuts = {
                "mm"       : "bot-market-maker",
                "momentum" : "bot-momentum",
                "random"   : "bot-random",
                "meanrev"  : "bot-mean-reversion",
                "user"     : USER_CLIENT_ID,
            }
            client_id = shortcuts.get(client_id, client_id)
            port = snap["portfolios"].get(client_id)
            if not port:
                self._print_cmd(
                    f"[red]No portfolio for '{client_id}'. "
                    f"Try: user, mm, momentum, random, meanrev[/red]"
                )
                return

            self._print_cmd(f"─── Portfolio: {client_id} ───")
            holdings = port.get("holdings", {})
            avg_cost = port.get("avg_cost", {})
            prices   = snap["prices"]

            if not holdings:
                self._print_cmd("  No open positions.")
            else:
                for sym, qty in holdings.items():
                    avg   = avg_cost.get(sym, 0)
                    curr  = prices.get(sym, avg)
                    pnl   = (curr - avg) * qty
                    col   = "green" if pnl >= 0 else "red"
                    self._print_cmd(
                        f"  {sym}: {qty} shares  avg:${avg:.2f}  "
                        f"now:${curr:.2f}  "
                        f"[{col}]pnl:{pnl:+.2f}[/{col}]"
                    )

            cash   = port.get("cash", 0)
            r_pnl  = port.get("realised_pnl", 0)
            ur_pnl = port.get("unrealised_pnl", 0)
            sharpe = port.get("sharpe")
            self._print_cmd(f"  Cash:       ${cash:,.2f}")
            self._print_cmd(f"  Realised:   {r_pnl:+.2f}")
            self._print_cmd(f"  Unrealised: {ur_pnl:+.2f}")
            if sharpe:
                self._print_cmd(f"  Sharpe:     {sharpe:.3f}")

        elif cmd == "leaders":
            portfolios = snap["portfolios"]
            if not portfolios:
                self._print_cmd("[yellow]No trade data yet.[/yellow]")
                return

            rows = sorted(
                [
                    (cid, p.get("realised_pnl", 0) + p.get("unrealised_pnl", 0))
                    for cid, p in portfolios.items()
                ],
                key=lambda x: x[1], reverse=True,
            )
            self._print_cmd("─── Leaderboard ───")
            medals = ["🥇", "🥈", "🥉"]
            for i, (cid, pnl) in enumerate(rows):
                medal = medals[i] if i < 3 else f"  {i+1}."
                col   = "green" if pnl >= 0 else "red"
                name  = cid.replace("bot-", "")
                self._print_cmd(
                    f"  {medal} {name:20} [{col}]{pnl:+.2f}[/{col}]"
                )

        else:
            self._print_cmd(
                f"[yellow]Unknown command '{cmd}'. "
                f"Type 'help' for commands.[/yellow]"
            )

    # ── Input thread ──────────────────────────────────────────────────────────

    def _input_loop(self, console: Console):
        """
        Runs in a background thread.
        Reads input without blocking the render loop.
        Rich Live takes over the screen so we print
        the prompt to stderr to keep it visible.
        """
        while self._running:
            try:
                sys.stderr.write("cmd> ")
                sys.stderr.flush()
                raw = sys.stdin.readline().strip()
                if not raw:
                    continue
                snap = self._state.snapshot()
                self._handle_command(raw, snap)
                if raw.lower() in ("quit", "exit", "q"):
                    self._running = False
                    break
            except (KeyboardInterrupt, EOFError):
                self._running = False
                break

    # ── Main start ────────────────────────────────────────────────────────────

    def start(self):
        self._running = True

        handlers = {
            "price"    : self._state.update_price,
            "candle"   : self._state.update_candle,
            "sentiment": self._state.update_sentiment,
            "portfolio": self._state.update_portfolio,
            "halt"     : self._state.update_halt,
            "expired"  : self._state.add_expired,
        }

        for name, handler in handlers.items():
            t = threading.Thread(
                target=self._listen_loop,
                args=(name, handler),
                name=f"dashboard-{name}",
                daemon=True,
            )
            t.start()

        console = Console()
        console.print(
            f"\n[bold cyan]Dashboard — focus: {self.focus_symbol}[/bold cyan]"
        )
        console.print(
            "[dim]Type commands in the prompt below. "
            "Type 'help' for commands.[/dim]\n"
        )

        # input in background thread so render loop stays smooth
        input_thread = threading.Thread(
            target=self._input_loop,
            args=(console,),
            name="dashboard-input",
            daemon=True,
        )
        input_thread.start()

        try:
            with Live(
                self._full_layout(self._state.snapshot()),
                console=console,
                refresh_per_second=REFRESH_RATE,
                screen=True,
            ) as live:
                while self._running:
                    snap = self._state.snapshot()
                    live.update(self._full_layout(snap))
                    sleep(1 / REFRESH_RATE)

        except KeyboardInterrupt:
            pass
        finally:
            self._running = False
            console.print("\n[cyan]Dashboard stopped.[/cyan]")

    def stop(self):
        self._running = False

    def _full_layout(self, snap: dict) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="ticker",   size=3),
            Layout(name="row1",     size=18),
            Layout(name="row2",     size=10),
            Layout(name="row3",     size=10),
            Layout(name="cmd_out",  size=8),
        )

        layout["row1"].split_row(
            Layout(name="orderbook", ratio=1),
            Layout(name="candles",   ratio=2),
            Layout(name="col3",      ratio=1),
        )
        layout["col3"].split_column(
            Layout(name="indicators", ratio=3),
            Layout(name="events",     ratio=1),
        )
        layout["row2"].split_row(
            Layout(name="sentiment", ratio=2),
            Layout(name="pnl_chart", ratio=3),
        )

        layout["ticker"].update(_ticker_panel(snap))
        layout["orderbook"].update(
            _orderbook_panel(snap, self.book_symbol)
        )
        layout["candles"].update(
            _candlestick_panel(snap, self.candle_symbol)
        )
        layout["indicators"].update(
            _indicators_panel(snap, self.focus_symbol)
        )
        layout["events"].update(_events_panel(snap))
        layout["sentiment"].update(_sentiment_gauge_panel(snap))
        layout["pnl_chart"].update(_pnl_chart_panel(snap))
        layout["row3"].update(_leaderboard_panel(snap))
        layout["cmd_out"].update(self._cmd_panel())

        return layout

    def _listen_loop(self, name: str, handler):
        consumer = self._consumers[name]
        while self._running:
            msg = consumer.poll_once(timeout=0.5)
            if msg:
                try:
                    handler(msg)
                except Exception as e:
                    logger.error(f"[dashboard:{name}] error: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def run_dashboard(focus_symbol: str = "PEAR"):
    logging.basicConfig(level=logging.WARNING)
    from core.kafka_client import ensure_topics
    ensure_topics()
    Dashboard(focus_symbol=focus_symbol).start()


if __name__ == "__main__":
    import sys
    symbol = sys.argv[1].upper() if len(sys.argv) > 1 else "PEAR"
    run_dashboard(symbol)