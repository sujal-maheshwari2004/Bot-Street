"""
User CLI — Manual Order Entry via Terminal

Allows the user to place orders directly into the market
using the same Kafka producer interface as the bots.

Commands:
  buy  <symbol> <qty> <price>   → limit buy order
  sell <symbol> <qty> <price>   → limit sell order
  buy  <symbol> <qty>           → market buy order
  sell <symbol> <qty>           → market sell order
  portfolio                     → show current holdings
  prices                        → show all current prices
  book <symbol>                 → show order book depth
  help                          → show commands
  quit                          → exit
"""

import logging
import threading
from time import time

import click
from rich.console import Console
from rich.table import Table
from rich import box

from config import (
    TOPIC_MARKET_ORDERS, TOPIC_PORTFOLIO_SNAP, TOPIC_PRICE_UPDATE,
    SYMBOL_LIST, SYMBOLS, USER_CLIENT_ID,
)
from core.schemas import Order
from core.kafka_client import MarketProducer, MarketConsumer, ensure_topics

logger  = logging.getLogger(__name__)
console = Console()


class UserSession:
    """
    Manages user state — current prices and portfolio snapshot.
    Updated in background by consuming price-update and portfolio-snapshot.
    """

    def __init__(self):
        self._prices    : dict[str, float] = {
            s: SYMBOLS[s][1] for s in SYMBOL_LIST
        }
        self._portfolio : dict             = {}
        self._lock      = threading.Lock()
        self._running   = False

        self._producer = MarketProducer(USER_CLIENT_ID)

        self._price_consumer = MarketConsumer(
            group_id="user-cli-prices",
            topics=[TOPIC_PRICE_UPDATE],
            offset="latest",
        )
        self._portfolio_consumer = MarketConsumer(
            group_id="user-cli-portfolio",
            topics=[TOPIC_PORTFOLIO_SNAP],
            offset="latest",
        )

    def start_listeners(self):
        """Start background threads to keep state fresh."""
        self._running = True
        for target, name in [
            (self._price_loop,     "user-price-listener"),
            (self._portfolio_loop, "user-portfolio-listener"),
        ]:
            t = threading.Thread(target=target, name=name, daemon=True)
            t.start()

    def stop(self):
        self._running = False
        self._producer.flush()

    # ── Order placement ───────────────────────────────────────────────────────

    def place_order(self, symbol: str, side: str, qty: int,
                    price: float | None) -> Order:
        order_type = "limit" if price else "market"
        order = Order(
            client_id  = USER_CLIENT_ID,
            symbol     = symbol,
            side       = side,
            order_type = order_type,
            quantity   = qty,
            price      = price,
        )
        self._producer.send_order(TOPIC_MARKET_ORDERS, order)
        self._producer.flush()
        return order

    # ── Display helpers ───────────────────────────────────────────────────────

    def show_prices(self):
        table = Table(title="Market Prices", box=box.SIMPLE_HEAVY)
        table.add_column("Symbol", style="cyan")
        table.add_column("Name", style="white")
        table.add_column("Price", style="green", justify="right")

        with self._lock:
            for symbol in SYMBOL_LIST:
                name  = SYMBOLS[symbol][0]
                price = self._prices.get(symbol, 0.0)
                table.add_row(symbol, name, f"${price:.2f}")

        console.print(table)

    def show_portfolio(self):
        with self._lock:
            port = self._portfolio

        if not port:
            console.print("[yellow]No portfolio data yet — place a trade first.[/yellow]")
            return

        # holdings table
        table = Table(title="Your Portfolio", box=box.SIMPLE_HEAVY)
        table.add_column("Symbol",   style="cyan")
        table.add_column("Qty",      justify="right")
        table.add_column("Avg Cost", justify="right", style="yellow")
        table.add_column("Curr Price", justify="right", style="green")
        table.add_column("Unreal P&L", justify="right")

        holdings = port.get("holdings", {})
        avg_cost = port.get("avg_cost", {})

        for symbol, qty in holdings.items():
            avg  = avg_cost.get(symbol, 0)
            curr = self._prices.get(symbol, avg)
            pnl  = (curr - avg) * qty
            pnl_str = f"[green]+${pnl:.2f}[/green]" if pnl >= 0 \
                      else f"[red]-${abs(pnl):.2f}[/red]"
            table.add_row(
                symbol, str(qty),
                f"${avg:.2f}", f"${curr:.2f}",
                pnl_str,
            )

        console.print(table)

        # summary row
        cash         = port.get("cash", 0)
        realised     = port.get("realised_pnl", 0)
        unrealised   = port.get("unrealised_pnl", 0)
        console.print(f"  Cash:          [green]${cash:,.2f}[/green]")
        console.print(f"  Realised P&L:  [{'green' if realised >= 0 else 'red'}]${realised:+.2f}[/]")
        console.print(f"  Unrealised P&L:[{'green' if unrealised >= 0 else 'red'}] ${unrealised:+.2f}[/]")
        if port.get("sharpe"):
            console.print(f"  Sharpe Ratio:  {port['sharpe']:.3f}")
        if port.get("var_95"):
            console.print(f"  VaR 95%:       ${port['var_95']:.2f}")

    # ── Background listeners ──────────────────────────────────────────────────

    def _price_loop(self):
        while self._running:
            msg = self._price_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            symbol = msg.get("symbol")
            price  = msg.get("price")
            if symbol and price:
                with self._lock:
                    self._prices[symbol] = price

    def _portfolio_loop(self):
        while self._running:
            msg = self._portfolio_consumer.poll_once(timeout=0.5)
            if msg is None:
                continue
            # only update for the user's own portfolio
            if msg.get("client_id") == USER_CLIENT_ID:
                with self._lock:
                    self._portfolio = msg


# ── CLI ───────────────────────────────────────────────────────────────────────

def run_cli():
    ensure_topics()
    session = UserSession()
    session.start_listeners()

    console.print("\n[bold cyan]═══ Market Simulator CLI ═══[/bold cyan]")
    console.print("Type [green]help[/green] for commands.\n")

    while True:
        try:
            raw = input("market> ").strip()
        except (KeyboardInterrupt, EOFError):
            break

        if not raw:
            continue

        parts = raw.lower().split()
        cmd   = parts[0]

        # ── help ──────────────────────────────────────────────────────
        if cmd == "help":
            console.print("""
[bold]Commands:[/bold]
  [cyan]buy  <SYMBOL> <qty> [price][/cyan]   limit or market buy
  [cyan]sell <SYMBOL> <qty> [price][/cyan]   limit or market sell
  [cyan]prices[/cyan]                         show all current prices
  [cyan]portfolio[/cyan]                      show your holdings and P&L
  [cyan]quit[/cyan]                           exit
            """)

        # ── quit ──────────────────────────────────────────────────────
        elif cmd in ("quit", "exit", "q"):
            break

        # ── prices ────────────────────────────────────────────────────
        elif cmd == "prices":
            session.show_prices()

        # ── portfolio ─────────────────────────────────────────────────
        elif cmd in ("portfolio", "port", "p"):
            session.show_portfolio()

        # ── buy / sell ────────────────────────────────────────────────
        elif cmd in ("buy", "sell"):
            if len(parts) < 3:
                console.print("[red]Usage: buy <SYMBOL> <qty> [price][/red]")
                continue

            symbol = parts[1].upper()
            if symbol not in SYMBOL_LIST:
                console.print(
                    f"[red]Unknown symbol '{symbol}'. "
                    f"Valid: {', '.join(SYMBOL_LIST)}[/red]"
                )
                continue

            try:
                qty = int(parts[2])
            except ValueError:
                console.print("[red]Quantity must be an integer.[/red]")
                continue

            if qty <= 0:
                console.print("[red]Quantity must be positive.[/red]")
                continue

            price = None
            if len(parts) >= 4:
                try:
                    price = float(parts[3])
                except ValueError:
                    console.print("[red]Price must be a number.[/red]")
                    continue

            try:
                order = session.place_order(symbol, cmd, qty, price)
                order_type = "market" if price is None else f"limit @ ${price:.2f}"
                console.print(
                    f"[green]✓ {cmd.upper()} {qty}x{symbol} "
                    f"{order_type} — order_id: {order.order_id[:8]}...[/green]"
                )
            except Exception as e:
                console.print(f"[red]Order failed: {e}[/red]")

        else:
            console.print(
                f"[yellow]Unknown command '{cmd}'. "
                f"Type 'help' for commands.[/yellow]"
            )

    session.stop()
    console.print("\n[cyan]Goodbye.[/cyan]")


@click.command()
@click.option("--debug", is_flag=True, help="Enable debug logging")
def main(debug: bool):
    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    run_cli()


if __name__ == "__main__":
    main()