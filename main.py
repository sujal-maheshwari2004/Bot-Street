"""
Main Runner — Starts the entire market simulator

Modes:
  python main.py sim                    → simulation only, headless
  python main.py api                    → simulation + FastAPI
  python main.py dashboard --symbol X  → simulation + terminal dashboard
  python main.py all                    → simulation + API + dashboard
  python main.py dash --symbol X        → dashboard only (sim already running)
  python main.py trade                  → interactive trading CLI
  python main.py terminal               → alias for trade
"""

import logging
import threading
import time

import click
from rich.console import Console

from core.kafka_client import ensure_topics
from engine.matching_engine import run_all_engines
from engine.portfolio_ledger import PortfolioLedger
from engine.circuit_breaker import CircuitBreaker
from engine.candle_aggregator import CandleAggregator
from engine.trade_logger import TradeLogger
from market.price_feed import PriceFeed
from market.sentiment_engine import SentimentEngine
from participants.market_maker import MarketMakerBot
from participants.momentum_bot import MomentumBot
from participants.random_bot import RandomBot
from participants.mean_reversion_bot import MeanReversionBot

console = Console()
logger  = logging.getLogger(__name__)


# ── Service launcher ──────────────────────────────────────────────────────────

def _thread(target, name: str) -> threading.Thread:
    t = threading.Thread(target=target, name=name, daemon=True)
    t.start()
    return t


class SimulatorRunner:
    """
    Owns and manages all simulation services.
    Can be started headless, with API, or with dashboard.
    """

    def __init__(self):
        self.ledger     = None
        self.cb         = None
        self.candles    = None
        self.trade_log  = None
        self.price_feed = None
        self.sentiment  = None
        self.engines    = {}
        self.bots       = []
        self._threads   : list[threading.Thread] = []

    def start_services(self):
        """Bootstrap Kafka and start all core services."""
        console.print("[cyan]→ Ensuring Kafka topics...[/cyan]")
        ensure_topics()

        console.print("[cyan]→ Starting matching engines...[/cyan]")
        self.engines, _ = run_all_engines()

        console.print("[cyan]→ Starting portfolio ledger...[/cyan]")
        self.ledger = PortfolioLedger()
        self._threads.append(_thread(self.ledger.start, "ledger"))

        console.print("[cyan]→ Starting circuit breaker...[/cyan]")
        self.cb = CircuitBreaker()
        self._threads.append(_thread(self.cb.start, "circuit-breaker"))

        console.print("[cyan]→ Starting candle aggregator...[/cyan]")
        self.candles = CandleAggregator()
        self._threads.append(_thread(self.candles.start, "candle-agg"))

        console.print("[cyan]→ Starting trade logger...[/cyan]")
        self.trade_log = TradeLogger()
        self._threads.append(_thread(self.trade_log.start, "trade-logger"))

        console.print("[cyan]→ Starting price feed...[/cyan]")
        self.price_feed = PriceFeed()
        self._threads.append(_thread(self.price_feed.start, "price-feed"))

        console.print("[cyan]→ Starting sentiment engine...[/cyan]")
        self.sentiment = SentimentEngine()
        self._threads.append(_thread(self.sentiment.start, "sentiment"))

        console.print("[cyan]→ Starting bots...[/cyan]")
        self.bots = [
            MarketMakerBot(),
            MomentumBot(),
            RandomBot(),
            MeanReversionBot(),
        ]
        for bot in self.bots:
            self._threads.append(_thread(bot.start, bot.client_id))

        # give services a moment to connect
        time.sleep(2)
        console.print("[bold green]✓ All services started[/bold green]\n")

    def stop_services(self):
        console.print("\n[yellow]Shutting down...[/yellow]")
        services = [
            self.ledger, self.cb, self.candles,
            self.trade_log, self.price_feed, self.sentiment,
        ] + self.bots
        for svc in services:
            if svc:
                try:
                    svc.stop()
                except Exception:
                    pass
        console.print("[cyan]Goodbye.[/cyan]")

    def run_api(self, host: str = "0.0.0.0", port: int = 8000):
        """Start FastAPI with uvicorn."""
        import uvicorn
        from api.routes import market, portfolio, market_status

        market.inject(
            self.price_feed, self.candles,
            self.sentiment, self.engines
        )
        portfolio.inject(self.ledger, self.price_feed)
        market_status.inject(self.cb, self.ledger, self.price_feed)

        console.print(
            f"[bold green]API running at "
            f"http://{host}:{port}[/bold green]"
        )
        console.print(
            f"[dim]Docs: http://localhost:{port}/docs[/dim]\n"
        )

        uvicorn.run(
            "api.main:app",
            host=host,
            port=port,
            log_level="warning",
            reload=False,
        )

    def run_dashboard(self, symbol: str = "PEAR"):
        """Start Rich terminal dashboard."""
        from display.dashboard import Dashboard
        Dashboard(focus_symbol=symbol).start()

    def run_headless(self):
        """Run simulation with no UI — just price output."""
        console.print(
            "[bold]Running headless simulation. "
            "Press Ctrl+C to stop.[/bold]"
        )
        console.print(
            "[dim]Open a second terminal and run:\n"
            "  uv run python main.py trade     ← trading CLI\n"
            "  uv run python main.py dash      ← dashboard only[/dim]\n"
        )
        try:
            while True:
                time.sleep(5)
                prices = (
                    self.price_feed.get_all_prices()
                    if self.price_feed else {}
                )
                for symbol, price in prices.items():
                    console.print(
                        f"  [cyan]{symbol}[/cyan] ${price:.2f}",
                        end="  "
                    )
                console.print()
        except KeyboardInterrupt:
            pass


# ── CLI ───────────────────────────────────────────────────────────────────────

@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging")
def cli(debug: bool):
    """Market Simulator — Kafka-backed algorithmic trading simulation."""
    level = logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


@cli.command()
def sim():
    """
    Run simulation headless — no UI, no API.

    Open second terminals for dashboard and trading:
      uv run python main.py dash
      uv run python main.py trade
    """
    runner = SimulatorRunner()
    console.print("\n[bold cyan]═══ Market Simulator ═══[/bold cyan]\n")
    runner.start_services()
    try:
        runner.run_headless()
    finally:
        runner.stop_services()


@cli.command()
@click.option("--host", default="0.0.0.0", help="API host")
@click.option("--port", default=8000,      help="API port")
def api(host: str, port: int):
    """
    Run simulation + FastAPI REST server.

    Docs available at http://localhost:8000/docs
    """
    runner = SimulatorRunner()
    console.print(
        "\n[bold cyan]═══ Market Simulator + API ═══[/bold cyan]\n"
    )
    runner.start_services()
    try:
        runner.run_api(host=host, port=port)
    finally:
        runner.stop_services()


@cli.command()
@click.option("--symbol", default="PEAR", help="Symbol to focus on")
def dashboard(symbol: str):
    """
    Run simulation + terminal dashboard.

    Open a second terminal for trading:
      uv run python main.py trade
    """
    runner = SimulatorRunner()
    console.print(
        "\n[bold cyan]═══ Market Simulator + Dashboard ═══[/bold cyan]\n"
    )
    console.print(
        "[dim]Tip: open a second terminal and run: "
        "uv run python main.py trade[/dim]\n"
    )
    runner.start_services()
    try:
        runner.run_dashboard(symbol=symbol.upper())
    finally:
        runner.stop_services()


@cli.command()
@click.option("--symbol", default="PEAR", help="Symbol to focus on")
def dash(symbol: str):
    """
    Dashboard only — connects to an already-running simulation.

    Use this in a second terminal when sim/api/dashboard is running.
    """
    logging.basicConfig(level=logging.WARNING)
    console.print(
        "\n[bold cyan]═══ Dashboard (connecting...) ═══[/bold cyan]\n"
    )
    ensure_topics()
    from display.dashboard import Dashboard
    Dashboard(focus_symbol=symbol.upper()).start()


@cli.command()
def trade():
    """
    Interactive trading CLI.

    Use this in a second terminal alongside dashboard or sim.
    Commands: buy, sell, prices, portfolio, help
    """
    logging.basicConfig(level=logging.WARNING)
    console.print(
        "\n[bold cyan]═══ Trading CLI ═══[/bold cyan]\n"
    )
    from user.cli import run_cli
    run_cli()


@cli.command()
def terminal():
    """Alias for 'trade' command."""
    logging.basicConfig(level=logging.WARNING)
    from user.cli import run_cli
    run_cli()


@cli.command("all")
@click.option("--host",   default="0.0.0.0", help="API host")
@click.option("--port",   default=8000,       help="API port")
@click.option("--symbol", default="PEAR",     help="Dashboard focus symbol")
def run_all(host: str, port: int, symbol: str):
    """
    Run everything — simulation + API + dashboard.

    API docs at http://localhost:8000/docs
    Open a second terminal for trading:
      uv run python main.py trade
    """
    runner = SimulatorRunner()
    console.print(
        "\n[bold cyan]═══ Market Simulator — Full Mode ═══[/bold cyan]\n"
    )
    console.print(
        "[dim]Tip: open a second terminal and run: "
        "uv run python main.py trade[/dim]\n"
    )
    runner.start_services()

    # API in background thread
    api_thread = threading.Thread(
        target=runner.run_api,
        args=(host, port),
        name="api-server",
        daemon=True,
    )
    api_thread.start()
    time.sleep(1)

    try:
        runner.run_dashboard(symbol=symbol.upper())
    finally:
        runner.stop_services()


if __name__ == "__main__":
    cli()