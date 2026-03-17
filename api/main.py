"""
FastAPI Application — Market Simulator REST API

All market services are started as background threads when the
API starts. The API then exposes their state via REST endpoints.
"""

import logging
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

from api.routes import orders, market, portfolio, market_status

logger = logging.getLogger(__name__)

# ── Global service instances ──────────────────────────────────────────────────
# Shared across routes via injection

_services = {}


def _start_in_thread(target, name: str):
    t = threading.Thread(target=target, name=name, daemon=True)
    t.start()
    return t


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup: bootstrap Kafka topics, start all services.
    Shutdown: stop all services cleanly.
    """
    logger.info("[api] starting market simulator services...")

    # ── Kafka topics ──────────────────────────────────────────────────
    ensure_topics()

    # ── Core engines ──────────────────────────────────────────────────
    ledger    = PortfolioLedger()
    cb        = CircuitBreaker()
    candles   = CandleAggregator()
    trade_log = TradeLogger()
    price_feed = PriceFeed()
    sentiment  = SentimentEngine()

    # ── Matching engines (one per symbol) ─────────────────────────────
    engines, _ = run_all_engines()

    # ── Bots ──────────────────────────────────────────────────────────
    bots = [
        MarketMakerBot(),
        MomentumBot(),
        RandomBot(),
        MeanReversionBot(),
    ]

    # ── Start all services in background threads ───────────────────────
    _start_in_thread(ledger.start,    "ledger")
    _start_in_thread(cb.start,        "circuit-breaker")
    _start_in_thread(candles.start,   "candle-aggregator")
    _start_in_thread(trade_log.start, "trade-logger")
    _start_in_thread(price_feed.start,"price-feed")
    _start_in_thread(sentiment.start, "sentiment-engine")

    for bot in bots:
        _start_in_thread(bot.start, bot.client_id)

    # store for shutdown
    _services.update({
        "ledger"    : ledger,
        "cb"        : cb,
        "candles"   : candles,
        "price_feed": price_feed,
        "sentiment" : sentiment,
        "engines"   : engines,
        "bots"      : bots,
    })

    # ── Inject into routes ─────────────────────────────────────────────
    market.inject(price_feed, candles, sentiment, engines)
    portfolio.inject(ledger, price_feed)
    market_status.inject(cb, ledger, price_feed)

    logger.info("[api] all services started")
    yield

    # ── Shutdown ───────────────────────────────────────────────────────
    logger.info("[api] shutting down...")
    for svc in [ledger, cb, candles, trade_log, price_feed, sentiment]:
        svc.stop()
    for bot in bots:
        bot.stop()


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Market Simulator API",
    description = "Kafka-backed algorithmic market simulator",
    version     = "0.1.0",
    lifespan    = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(orders.router)
app.include_router(market.router)
app.include_router(portfolio.router)
app.include_router(market_status.router)


@app.get("/")
def root():
    return {
        "name"   : "Market Simulator API",
        "version": "0.1.0",
        "docs"   : "/docs",
        "health" : "/system/health",
    }