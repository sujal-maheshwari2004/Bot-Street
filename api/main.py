"""
FastAPI Application — Market Simulator REST API (K8s edition)

In K8s each service runs in its own pod. This pod only:
  - Consumes Kafka topics to build in-memory caches
  - Serves the REST API
  - Mounts the MCP server at /mcp

All other services (engines, bots, ledger, etc.) run as separate
Deployments and communicate exclusively via Kafka.
"""

import logging
import threading
from contextlib import asynccontextmanager
from collections import defaultdict

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.kafka_client import MarketConsumer, ensure_topics
from config import (
    TOPIC_PRICE_UPDATE, TOPIC_CANDLES, TOPIC_MARKET_SENTIMENT,
    TOPIC_PORTFOLIO_SNAP, TOPIC_MARKET_HALT,
    SYMBOL_LIST, SYMBOLS,
)
from api.routes import orders, market, portfolio, market_status

logger = logging.getLogger(__name__)


# ── In-memory cache ───────────────────────────────────────────────────────────

class APICache:
    def __init__(self):
        self.prices     : dict[str, float] = {s: SYMBOLS[s][1] for s in SYMBOL_LIST}
        self.sentiment  : dict[str, dict]  = {}
        self.candles    : dict[str, list]  = defaultdict(list)
        self.portfolios : dict[str, dict]  = {}
        self.halted     : dict[str, bool]  = {s: False for s in SYMBOL_LIST}
        self._lock = threading.Lock()

    def update_price(self, msg: dict):
        symbol = msg.get("symbol")
        price  = msg.get("price")
        if symbol and price:
            with self._lock:
                self.prices[symbol] = price

    def update_sentiment(self, msg: dict):
        symbol = msg.get("symbol")
        if symbol:
            with self._lock:
                self.sentiment[symbol] = msg

    def update_candle(self, msg: dict):
        symbol = msg.get("symbol")
        if symbol:
            with self._lock:
                self.candles[symbol].append(msg)
                if len(self.candles[symbol]) > 100:
                    self.candles[symbol].pop(0)

    def update_portfolio(self, msg: dict):
        client_id = msg.get("client_id")
        if client_id:
            with self._lock:
                self.portfolios[client_id] = msg

    def update_halt(self, msg: dict):
        symbol = msg.get("symbol")
        if symbol:
            with self._lock:
                self.halted[symbol] = msg.get("status") == "halted"


cache = APICache()


# ── Kafka consumer threads ────────────────────────────────────────────────────

def _start_consumer(group_id: str, topics: list[str], handler, name: str):
    consumer = MarketConsumer(group_id=group_id, topics=topics, offset="latest")

    def loop():
        while True:
            msg = consumer.poll_once(timeout=0.5)
            if msg:
                try:
                    handler(msg)
                except Exception as e:
                    logger.error(f"[api-cache:{name}] handler error: {e}")

    t = threading.Thread(target=loop, name=f"api-cache-{name}", daemon=True)
    t.start()
    return t


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[api] starting Kafka cache consumers...")
    ensure_topics()

    _start_consumer("api-cache-prices",    [TOPIC_PRICE_UPDATE],     cache.update_price,     "prices")
    _start_consumer("api-cache-sentiment", [TOPIC_MARKET_SENTIMENT], cache.update_sentiment, "sentiment")
    _start_consumer("api-cache-candles",   [TOPIC_CANDLES],          cache.update_candle,    "candles")
    _start_consumer("api-cache-portfolio", [TOPIC_PORTFOLIO_SNAP],   cache.update_portfolio, "portfolio")
    _start_consumer("api-cache-halts",     [TOPIC_MARKET_HALT],      cache.update_halt,      "halts")

    # inject cache into routes and MCP
    market.inject_cache(cache)
    portfolio.inject_cache(cache)
    market_status.inject_cache(cache)

    from api.mcp_server import set_cache
    set_cache(cache)

    logger.info("[api] cache consumers running")
    yield
    logger.info("[api] shutting down")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Bot Street API",
    description = "Kafka-backed algorithmic market simulator",
    version     = "0.2.0",
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

# ── MCP sub-app ───────────────────────────────────────────────────────────────

from api.mcp_server import mcp
app.mount("/mcp", mcp.streamable_http_app())


@app.get("/")
def root():
    return {
        "name"   : "Bot Street API",
        "version": "0.2.0",
        "docs"   : "/docs",
        "health" : "/system/health",
        "mcp"    : "/mcp",
    }