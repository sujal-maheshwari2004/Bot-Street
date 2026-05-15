"""Market status — health, halt status, leaderboard."""

from fastapi import APIRouter, HTTPException
from api.models import MarketStatusResponse, LeaderboardEntry, HealthResponse
from config import SYMBOL_LIST, SYMBOLS, BOOTSTRAP_SERVERS, ALL_TOPICS
from core.kafka_client import AdminClient

router = APIRouter(prefix="/system", tags=["System"])

_cache = None


def inject_cache(cache):
    global _cache
    _cache = cache


def inject(circuit_breaker, ledger, price_feed):
    pass


@router.get("/health", response_model=HealthResponse)
def health():
    try:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
        existing = list(admin.list_topics(timeout=5).topics.keys())
        kafka_status = "ok"
    except Exception as e:
        existing = []
        kafka_status = f"error: {e}"

    return HealthResponse(
        status  = "ok" if kafka_status == "ok" else "degraded",
        kafka   = kafka_status,
        topics  = existing,
        symbols = SYMBOL_LIST,
    )


@router.get("/status", response_model=list[MarketStatusResponse])
def market_status():
    prices = dict(_cache.prices) if _cache else {}
    halted = dict(_cache.halted) if _cache else {}
    result = []
    for symbol in SYMBOL_LIST:
        name, _, profile = SYMBOLS[symbol]
        result.append(MarketStatusResponse(
            symbol  = symbol,
            halted  = halted.get(symbol, False),
            name    = name,
            price   = prices.get(symbol),
            profile = profile,
        ))
    return result


@router.get("/leaderboard", response_model=list[LeaderboardEntry])
def leaderboard():
    if _cache is None:
        raise HTTPException(status_code=503, detail="Cache not ready.")

    portfolios = dict(_cache.portfolios)
    prices     = dict(_cache.prices)

    rows = []
    for client_id, port in portfolios.items():
        r = port.get("realised_pnl", 0.0)
        u = port.get("unrealised_pnl", 0.0)
        rows.append({
            "client_id"    : client_id,
            "total_pnl"    : round(r + u, 2),
            "realised_pnl" : r,
            "unrealised_pnl": u,
            "cash"         : port.get("cash", 0.0),
            "sharpe"       : port.get("sharpe"),
            "max_drawdown" : port.get("max_drawdown"),
            "trade_count"  : port.get("trade_count", 0),
        })

    rows.sort(key=lambda r: r["total_pnl"], reverse=True)

    return [
        LeaderboardEntry(
            rank           = i + 1,
            client_id      = r["client_id"],
            total_pnl      = r["total_pnl"],
            realised_pnl   = r["realised_pnl"],
            unrealised_pnl = r["unrealised_pnl"],
            cash           = r["cash"],
            sharpe         = r["sharpe"],
            max_drawdown   = r["max_drawdown"],
            trade_count    = r["trade_count"],
        )
        for i, r in enumerate(rows)
    ]