"""Market status — health, halt status, leaderboard."""

from fastapi import APIRouter, HTTPException
from api.models import MarketStatusResponse, LeaderboardEntry, HealthResponse
from config import SYMBOL_LIST, SYMBOLS
from core.kafka_client import AdminClient
from config import BOOTSTRAP_SERVERS, ALL_TOPICS

router = APIRouter(prefix="/system", tags=["System"])

_circuit_breaker = None
_ledger          = None
_price_feed      = None


def inject(circuit_breaker, ledger, price_feed):
    global _circuit_breaker, _ledger, _price_feed
    _circuit_breaker = circuit_breaker
    _ledger          = ledger
    _price_feed      = price_feed


@router.get("/health", response_model=HealthResponse)
def health():
    """Kafka connectivity and topic status."""
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
    """Halt/active status for all symbols."""
    prices = _price_feed.get_all_prices() if _price_feed else {}
    result = []
    for symbol in SYMBOL_LIST:
        halted = (
            _circuit_breaker.is_halted(symbol)
            if _circuit_breaker else False
        )
        name, _, profile = SYMBOLS[symbol]
        result.append(MarketStatusResponse(
            symbol  = symbol,
            halted  = halted,
            name    = name,
            price   = prices.get(symbol),
            profile = profile,
        ))
    return result


@router.get("/leaderboard", response_model=list[LeaderboardEntry])
def leaderboard():
    """All participants ranked by total P&L."""
    if _ledger is None:
        raise HTTPException(status_code=503, detail="Ledger not running.")

    rows = _ledger.get_leaderboard()
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