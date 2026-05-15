"""Portfolio — holdings, P&L, risk metrics per client."""

from fastapi import APIRouter, HTTPException
from api.models import PortfolioResponse, HoldingResponse

router = APIRouter(prefix="/portfolio", tags=["Portfolio"])

_cache = None


def inject_cache(cache):
    global _cache
    _cache = cache


def inject(ledger, price_feed):
    pass


@router.get("/{client_id}", response_model=PortfolioResponse)
def get_portfolio(client_id: str):
    if _cache is None:
        raise HTTPException(status_code=503, detail="Cache not ready.")

    port = _cache.portfolios.get(client_id)
    if port is None:
        raise HTTPException(status_code=404, detail=f"Client '{client_id}' not found.")

    prices = dict(_cache.prices)
    raw_holdings = port.get("holdings", {})
    avg_cost     = port.get("avg_cost", {})

    holdings = []
    for sym, qty in raw_holdings.items():
        avg  = avg_cost.get(sym, 0.0)
        curr = prices.get(sym)
        unr  = round((curr - avg) * qty, 2) if curr else None
        holdings.append(HoldingResponse(
            symbol=sym, quantity=qty,
            avg_cost=avg, curr_price=curr, unrealised=unr,
        ))

    return PortfolioResponse(
        client_id      = client_id,
        holdings       = holdings,
        cash           = port.get("cash", 0.0),
        realised_pnl   = port.get("realised_pnl", 0.0),
        unrealised_pnl = port.get("unrealised_pnl", 0.0),
        sharpe         = port.get("sharpe"),
        max_drawdown   = port.get("max_drawdown"),
        var_95         = port.get("var_95"),
        trade_count    = port.get("trade_count", 0),
    )