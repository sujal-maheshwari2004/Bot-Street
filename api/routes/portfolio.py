"""Portfolio — holdings, P&L, risk metrics per client."""

from fastapi import APIRouter, HTTPException
from api.models import PortfolioResponse, HoldingResponse

router = APIRouter(prefix="/portfolio", tags=["Portfolio"])

_ledger      = None
_price_feed  = None


def inject(ledger, price_feed):
    global _ledger, _price_feed
    _ledger     = ledger
    _price_feed = price_feed


@router.get("/{client_id}", response_model=PortfolioResponse)
def get_portfolio(client_id: str):
    """Holdings, cash, P&L and risk metrics for a client."""
    if _ledger is None:
        raise HTTPException(status_code=503, detail="Ledger not running.")

    portfolio = _ledger.get_portfolio(client_id)
    if portfolio is None:
        raise HTTPException(
            status_code=404,
            detail=f"Client '{client_id}' not found."
        )

    prices = _price_feed.get_all_prices() if _price_feed else {}
    snap   = portfolio.to_snapshot(prices)

    holdings = []
    for symbol, qty in snap.holdings.items():
        avg   = snap.avg_cost.get(symbol, 0)
        curr  = prices.get(symbol)
        unr   = (curr - avg) * qty if curr else None
        holdings.append(HoldingResponse(
            symbol     = symbol,
            quantity   = qty,
            avg_cost   = avg,
            curr_price = curr,
            unrealised = round(unr, 2) if unr is not None else None,
        ))

    return PortfolioResponse(
        client_id      = client_id,
        holdings       = holdings,
        cash           = snap.cash,
        realised_pnl   = snap.realised_pnl,
        unrealised_pnl = snap.unrealised_pnl,
        sharpe         = snap.sharpe,
        max_drawdown   = snap.max_drawdown,
        var_95         = snap.var_95,
        trade_count    = portfolio._trade_count,
    )