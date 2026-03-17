"""Orders — place and cancel orders via REST."""

from fastapi import APIRouter, HTTPException, Depends
from api.models import PlaceOrderRequest, OrderResponse
from core.schemas import Order
from core.kafka_client import MarketProducer
from config import TOPIC_MARKET_ORDERS, SYMBOL_LIST

router = APIRouter(prefix="/orders", tags=["Orders"])

# shared producer — one instance per app lifecycle
_producer: MarketProducer | None = None

def get_producer() -> MarketProducer:
    global _producer
    if _producer is None:
        _producer = MarketProducer("api-orders")
    return _producer


@router.post("", response_model=OrderResponse)
def place_order(req: PlaceOrderRequest,
                producer: MarketProducer = Depends(get_producer)):
    """
    Place a buy or sell order into the market.

    - **market** order: fills immediately at best available price
    - **limit** order: rests in book until matched or TTL expires
    """
    if req.symbol not in SYMBOL_LIST:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown symbol '{req.symbol}'. Valid: {SYMBOL_LIST}"
        )
    if req.order_type == "limit" and req.price is None:
        raise HTTPException(
            status_code=400,
            detail="Limit orders require a price."
        )

    order = Order(
        client_id  = req.client_id,
        symbol     = req.symbol,
        side       = req.side,
        order_type = req.order_type,
        quantity   = req.quantity,
        price      = req.price,
    )

    producer.send_order(TOPIC_MARKET_ORDERS, order)
    producer.flush()

    return OrderResponse(
        order_id   = order.order_id,
        client_id  = order.client_id,
        symbol     = order.symbol,
        side       = order.side,
        order_type = order.order_type,
        quantity   = order.quantity,
        price      = order.price,
        timestamp  = order.timestamp,
        expires_at = order.expires_at,
        status     = "accepted",
    )


@router.delete("/{order_id}")
def cancel_order(order_id: str):
    """
    Cancel a resting limit order by order_id.
    Note: cancellation is best-effort — order may have already filled.
    """
    # In POC: cancellation requires direct access to matching engine.
    # The matching engine exposes cancel via its OrderBook.cancel_order().
    # Full implementation wires through app state in main.py.
    raise HTTPException(
        status_code=501,
        detail="Cancel not yet wired to matching engine. Coming in next build."
    )