"""Market data — prices, order book, candles, indicators, sentiment."""

from fastapi import APIRouter, HTTPException, Query
from api.models import (
    PriceResponse, OrderBookResponse,
    CandleResponse, SentimentResponse,
)
from config import SYMBOL_LIST, SYMBOLS

router = APIRouter(prefix="/market", tags=["Market Data"])

_cache = None


def inject_cache(cache):
    global _cache
    _cache = cache


def _require_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s not in SYMBOL_LIST:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol '{s}' not found. Valid: {SYMBOL_LIST}"
        )
    return s


@router.get("/{symbol}/price", response_model=PriceResponse)
def get_price(symbol: str):
    s = _require_symbol(symbol)
    if _cache is None:
        raise HTTPException(status_code=503, detail="Cache not ready.")

    price = _cache.prices.get(s)
    if price is None:
        raise HTTPException(status_code=404, detail=f"No price data for {s}.")

    # Read full PriceUpdate from cache — populated by price-feed via Kafka.
    # Falls back to nulls gracefully if not yet received (cold start).
    d = _cache.price_data.get(s, {})

    return PriceResponse(
        symbol      = s,
        price       = price,
        vwap        = d.get("vwap"),
        bid         = d.get("bid"),
        ask         = d.get("ask"),
        spread      = d.get("spread"),
        volume      = d.get("volume", 0),
        rsi         = d.get("rsi"),
        macd        = d.get("macd"),
        macd_signal = d.get("macd_signal"),
        bb_upper    = d.get("bb_upper"),
        bb_lower    = d.get("bb_lower"),
        ema_short   = d.get("ema_short"),
        ema_long    = d.get("ema_long"),
        ofi         = d.get("ofi"),
        timestamp   = d.get("timestamp", 0.0),
    )


@router.get("/{symbol}/orderbook", response_model=OrderBookResponse)
def get_order_book(symbol: str, levels: int = Query(default=10, ge=1, le=20)):
    s = _require_symbol(symbol)
    # Order book depth lives in the engine pod — API pod has no engine reference.
    return OrderBookResponse(symbol=s, bids=[], asks=[], spread=None, mid=None)


@router.get("/{symbol}/candles", response_model=list[CandleResponse])
def get_candles(symbol: str, n: int = Query(default=20, ge=1, le=100)):
    s = _require_symbol(symbol)
    if _cache is None:
        raise HTTPException(status_code=503, detail="Cache not ready.")
    raw = _cache.candles.get(s, [])[-n:]
    return [
        CandleResponse(
            symbol      = c.get("symbol", s),
            open        = c["open"],
            high        = c["high"],
            low         = c["low"],
            close       = c["close"],
            volume      = c["volume"],
            vwap        = c.get("vwap", 0.0),
            trade_count = c.get("trade_count", 0),
            interval_s  = c.get("interval_s", 10),
            open_time   = c["open_time"],
            close_time  = c["close_time"],
        )
        for c in raw
    ]


@router.get("/{symbol}/sentiment", response_model=SentimentResponse)
def get_sentiment(symbol: str):
    s = _require_symbol(symbol)
    if _cache is None:
        raise HTTPException(status_code=503, detail="Cache not ready.")
    sent = _cache.sentiment.get(s)
    if sent is None:
        raise HTTPException(status_code=404, detail=f"No sentiment data for {s} yet.")
    return SentimentResponse(
        symbol         = s,
        sentiment      = sent["sentiment"],
        strength       = sent["strength"],
        buy_ratio      = sent["buy_ratio"],
        trade_velocity = sent["trade_velocity"],
        timestamp      = sent.get("timestamp", 0.0),
    )


@router.get("/all/prices")
def get_all_prices():
    if _cache is None:
        return {s: SYMBOLS[s][1] for s in SYMBOL_LIST}
    return dict(_cache.prices)