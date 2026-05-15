"""Market data — prices, order book, candles, indicators, sentiment."""

from fastapi import APIRouter, HTTPException, Query
from api.models import (
    PriceResponse, OrderBookResponse, OrderBookLevel,
    CandleResponse, SentimentResponse,
)
from config import SYMBOL_LIST, SYMBOLS

router = APIRouter(prefix="/market", tags=["Market Data"])

_cache = None


def inject_cache(cache):
    global _cache
    _cache = cache


# keep old inject for backward compat with any direct callers
def inject(price_feed, candle_agg, sentiment_engine, engines):
    pass


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
    return PriceResponse(
        symbol=s, price=price,
        vwap=None, bid=None, ask=None, spread=None, volume=0,
        rsi=None, macd=None, macd_signal=None,
        bb_upper=None, bb_lower=None,
        ema_short=None, ema_long=None, ofi=None,
        timestamp=0.0,
    )


@router.get("/{symbol}/orderbook", response_model=OrderBookResponse)
def get_order_book(symbol: str, levels: int = Query(default=10, ge=1, le=20)):
    s = _require_symbol(symbol)
    # order book depth lives in the engine pod — API pod has no engine reference.
    # Return empty depth with note; frontend handles gracefully.
    return OrderBookResponse(symbol=s, bids=[], asks=[], spread=None, mid=None)


@router.get("/{symbol}/candles", response_model=list[CandleResponse])
def get_candles(symbol: str, n: int = Query(default=20, ge=1, le=100)):
    s = _require_symbol(symbol)
    if _cache is None:
        raise HTTPException(status_code=503, detail="Cache not ready.")
    raw = _cache.candles.get(s, [])[-n:]
    return [
        CandleResponse(
            symbol=c.get("symbol", s),
            open=c["open"], high=c["high"], low=c["low"], close=c["close"],
            volume=c["volume"], vwap=c.get("vwap", 0.0),
            trade_count=c.get("trade_count", 0),
            interval_s=c.get("interval_s", 10),
            open_time=c["open_time"], close_time=c["close_time"],
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
        symbol=s,
        sentiment=sent["sentiment"],
        strength=sent["strength"],
        buy_ratio=sent["buy_ratio"],
        trade_velocity=sent["trade_velocity"],
        timestamp=sent.get("timestamp", 0.0),
    )


@router.get("/all/prices")
def get_all_prices():
    if _cache is None:
        return {s: SYMBOLS[s][1] for s in SYMBOL_LIST}
    return dict(_cache.prices)