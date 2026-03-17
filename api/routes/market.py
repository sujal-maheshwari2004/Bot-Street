"""Market data — prices, order book, candles, indicators, sentiment."""

from fastapi import APIRouter, HTTPException, Query
from api.models import (
    PriceResponse, OrderBookResponse, OrderBookLevel,
    CandleResponse, SentimentResponse,
)
from config import SYMBOL_LIST, SYMBOLS
from dataclasses import asdict

router = APIRouter(prefix="/market", tags=["Market Data"])

# these are injected by main.py at startup
_price_feed      = None
_candle_agg      = None
_sentiment_engine = None
_engines         = {}   # symbol → MatchingEngine


def inject(price_feed, candle_agg, sentiment_engine, engines):
    global _price_feed, _candle_agg, _sentiment_engine, _engines
    _price_feed       = price_feed
    _candle_agg       = candle_agg
    _sentiment_engine = sentiment_engine
    _engines          = engines


def _require_symbol(symbol: str):
    s = symbol.upper()
    if s not in SYMBOL_LIST:
        raise HTTPException(
            status_code=404,
            detail=f"Symbol '{s}' not found. Valid: {SYMBOL_LIST}"
        )
    return s


@router.get("/{symbol}/price", response_model=PriceResponse)
def get_price(symbol: str):
    """Latest price and all technical indicators for a symbol."""
    s = _require_symbol(symbol)

    if _price_feed is None:
        raise HTTPException(status_code=503, detail="Price feed not running.")

    price = _price_feed.get_price(s)
    if price is None:
        raise HTTPException(status_code=404, detail=f"No price data for {s}.")

    # get indicator snapshot from price feed's internal engine
    ind_engine = _price_feed._indicators.get(s)
    micro_engine = _price_feed._microstructure.get(s)

    ind_snap   = ind_engine.snapshot()  if ind_engine   else None
    micro_snap = micro_engine.snapshot() if micro_engine else None

    return PriceResponse(
        symbol      = s,
        price       = price,
        vwap        = ind_snap.vwap       if ind_snap else None,
        bid         = micro_snap.spread.bid if micro_snap else None,
        ask         = micro_snap.spread.ask if micro_snap else None,
        spread      = micro_snap.spread.absolute_spread if micro_snap else None,
        volume      = _price_feed._total_volume.get(s, 0),
        rsi         = ind_snap.rsi        if ind_snap else None,
        macd        = ind_snap.macd       if ind_snap else None,
        macd_signal = ind_snap.macd_signal if ind_snap else None,
        bb_upper    = ind_snap.bb_upper   if ind_snap else None,
        bb_lower    = ind_snap.bb_lower   if ind_snap else None,
        ema_short   = ind_snap.ema_short  if ind_snap else None,
        ema_long    = ind_snap.ema_long   if ind_snap else None,
        ofi         = micro_snap.flow.ofi if micro_snap else None,
        timestamp   = _price_feed._session_start,
    )


@router.get("/{symbol}/orderbook", response_model=OrderBookResponse)
def get_order_book(symbol: str,
                   levels: int = Query(default=10, ge=1, le=20)):
    """Top N bids and asks for a symbol."""
    s = _require_symbol(symbol)
    engine = _engines.get(s)

    if engine is None:
        raise HTTPException(status_code=503, detail="Matching engine not running.")

    depth = engine.get_depth(levels)
    bids  = [OrderBookLevel(price=p, quantity=q) for p, q in depth["bids"]]
    asks  = [OrderBookLevel(price=p, quantity=q) for p, q in depth["asks"]]

    return OrderBookResponse(
        symbol = s,
        bids   = bids,
        asks   = asks,
        spread = engine.get_spread(),
        mid    = engine.get_mid_price(),
    )


@router.get("/{symbol}/candles", response_model=list[CandleResponse])
def get_candles(symbol: str,
                n: int = Query(default=20, ge=1, le=100)):
    """Last N closed OHLCV candles for a symbol."""
    s = _require_symbol(symbol)

    if _candle_agg is None:
        raise HTTPException(status_code=503, detail="Candle aggregator not running.")

    candles = _candle_agg.get_history(s, n)
    return [
        CandleResponse(
            symbol      = c.symbol,
            open        = c.open,
            high        = c.high,
            low         = c.low,
            close       = c.close,
            volume      = c.volume,
            vwap        = c.vwap,
            trade_count = c.trade_count,
            interval_s  = c.interval_s,
            open_time   = c.open_time,
            close_time  = c.close_time,
        )
        for c in candles
    ]


@router.get("/{symbol}/sentiment", response_model=SentimentResponse)
def get_sentiment(symbol: str):
    """Current sentiment signal for a symbol."""
    s = _require_symbol(symbol)

    if _sentiment_engine is None:
        raise HTTPException(status_code=503, detail="Sentiment engine not running.")

    sentiment = _sentiment_engine.get_sentiment(s)
    if sentiment is None:
        raise HTTPException(status_code=404, detail=f"No sentiment data for {s} yet.")

    return SentimentResponse(
        symbol         = s,
        sentiment      = sentiment.sentiment,
        strength       = sentiment.strength,
        buy_ratio      = sentiment.buy_ratio,
        trade_velocity = sentiment.trade_velocity,
        timestamp      = sentiment.timestamp,
    )


@router.get("/all/prices")
def get_all_prices():
    """Current prices for all symbols."""
    if _price_feed is None:
        return {s: SYMBOLS[s][1] for s in SYMBOL_LIST}
    return _price_feed.get_all_prices()