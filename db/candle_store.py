import logging
from db.client import get_db

logger = logging.getLogger(__name__)

COLLECTION = "candles"


def insert_candle(candle: dict) -> None:
    try:
        db = get_db()
        db[COLLECTION].insert_one(candle)
        logger.debug(f"[candle-store] inserted candle {candle.get('symbol')} @ {candle.get('open_time')}")
    except Exception as e:
        logger.error(f"[candle-store] insert failed: {e}")


def get_candles(symbol: str, limit: int = 20) -> list[dict]:
    try:
        db = get_db()
        cursor = (
            db[COLLECTION]
            .find({"symbol": symbol}, {"_id": 0})
            .sort("open_time", -1)
            .limit(limit)
        )
        return list(reversed(list(cursor)))  # oldest first
    except Exception as e:
        logger.error(f"[candle-store] query failed: {e}")
        return []