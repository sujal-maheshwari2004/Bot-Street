import logging
from db.client import get_db

logger = logging.getLogger(__name__)

COLLECTION = "trades"


def insert_trade(trade: dict) -> None:
    try:
        db = get_db()
        db[COLLECTION].insert_one({**trade, "_id": trade["trade_id"]})
        logger.debug(f"[trade-store] inserted trade {trade['trade_id']}")
    except Exception as e:
        logger.error(f"[trade-store] insert failed: {e}")


def get_trades(symbol: str, limit: int = 100) -> list[dict]:
    try:
        db = get_db()
        cursor = (
            db[COLLECTION]
            .find({"symbol": symbol}, {"_id": 0})
            .sort("timestamp", -1)
            .limit(limit)
        )
        return list(cursor)
    except Exception as e:
        logger.error(f"[trade-store] query failed: {e}")
        return []