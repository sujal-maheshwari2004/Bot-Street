"""
Trade Logger — Writes every trade to MongoDB Atlas

Consumes trade-executed, persists each trade to the `trades` collection.
Interface identical to the old file-based version — drop-in replacement.

Benefits over trades.jsonl:
  - Survives pod restarts
  - Queryable by symbol, time range, participant
  - Portfolio ledger can replay from Atlas on startup
"""

import logging
from config import TOPIC_TRADE_EXECUTED
from core.kafka_client import MarketConsumer
from db.trade_store import insert_trade

logger = logging.getLogger(__name__)


class TradeLogger:
    """
    Consumes trade-executed and writes to MongoDB trades collection.
    Lightweight — no producer needed.
    """

    def __init__(self):
        self._consumer = MarketConsumer(
            group_id="trade-logger",
            topics=[TOPIC_TRADE_EXECUTED],
            offset="latest",
        )
        self._running = False
        self._count   = 0
        logger.info("[trade-logger] initialised — writing to MongoDB")

    def start(self):
        self._running = True
        logger.info("[trade-logger] started")
        try:
            self._run_loop()
        finally:
            self._running = False
            logger.info(f"[trade-logger] stopped — logged {self._count} trades")

    def stop(self):
        self._running = False

    def _run_loop(self):
        while self._running:
            msg = self._consumer.poll_once(timeout=0.5)
            if msg is None:
                continue

            insert_trade(msg)
            self._count += 1

            if self._count % 100 == 0:
                logger.info(f"[trade-logger] {self._count} trades written to Atlas")