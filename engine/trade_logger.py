"""
Trade Logger — Appends every trade to a JSONL file

Simple but valuable:
  - Free audit trail of all market activity
  - Can be replayed into Kafka for backtesting
  - Useful for debugging sentiment + indicator logic

JSONL format: one JSON object per line
  {"trade_id": "...", "symbol": "PEAR", "price": 151.0, ...}
  {"trade_id": "...", "symbol": "TSLA", "price": 90.2,  ...}
"""

import json
import logging
from pathlib import Path
from time import time

from config import TOPIC_TRADE_EXECUTED
from core.kafka_client import MarketConsumer

logger = logging.getLogger(__name__)

LOG_PATH = Path("data/trades.jsonl")


class TradeLogger:
    """
    Consumes trade-executed and appends to data/trades.jsonl.
    Lightweight — no producer needed.
    """

    def __init__(self):
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

        self._consumer = MarketConsumer(
            group_id="trade-logger",
            topics=[TOPIC_TRADE_EXECUTED],
            offset="latest",
        )
        self._running = False
        self._count   = 0

        logger.info(f"[trade-logger] logging trades to {LOG_PATH}")

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
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            while self._running:
                msg = self._consumer.poll_once(timeout=0.5)
                if msg is None:
                    continue

                f.write(json.dumps(msg) + "\n")
                f.flush()   # ensure write on every trade
                self._count += 1

                if self._count % 100 == 0:
                    logger.info(f"[trade-logger] {self._count} trades logged")