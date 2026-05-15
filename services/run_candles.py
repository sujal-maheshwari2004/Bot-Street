import logging
import threading
from core.kafka_client import ensure_topics
from engine.candle_aggregator import CandleAggregator
from engine.trade_logger import TradeLogger

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()

    trade_logger = TradeLogger()
    tl_thread = threading.Thread(target=trade_logger.start, name="trade-logger", daemon=True)
    tl_thread.start()

    CandleAggregator().start()  # blocks


if __name__ == "__main__":
    main()