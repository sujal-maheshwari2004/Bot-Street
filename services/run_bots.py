import logging
import threading
from core.kafka_client import ensure_topics
from participants.market_maker import MarketMakerBot
from participants.momentum_bot import MomentumBot
from participants.random_bot import RandomBot
from participants.mean_reversion_bot import MeanReversionBot

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()

    bots = [
        MarketMakerBot(),
        MomentumBot(),
        RandomBot(),
        MeanReversionBot(),
    ]

    threads = []
    for bot in bots:
        t = threading.Thread(target=bot.start, name=bot.client_id, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()