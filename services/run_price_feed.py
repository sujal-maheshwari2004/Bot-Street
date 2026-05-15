import logging
from core.kafka_client import ensure_topics
from market.price_feed import PriceFeed

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()
    PriceFeed().start()


if __name__ == "__main__":
    main()