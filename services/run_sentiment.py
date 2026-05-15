import logging
from core.kafka_client import ensure_topics
from market.sentiment_engine import SentimentEngine

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()
    SentimentEngine().start()


if __name__ == "__main__":
    main()