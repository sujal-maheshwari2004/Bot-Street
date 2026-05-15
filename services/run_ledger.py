import logging
from core.kafka_client import ensure_topics
from engine.portfolio_ledger import PortfolioLedger

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()
    PortfolioLedger().start()


if __name__ == "__main__":
    main()