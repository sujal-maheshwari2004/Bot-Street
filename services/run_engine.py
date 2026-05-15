import logging
from core.kafka_client import ensure_topics
from engine.matching_engine import run_all_engines

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()
    _, threads = run_all_engines()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()