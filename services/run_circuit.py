import logging
from core.kafka_client import ensure_topics
from engine.circuit_breaker import CircuitBreaker

logging.basicConfig(level=logging.INFO)


def main():
    ensure_topics()
    CircuitBreaker().start()


if __name__ == "__main__":
    main()