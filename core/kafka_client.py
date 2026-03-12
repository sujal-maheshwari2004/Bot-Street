import json
import logging
from dataclasses import asdict
from typing import Callable

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP_SERVERS, ALL_TOPICS, SYMBOL_LIST, TOPIC_MARKET_ORDERS

logger = logging.getLogger(__name__)

# ── Topic bootstrap ───────────────────────────────────────────────────────────
def ensure_topics():
    """
    Creates all required topics if they don't already exist.
    Partitions market-orders by symbol count so each symbol
    can be consumed independently by the matching engine.
    Call this once at startup before any producer/consumer starts.
    """
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    try:
        existing = admin.list_topics(timeout=10).topics.keys()
    except KafkaException as e:
        logger.error(f"[kafka] cannot reach broker: {e}")
        raise

    to_create = []
    for topic in ALL_TOPICS:
        if topic not in existing:
            # market-orders gets one partition per symbol
            # so matching engine can consume per-symbol independently
            partitions = len(SYMBOL_LIST) if topic == TOPIC_MARKET_ORDERS else 1
            to_create.append(
                NewTopic(
                    topic,
                    num_partitions=partitions,
                    replication_factor=1,
                )
            )

    if not to_create:
        logger.info("[kafka] all topics already exist")
        return

    futures = admin.create_topics(to_create)
    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"[kafka] created topic: {topic}")
        except Exception as e:
            logger.warning(f"[kafka] topic '{topic}' skipped: {e}")


# ── Serialisation helpers ─────────────────────────────────────────────────────
def _serialize(message) -> bytes:
    """Dataclass → JSON bytes."""
    return json.dumps(asdict(message)).encode("utf-8")


def _deserialize(raw: bytes) -> dict:
    """JSON bytes → dict."""
    return json.loads(raw.decode("utf-8"))


# ── Symbol → partition mapping ────────────────────────────────────────────────
def symbol_to_partition(symbol: str) -> int:
    """
    Maps a symbol to a consistent partition index.
    Ensures all orders for PEAR always land on the same partition
    so the matching engine never sees out-of-order cross-symbol data.
    """
    return SYMBOL_LIST.index(symbol) if symbol in SYMBOL_LIST else 0


# ── Producer ──────────────────────────────────────────────────────────────────
class MarketProducer:
    """
    Thin wrapper around confluent Producer.
    All participants (user, bots, engines) use this to publish messages.

    Usage:
        p = MarketProducer("bot-momentum")
        p.send(TOPIC_MARKET_ORDERS, order, partition_key=order.symbol)
        p.flush()
    """

    def __init__(self, client_id: str):
        self._client_id = client_id
        self._producer = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "client.id": client_id,
            "acks": "all",                  # wait for broker ack
            "retries": 3,
            "retry.backoff.ms": 200,
        })
        logger.info(f"[producer:{client_id}] ready")

    def send(
        self,
        topic: str,
        message,
        partition_key: str | None = None,
        partition: int | None = None,
    ):
        """
        Serialize and produce a message.
        - partition_key: used to hash onto a partition (e.g. symbol)
        - partition: explicit partition override (e.g. for market-orders)
        """
        payload = _serialize(message)
        key = partition_key.encode("utf-8") if partition_key else None

        kwargs = dict(
            topic=topic,
            value=payload,
            key=key,
            on_delivery=self._on_delivery,
        )
        if partition is not None:
            kwargs["partition"] = partition

        self._producer.produce(**kwargs)
        self._producer.poll(0)   # trigger callbacks without blocking

    def send_order(self, topic: str, order):
        """
        Convenience method for orders — automatically routes to
        the correct partition based on symbol.
        """
        self.send(
            topic,
            order,
            partition=symbol_to_partition(order.symbol),
        )

    def flush(self, timeout: float = 5.0):
        """Block until all queued messages are delivered."""
        self._producer.flush(timeout)

    def _on_delivery(self, err, msg):
        if err:
            logger.error(
                f"[producer:{self._client_id}] delivery failed "
                f"topic={msg.topic()} err={err}"
            )


# ── Consumer ──────────────────────────────────────────────────────────────────
class MarketConsumer:
    """
    Thin wrapper around confluent Consumer.
    Each engine/service creates its own consumer with a unique group_id.

    Usage:
        c = MarketConsumer("matching-engine-PEAR", [TOPIC_MARKET_ORDERS])
        for msg in c.stream():
            process(msg)
    """

    def __init__(
        self,
        group_id: str,
        topics: list[str],
        offset: str = "latest",
        partitions: list[int] | None = None,
    ):
        self._group_id = group_id
        self._consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": group_id,
            "auto.offset.reset": offset,
            "enable.auto.commit": True,
            "session.timeout.ms": 10_000,
            "heartbeat.interval.ms": 3_000,
        })

        if partitions:
            # Assign specific partitions directly (used by matching engine
            # to consume only its symbol's partition)
            from confluent_kafka import TopicPartition
            assignments = [
                TopicPartition(topic, p)
                for topic in topics
                for p in partitions
            ]
            self._consumer.assign(assignments)
            logger.info(
                f"[consumer:{group_id}] assigned "
                f"topics={topics} partitions={partitions}"
            )
        else:
            self._consumer.subscribe(topics)
            logger.info(
                f"[consumer:{group_id}] subscribed to topics={topics}"
            )

    def poll_once(self, timeout: float = 1.0) -> dict | None:
        """
        Poll for a single message. Returns deserialized dict or None.
        Non-blocking — returns None if no message within timeout.
        """
        msg = self._consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None   # end of partition, not an error
            logger.error(f"[consumer:{self._group_id}] error: {msg.error()}")
            return None
        return _deserialize(msg.value())

    def stream(self, timeout: float = 1.0):
        """
        Generator — yields deserialized message dicts indefinitely.
        Use in a while True loop:
            for msg in consumer.stream():
                ...
        """
        try:
            while True:
                msg = self.poll_once(timeout)
                if msg is not None:
                    yield msg
        except KeyboardInterrupt:
            logger.info(f"[consumer:{self._group_id}] shutting down")
        finally:
            self.close()

    def close(self):
        self._consumer.close()
        logger.info(f"[consumer:{self._group_id}] closed")