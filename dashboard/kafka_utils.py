import json
import logging
from typing import List, Mapping, Sequence

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


def create_consumer(
    bootstrap_servers: str,
    group_id: str = "dashboard-consumer",
    auto_offset_reset: str = "latest",
    extra_config: Mapping[str, str] | None = None,
) -> Consumer:
    """
    Build a Kafka consumer configured for the dashboard.
    """
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": True,
    }
    if extra_config:
        config.update(extra_config)
    return Consumer(config)


def _ensure_subscription(consumer: Consumer, topics: Sequence[str]) -> None:
    if not consumer.subscription():
        consumer.subscribe(list(topics))


def consume_processed_metrics(
    consumer: Consumer,
    # topics: Sequence[str] = ("processed_metrics",),
    max_messages: int = 200,
    poll_timeout: float = 1.0,
) -> List[dict]:
    """
    Poll the specified topics and return parsed messages.

    Returns a list of messages, each as a dict with the message payload
    and the Kafka key (if present) under the 'key' field.
    """
    # _ensure_subscription(consumer, topics)
    # consumer.subscribe(list(topics))

    messages: List[dict] = []
    for _ in range(max_messages):
        msg = consumer.poll(timeout=poll_timeout)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.warning("Kafka error: %s", msg.error())
            continue

        try:
            value = msg.value().decode("utf-8")
            payload = json.loads(value)
            payload["key"] = msg.key().decode("utf-8") if msg.key() else None
            messages.append(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("Failed to decode message: %s", exc)
            continue

    return messages
