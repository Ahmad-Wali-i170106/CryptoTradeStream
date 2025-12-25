import json
import os
import sys
from typing import List, Mapping

from confluent_kafka import Consumer, KafkaError
from loguru import logger

# Configure loguru to output to both stdout and a file
# Remove default handler and add custom ones
logger.remove()

# Add stdout handler with no buffering for real-time console output
logger.add(
    sys.stdout,
    format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="DEBUG",
    colorize=True,
    backtrace=True,
    diagnose=True,
)

# Add file handler for persistent logging
# logger.add(
#     os.getenv("LOGS_FOLDER", "/var/log/crypto-dashboard.log"),
#     format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
#     level="DEBUG",
#     rotation="100 MB",
#     retention="7 days",
#     backtrace=True,
#     diagnose=True,
# )


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

    logger.info(f"Creating Kafka consumer with group_id={group_id}")
    return Consumer(config)


def consume_processed_metrics(
    consumer: Consumer,
    max_messages: int = 200,
    poll_timeout: float = 1.0,
) -> List[dict]:
    """
    Poll the specified topics and return parsed messages.

    Returns a list of messages, each as a dict with the message payload
    and the Kafka key (if present) under the 'key' field.
    """
    messages: List[dict] = []
    messages_polled = 0

    for attempt in range(max_messages):
        msg = consumer.poll(timeout=poll_timeout)
        messages_polled += 1

        if msg is None:
            logger.debug(f"No message available (attempt {attempt + 1}/{max_messages})")
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Reached end of partition")
                continue
            logger.warning(f"Kafka error: {msg.error()}")
            continue

        try:
            value = msg.value().decode("utf-8")
            logger.debug(f"Received message: {value}")
            payload = json.loads(value)
            payload["key"] = msg.key().decode("utf-8") if msg.key() else None
            messages.append(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning(f"Failed to decode message: {exc}")
            continue

    if messages:
        logger.info(
            f"Consumed {len(messages)} messages from Kafka (polled {messages_polled} times)"
        )
    else:
        logger.debug(f"No messages consumed (polled {messages_polled} times)")

    return messages
