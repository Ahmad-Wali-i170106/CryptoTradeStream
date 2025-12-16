import json
import os
import time

from confluent_kafka import Producer
from loguru import logger
from websockets.sync.client import connect

# --- Message Processing Function ---


def process_incoming_trade_message(message: str, kafka_producer: Producer):
    """
    Processes an incoming trade message from the WebSocket and publishes it to Kafka.
    """

    def delivery_report(err, msg):
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(
                f"Message delivered: {msg.topic()} [{msg.partition()}] "
                f"Offset: {msg.offset()}"
            )

    try:
        data = json.loads(message)

        # Extract the necessary fields for your pipeline
        trade_id = data.get("t")
        symbol = data.get("s")
        price = data.get("p")
        quantity = data.get("q")

        # Simple logging to show it's working
        logger.info(
            f"Trade ID: {trade_id} | Symbol: {symbol} | Price: {price} | Quantity: {quantity}"
        )

        # Kafka producer expects key and value to be bytes
        kafka_producer.produce(
            "raw_trades",
            key=symbol.encode("utf-8"),
            value=message.encode("utf-8"),
            callback=delivery_report,
        )
        # Flush after every message can be inefficient for high-throughput.
        # Consider buffering messages and flushing periodically or relying on
        # the producer's internal buffer management for better performance
        # in a production environment. For this example, we keep it for immediate feedback.
        kafka_producer.flush()

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON message: {message}")
    except Exception as e:
        logger.exception(f"Error processing message: {e}")


def process_incoming_trade_message2(message: str):
    """
    Processes an incoming trade message from the WebSocket and publishes it to Kafka.
    """

    def delivery_report(err, msg):
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(
                f"Message delivered: {msg.topic()} [{msg.partition()}] "
                f"Offset: {msg.offset()}"
            )

    try:
        data = json.loads(message)

        # Extract the necessary fields for your pipeline
        trade_id = data.get("t")
        symbol = data.get("s")
        price = data.get("p")
        quantity = data.get("q")

        # Simple logging to show it's working
        logger.info(
            f"Trade ID: {trade_id} | Symbol: {symbol} | Price: {price} | Quantity: {quantity}"
        )

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON message: {message}")
    except Exception as e:
        logger.exception(f"Error processing message: {e}")


# --- Main Execution ---

if __name__ == "__main__":
    # 1. Configuration
    # BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade/solusdt@trade/xrpusdt@trade"

    logger.info(f"Connecting to Binance at: {BINANCE_WEBSOCKET_URL}")
    conf = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    producer = Producer(conf)

    # The original implementation had a "run_forever" concept.
    # We will simulate this by looping and attempting to reconnect on disconnection/error.
    while True:
        try:
            logger.info(f"Attempting to connect to {BINANCE_WEBSOCKET_URL}...")
            # Use websockets.sync.client.connect as a context manager for reliable connection handling
            with connect(BINANCE_WEBSOCKET_URL) as websocket:
                logger.info("### Connection Opened ### Listening for BTUSDT trades...")
                # The "on_open" logic from the old callback is now handled here directly
                # after the successful establishment of the WebSocket connection.

                # Iterate over incoming messages from the WebSocket
                for message in websocket:
                    print(message)
                    process_incoming_trade_message2(str(message))
                    # process_incoming_trade_message(message, kafka_producer=producer)

        except Exception as e:
            # This block catches any exceptions that occur during connection
            # or message reception, effectively replacing the old on_error callback
            # and providing a robust reconnection mechanism.
            logger.error(
                f"### Connection ERROR ###: {e}. Retrying connection in 5 seconds..."
            )
            # In a production system, consider implementing an exponential backoff strategy
            # to avoid aggressively hammering the server during prolonged outages.
            time.sleep(5)
        finally:
            # This block is executed when the `with connect` block exits,
            # either normally or due to an exception, simulating the on_close callback.
            logger.info("### WebSocket Connection Closed ###")
