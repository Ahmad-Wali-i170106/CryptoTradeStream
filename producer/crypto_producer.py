import json
import os
from functools import partial

import rel
import websocket
from confluent_kafka import Producer
from loguru import logger

# 1. Configuration
# Real-time trades for Bitcoin/USDT
# BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade/solusdt@trade/xrpusdt@trade"

# --- WebSocket Callbacks ---


def on_message(ws, message, producer: Producer):
    """
    Called every time a new trade message is received.
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

        # --- PLACE KAFKA PRODUCER CODE HERE ---
        producer.produce(
            "raw_trades", key=symbol, value=message, callback=delivery_report
        )
        producer.flush()

    except Exception as e:
        print(f"Error processing message: {e}")


def on_error(ws, error):
    print(f"### ERROR ###: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"### Connection Closed ### Code: {close_status_code}, Message: {close_msg}")


def on_open(ws):
    """
    Called when the connection is successfully established.
    """
    print("### Connection Opened ### Subscribing to BTUSDT trades...")
    # For Binance, the URL already subscribes, but for other APIs
    # you might send a JSON subscription message here.


# --- Main Execution ---

if __name__ == "__main__":
    print(f"Connecting to Binance at: {BINANCE_WEBSOCKET_URL}")

    # Enable debugging to see connection details
    # websocket.enableTrace(True)
    kafka_producer = Producer(
        {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    )
    # Create the WebSocket application
    ws = websocket.WebSocketApp(
        BINANCE_WEBSOCKET_URL,
        on_open=on_open,
        on_message=partial(on_message, producer=kafka_producer),
        on_error=on_error,
        on_close=on_close,
    )

    # Run forever. reconnect=True is useful for production, but can be omitted here.
    ws.run_forever(
        dispatcher=rel, reconnect=5
    )  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
