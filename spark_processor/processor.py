from confluent_kafka import Consumer, Producer


# Function to send messages to Kafka using confluent_kafka
def send_message_confluent(value: str, topic: str = 'processed_metrics', bootstrap_servers="localhost:9092"):
    conf = {"bootstrap.servers": bootstrap_servers}
    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered: {msg.topic()} [{msg.partition()}]")

    producer.produce(
        topic, key=None, value=value.encode("utf-8"), callback=delivery_report
    )
    # Wait for all messages to be delivered
    producer.flush()

# Function to consume messages from kafka using confluent-kafka
def consume_messages_confluent(
    topic: str = 'raw_trades',
    bootstrap_servers="localhost:9092",
    group_id="my_consumer_group",
    auto_offset_reset="earliest",
):
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # if end-of-partition: ignore, else print error
                print(f"Consumer error: {msg.error()}")
                continue

            print(
                f"Received: topic={msg.topic()}, partition={msg.partition()}, "
                f"offset={msg.offset()}, value={msg.value().decode('utf-8')}"
            )
            # TODO: Place my PySpark logic

    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    finally:
        consumer.close()
