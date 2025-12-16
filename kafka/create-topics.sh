#!/bin/bash
# Wait for the Kafka broker to be ready
echo "Waiting for Kafka to be ready..."
# The internal host name for the Kafka container is 'kafka'
KAFKA_HOST=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
echo "Using Kafka host: $KAFKA_HOST"

# Locate kafka-topics command
KAFKA_TOPICS_CMD=$(command -v kafka-topics)
if [ -z "$KAFKA_TOPICS_CMD" ]; then
  echo "Error: kafka-topics command not found. Exiting."
  exit 1
fi

# Loop until the kafka-topics command can connect
for i in {1..20}; do
  /usr/bin/kafka-topics --bootstrap-server "$KAFKA_HOST" --list > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "Kafka is ready!"
    break
  fi
  echo "Still waiting for Kafka... sleeping for 5 seconds"
  sleep 5
done

# Check if we successfully connected
if [ $i -eq 20 ]; then
  echo "Error: Kafka did not start in time. Exiting."
  exit 1
fi

# --- Create Topics ---
echo "Creating topics..."

# Topic 1: Raw Trade Data (e.g., 3 partitions for parallelism)
/usr/bin/kafka-topics --create --topic raw_trades --bootstrap-server "$KAFKA_HOST" --partitions 5 --replication-factor 1

# Topic 2: Processed Metrics (e.g., 1 partition since it's an aggregated stream)
/usr/bin/kafka-topics --create --topic processed_metrics --bootstrap-server "$KAFKA_HOST" --partitions 1 --replication-factor 1

# Topic 3: Alerts (e.g., 1 partition since it's an aggregated stream)
/usr/bin/kafka-topics --create --topic alerts --bootstrap-server "$KAFKA_HOST" --partitions 1 --replication-factor 1

echo "Topics created successfully."
