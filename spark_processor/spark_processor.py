import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as sql_abs
from pyspark.sql.functions import col, first, from_json, last, sum, when, window
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VOLATILITY_WINDOW = os.getenv("VOLATILITY_WINDOW", "10 seconds")
VOLATILITY_THRESHOLD = float(
    os.getenv("VOLATILITY_THRESHOLD", "1.0")
)  # percent threshold

# 1. Define the Schema for the Raw Binance Trade Data
# We define the schema to tell PySpark how to read the JSON message value.
# (The 'p' is price, 'q' is quantity, 'E' is event_time)
trade_schema = StructType(
    [
        StructField("E", LongType(), True),  # Event time
        StructField("s", StringType(), True),  # Symbol (e.g., BTCUSDT)
        StructField("p", StringType(), True),  # Price (comes as string)
        StructField("q", StringType(), True),  # Quantity (comes as string)
    ]
)

# 2. Initialize Spark Session
# Note: Spark configuration is usually managed by the execution environment (e.g., docker-compose)
spark = SparkSession.builder.appName("CryptoStreamingProcessor").getOrCreate()

# Set log level to WARN to reduce console spam
spark.sparkContext.setLogLevel("WARN")

# 3. Read Stream from Kafka ('raw_trades' topic)
kafka_df = (
    spark.readStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    .option("subscribe", "raw_trades")
    .option("startingOffsets", "latest")
    .load()
)

# Log raw Kafka input
kafka_df.writeStream.format("console").outputMode("append").start()

# kafka_df.printSchema()
# 4. Transform and Select Data
# Extract the value (which is a JSON string), cast to defined schema, and convert strings to double.
stream_df = (
    kafka_df.selectExpr("CAST(value AS STRING)", "timestamp")
    .select(from_json(col("value"), trade_schema).alias("data"), col("timestamp"))
    .select(
        col("data.s").alias("symbol"),
        (col("data.E") / 1000)
        .cast(TimestampType())
        .alias("event_time"),  # Convert Unix ms to Timestamp
        col("data.p").cast(DoubleType()).alias("price"),
        col("data.q").cast(DoubleType()).alias("quantity"),
    )
    .withColumn("volume_price", col("price") * col("quantity"))
)  # Calculated field for VWAP numerator
# stream_df.head()
# Apply Streaming Aggregation (VWAP Calculation)
# VWAP (Volume-Weighted Average Price) is SUM(Price * Quantity) / SUM(Quantity)
windowed_vwap_df = (
    stream_df.withWatermark("event_time", "1 minute")
    .groupBy(
        window(
            col("event_time"), "30 seconds", "10 seconds"
        ),  # 30s window, sliding every 10s
        col("symbol"),
    )
    .agg(
        (sum(col("volume_price")) / sum(col("quantity"))).alias("vwap"),
        sum(col("quantity")).alias("total_volume"),
    )
    .select(
        col("window.end").alias("window_end"),
        col("symbol"),
        col("vwap"),
        col("total_volume"),
    )
)

# Compute volatility alerts: percent change in the time window
volatility_df = (
    stream_df.withWatermark("event_time", "1 minute")
    .groupBy(
        window(col("event_time"), VOLATILITY_WINDOW, VOLATILITY_WINDOW), col("symbol")
    )
    .agg(
        first(col("price")).alias("first_price"),
        last(col("price")).alias("last_price"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("first_price"),
        col("last_price"),
    )
    .withColumn(
        "percent_change",
        when(col("first_price") == 0, 0.0).otherwise(
            (col("last_price") - col("first_price")) / col("first_price") * 100.0
        ),
    )
    .filter(sql_abs(col("percent_change")) > VOLATILITY_THRESHOLD)
)

# Log VWAP aggregation
windowed_vwap_df.writeStream.format("console").outputMode("update").start()

# Log volatility alerts
volatility_df.writeStream.format("console").outputMode("update").start()


# 6. Write Stream to Kafka ('processed_metrics' topic)
# Convert the structured data back into a Kafka message format (JSON string value)
# checkpointing is CRITICAL for stateful operations!
vwap_query = (
    windowed_vwap_df.selectExpr(
        "CAST(symbol AS STRING) AS key",
        "to_json(struct(*)) AS value",  # Package all columns into a single JSON string
    )
    .writeStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    .option("topic", "processed_metrics")
    .option("checkpointLocation", "/tmp/spark/checkpoint/vwap")
    .start()
)

# Write alerts to Kafka 'alerts' topic (JSON payload)
alerts_query = (
    volatility_df.selectExpr(
        "CAST(symbol AS STRING) AS key", "to_json(struct(*)) AS value"
    )
    .writeStream.format("kafka")
    .option(
        "kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    )
    .option("topic", "alerts")
    .option("checkpointLocation", "/tmp/spark/checkpoint/alerts")
    .start()
)

# Wait for the termination of the query (i.e., run forever)
# alerts_query.awaitTermination()
# vwap_query.awaitTermination()
spark.streams.awaitAnyTermination()
