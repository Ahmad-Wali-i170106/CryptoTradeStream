#!/usr/bin/env python3
"""
Simple CLI consumer for Crypto VWAP Dashboard Kafka topics.

Reads from 'processed_metrics' and 'alerts' topics and displays
results to the console with periodic refresh.

Usage:
    python cli_app.py
"""

import os
import sys
import time
from collections import defaultdict
from datetime import datetime

import pandas as pd
from kafka_utils import consume_processed_metrics, create_consumer
from loguru import logger

# Configuration
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
METRICS_GROUP_ID = os.getenv("KAFKA_DASHBOARD_GROUP_ID", "cli-dashboard-consumer")
ALERTS_GROUP_ID = os.getenv("KAFKA_ALERTS_GROUP_ID", "cli-dashboard-alerts-consumer")

# Polling settings
MAX_MESSAGES_PER_POLL = 100
POLL_TIMEOUT = 1.0
REFRESH_INTERVAL = 5  # seconds between display refreshes


def create_metrics_consumer():
    """Create and subscribe to the metrics topic."""
    consumer = create_consumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=METRICS_GROUP_ID,
    )
    consumer.subscribe(["processed_metrics"])
    logger.info(
        f"Metrics consumer created for {BOOTSTRAP_SERVERS}, group: {METRICS_GROUP_ID}"
    )
    return consumer


def create_alerts_consumer():
    """Create and subscribe to the alerts topic."""
    consumer = create_consumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=ALERTS_GROUP_ID,
    )
    consumer.subscribe(["alerts"])
    logger.info(
        f"Alerts consumer created for {BOOTSTRAP_SERVERS}, group: {ALERTS_GROUP_ID}"
    )
    return consumer


def print_separator(title: str = ""):
    """Print a nice separator line."""
    width = 100
    if title:
        padding = (width - len(title) - 2) // 2
        print(
            "\n"
            + "=" * padding
            + f" {title} "
            + "=" * (width - padding - len(title) - 2)
        )
    else:
        print("\n" + "=" * width)


def print_metrics(metrics_list: list):
    """Pretty-print metrics to console."""
    if not metrics_list:
        return

    print_separator("METRICS")

    # Convert to DataFrame for nice tabular display
    df = pd.DataFrame(metrics_list)

    if "window_end" in df.columns:
        df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")
    if "vwap" in df.columns:
        df["vwap"] = pd.to_numeric(df["vwap"], errors="coerce")
    if "total_volume" in df.columns:
        df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")

    # Sort by window_end descending to show newest first
    df = df.sort_values("window_end", ascending=False)

    # Display latest snapshot per symbol
    latest_per_symbol = df.groupby("symbol").first()

    print("\nLatest metrics per symbol:")
    print(latest_per_symbol[["vwap", "total_volume", "window_end"]].to_string())
    print(f"\nTotal metrics records received: {len(df)}")


def print_alerts(alerts_list: list, seen_alerts: set):
    """Pretty-print alerts to console with highlighting for new alerts."""
    if not alerts_list:
        return

    print_separator("ALERTS")

    # Convert to DataFrame for nice tabular display
    df = pd.DataFrame(alerts_list)

    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], errors="coerce")
    if "window_end" in df.columns:
        df["window_end"] = pd.to_datetime(df["window_end"], errors="coerce")
    if "percent_change" in df.columns:
        df["percent_change"] = pd.to_numeric(df["percent_change"], errors="coerce")
    if "first_price" in df.columns:
        df["first_price"] = pd.to_numeric(df["first_price"], errors="coerce")
    if "last_price" in df.columns:
        df["last_price"] = pd.to_numeric(df["last_price"], errors="coerce")

    # Sort by window_end descending to show newest first
    df = df.sort_values("window_end", ascending=False)

    print("\nMost recent alerts (newest first):")
    print(
        df[
            ["symbol", "percent_change", "first_price", "last_price", "window_end"]
        ].to_string()
    )
    print(f"\nTotal alerts received: {len(df)}")

    # Highlight new alerts
    print("\n⚠️  NEW ALERTS:")
    new_count = 0
    for _, row in df.iterrows():
        # Build alert key for deduplication
        window_end = row.get("window_end")
        if pd.notna(window_end):
            key = f"{row.get('symbol')}_{pd.Timestamp(window_end).isoformat()}"
        else:
            key = f"{row.get('symbol')}_{window_end}"

        if key not in seen_alerts:
            seen_alerts.add(key)
            pct = row.get("percent_change", 0.0)
            symbol = row.get("symbol", "UNKNOWN")
            first = row.get("first_price", "N/A")
            last = row.get("last_price", "N/A")
            when = row.get("window_end", "N/A")
            print(f"  🔴 {symbol}: {pct:.2f}% change ({first} -> {last}) at {when}")
            new_count += 1

    if new_count == 0:
        print("  (no new alerts since last refresh)")


def main():
    """Main CLI loop."""
    print("\n" + "=" * 100)
    print("Crypto VWAP Dashboard CLI Consumer".center(100))
    print("=" * 100)
    print(f"Kafka Bootstrap Servers: {BOOTSTRAP_SERVERS}")
    print(f"Metrics Consumer Group: {METRICS_GROUP_ID}")
    print(f"Alerts Consumer Group: {ALERTS_GROUP_ID}")
    print(f"Poll Timeout: {POLL_TIMEOUT}s, Max Messages: {MAX_MESSAGES_PER_POLL}")
    print(f"Display Refresh Interval: {REFRESH_INTERVAL}s")
    print("\nPress Ctrl+C to exit.\n")

    # Initialize consumers
    metrics_consumer = create_metrics_consumer()
    alerts_consumer = create_alerts_consumer()

    # Accumulated data and tracking
    all_metrics = {}
    all_alerts = {}
    seen_alerts = set()

    try:
        iteration = 0
        while True:
            iteration += 1
            start_time = time.time()

            # Poll for new metrics
            try:
                new_metrics = consume_processed_metrics(
                    metrics_consumer,
                    max_messages=MAX_MESSAGES_PER_POLL,
                    poll_timeout=POLL_TIMEOUT,
                )
                if new_metrics:
                    logger.info(f"Received {len(new_metrics)} new metrics")
                    for m in new_metrics:
                        # Use (symbol, window_end) as unique key
                        key = (m.get("symbol"), m.get("window_end"))
                        all_metrics[key] = m
            except Exception as e:
                logger.error(f"Error polling metrics: {e}", exc_info=True)

            # Poll for new alerts
            try:
                new_alerts = consume_processed_metrics(
                    alerts_consumer,
                    max_messages=MAX_MESSAGES_PER_POLL,
                    poll_timeout=POLL_TIMEOUT,
                )
                if new_alerts:
                    logger.info(f"Received {len(new_alerts)} new alerts")
                    for a in new_alerts:
                        # Use (symbol, window_end) as unique key
                        key = (a.get("symbol"), a.get("window_end"))
                        all_alerts[key] = a
            except Exception as e:
                logger.error(f"Error polling alerts: {e}", exc_info=True)

            # Display current state
            print_metrics(list(all_metrics.values()))
            print_alerts(list(all_alerts.values()), seen_alerts)

            print_separator()
            elapsed = time.time() - start_time
            remaining = max(0, REFRESH_INTERVAL - elapsed)
            print(
                f"Iteration {iteration} completed in {elapsed:.2f}s. Refreshing in {remaining:.1f}s..."
            )

            # Sleep until next refresh
            if remaining > 0:
                time.sleep(remaining)

    except KeyboardInterrupt:
        print("\n\nShutting down...")
    finally:
        metrics_consumer.close()
        alerts_consumer.close()
        logger.info("Consumers closed.")


if __name__ == "__main__":
    main()
