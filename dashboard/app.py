import os
import time

import pandas as pd
import streamlit as st
from kafka_utils import consume_processed_metrics, create_consumer
from loguru import logger

# Configure logging for Streamlit app
logger.remove()
logger.add(
    lambda msg: print(msg, end=""),
    format="{time:HH:mm:ss} | {level: <8} | Dashboard - {message}\n",
    level="INFO",
    colorize=True,
)

logger.info("=" * 80)
logger.info("Crypto VWAP Dashboard Starting")
logger.info("=" * 80)

st.set_page_config(
    page_title="Crypto VWAP Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("Crypto VWAP Dashboard")
st.caption("View VWAP, volume, and alerts from Kafka topics in real-time.")

# Configuration
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
metrics_group_id = os.getenv("KAFKA_DASHBOARD_GROUP_ID", "dashboard-metrics-consumer")
alerts_group_id = os.getenv("KAFKA_ALERTS_GROUP_ID", "dashboard-alerts-consumer")

logger.info(f"Kafka bootstrap servers: {bootstrap_servers}")
logger.info(f"Metrics consumer group: {metrics_group_id}")
logger.info(f"Alerts consumer group: {alerts_group_id}")


# Cache Kafka consumer creation
@st.cache_resource
def get_metrics_consumer():
    try:
        consumer = create_consumer(
            bootstrap_servers=bootstrap_servers,
            group_id=metrics_group_id,
        )
        consumer.subscribe(["processed_metrics"])
        logger.info("✓ Metrics consumer connected and subscribed")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create metrics consumer: {e}")
        return None


@st.cache_resource
def get_alerts_consumer():
    try:
        consumer = create_consumer(
            bootstrap_servers=bootstrap_servers,
            group_id=alerts_group_id,
        )
        consumer.subscribe(["alerts"])
        logger.info("✓ Alerts consumer connected and subscribed")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create alerts consumer: {e}")
        return None


# Poll data from Kafka
def poll_metrics(consumer, max_messages: int = 50, timeout: float = 0.5):
    if not consumer:
        return []
    try:
        return consume_processed_metrics(
            consumer, max_messages=max_messages, poll_timeout=timeout
        )
    except Exception as e:
        logger.error(f"Error polling metrics: {e}")
        return []


def poll_alerts(consumer, max_messages: int = 50, timeout: float = 0.5):
    if not consumer:
        return []
    try:
        return consume_processed_metrics(
            consumer, max_messages=max_messages, poll_timeout=timeout
        )
    except Exception as e:
        logger.error(f"Error polling alerts: {e}")
        return []


# Initialize session state
if "metrics_df" not in st.session_state:
    st.session_state.metrics_df = pd.DataFrame(
        columns=["window_end", "symbol", "vwap", "total_volume"]
    )

if "alerts_df" not in st.session_state:
    st.session_state.alerts_df = pd.DataFrame(
        columns=[
            "window_start",
            "window_end",
            "symbol",
            "first_price",
            "last_price",
            "percent_change",
        ]
    )

# Real-time dashboard
placeholder = st.empty()

while True:
    # Poll metrics
    metrics_consumer = get_metrics_consumer()
    new_metrics = poll_metrics(metrics_consumer)
    if new_metrics:
        metrics_df = pd.DataFrame(new_metrics)
        metrics_df["window_end"] = pd.to_datetime(
            metrics_df["window_end"], errors="coerce"
        )
        metrics_df["vwap"] = pd.to_numeric(metrics_df["vwap"], errors="coerce")
        metrics_df["total_volume"] = pd.to_numeric(
            metrics_df["total_volume"], errors="coerce"
        )
        st.session_state.metrics_df = pd.concat(
            [st.session_state.metrics_df, metrics_df], ignore_index=True
        ).drop_duplicates(subset=["window_end", "symbol"], keep="last")

    # Poll alerts
    alerts_consumer = get_alerts_consumer()
    new_alerts = poll_alerts(alerts_consumer)
    if new_alerts:
        alerts_df = pd.DataFrame(new_alerts)
        alerts_df["window_start"] = pd.to_datetime(
            alerts_df["window_start"], errors="coerce"
        )
        alerts_df["window_end"] = pd.to_datetime(
            alerts_df["window_end"], errors="coerce"
        )
        alerts_df["percent_change"] = pd.to_numeric(
            alerts_df["percent_change"], errors="coerce"
        )
        alerts_df["first_price"] = pd.to_numeric(
            alerts_df["first_price"], errors="coerce"
        )
        alerts_df["last_price"] = pd.to_numeric(
            alerts_df["last_price"], errors="coerce"
        )
        st.session_state.alerts_df = pd.concat(
            [st.session_state.alerts_df, alerts_df], ignore_index=True
        ).drop_duplicates(subset=["window_end", "symbol"], keep="last")

    # Display dashboard content
    with placeholder.container():
        metrics_df = st.session_state.metrics_df.dropna(subset=["window_end"])
        alerts_df = st.session_state.alerts_df.dropna(subset=["window_end"])

        if metrics_df.empty and alerts_df.empty:
            st.info(
                "⏳ Waiting for data... Ensure Kafka is running and data is being produced."
            )
        else:
            # Display metrics data
            if not metrics_df.empty:
                metrics_df = metrics_df.sort_values("window_end")
                latest_metrics = metrics_df.groupby("symbol").tail(1)

                st.subheader("Latest VWAP")
                st.dataframe(
                    latest_metrics[
                        ["symbol", "vwap", "total_volume", "window_end"]
                    ].sort_values("symbol"),
                    use_container_width=True,
                    hide_index=True,
                )

                st.subheader("VWAP Over Time")
                vwap_pivot = metrics_df.pivot(
                    index="window_end", columns="symbol", values="vwap"
                )
                if not vwap_pivot.empty:
                    st.line_chart(vwap_pivot)

                st.subheader("Volume Over Time")
                vol_pivot = metrics_df.pivot(
                    index="window_end", columns="symbol", values="total_volume"
                )
                if not vol_pivot.empty:
                    st.area_chart(vol_pivot)

            # Display alerts data
            if not alerts_df.empty:
                st.subheader("Recent Alerts")
                st.dataframe(
                    alerts_df[
                        [
                            "window_end",
                            "symbol",
                            "percent_change",
                            "first_price",
                            "last_price",
                        ]
                    ].sort_values("window_end", ascending=False),
                    use_container_width=True,
                    hide_index=True,
                )

    time.sleep(5)  # Update every 5 seconds
