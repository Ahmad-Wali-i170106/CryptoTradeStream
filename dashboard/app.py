import os
from typing import List

import pandas as pd
import streamlit as st
from kafka_utils import consume_processed_metrics, create_consumer

st.set_page_config(page_title="Crypto VWAP Dashboard", layout="wide")
st.title("Crypto VWAP Dashboard")
st.caption(
    "Live view of VWAP and volume per symbol from processed_metrics Kafka topic."
)

# Configuration
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
group_id = os.getenv("KAFKA_DASHBOARD_GROUP_ID", "dashboard-consumer")


@st.cache_resource(show_spinner=False)
def get_consumer():
    cons = create_consumer(bootstrap_servers=bootstrap_servers, group_id=group_id)
    # Subscribe to processed_metrics topic once for the metrics consumer
    cons.subsribe(
        [
            "processed_metrics",
        ]
    )
    return cons


def poll_metrics(max_messages: int, timeout: float) -> List[dict]:
    consumer = get_consumer()
    return consume_processed_metrics(
        consumer, max_messages=max_messages, poll_timeout=timeout
    )


@st.cache_resource(show_spinner=False)
def get_alerts_consumer():
    cons = create_consumer(
        bootstrap_servers=bootstrap_servers,
        group_id=os.getenv("KAFKA_ALERTS_GROUP_ID", "dashboard-alerts-consumer"),
    )
    # subscribe to alerts topic only once for the alerts consumer
    cons.subscribe(
        [
            "alerts",
        ]
    )
    return cons


def poll_alerts(max_messages: int, timeout: float) -> List[dict]:
    consumer = get_alerts_consumer()
    return consume_processed_metrics(
        consumer, max_messages=max_messages, poll_timeout=timeout
    )


with st.sidebar:
    st.header("Polling Controls")
    max_messages = st.slider("Messages per refresh", 10, 500, 200, step=10)
    poll_timeout = st.slider("Poll timeout (s)", 0.1, 5.0, 1.0, step=0.1)
    st.write(f"Bootstrap servers: `{bootstrap_servers}`")
    st.write(f"Consumer group: `{group_id}`")

    if st.button("Clear alerts"):
        # Reset stored alerts and the seen-alerts set
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
        st.session_state.seen_alerts = set()
        st.success("Alerts cleared")

    if st.button("Clear metrics data"):
        st.session_state.metrics_df = pd.DataFrame(
            columns=["window_end", "symbol", "vwap", "total_volume"]
        )
        st.success("Metrics data cleared")

# Session state to keep accumulated data across reruns
if "metrics_df" not in st.session_state:
    st.session_state.metrics_df = pd.DataFrame(
        columns=["window_end", "symbol", "vwap", "total_volume"]
    )

# Initialize alerts storage in session state
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
if "seen_alerts" not in st.session_state:
    st.session_state.seen_alerts = set()

new_records = poll_metrics(max_messages=max_messages, timeout=poll_timeout)

# Also poll for alerts on each refresh
new_alert_records = poll_alerts(max_messages=max_messages, timeout=poll_timeout)

if new_records:
    new_df = pd.DataFrame(new_records)
    # Parse and normalize types
    if "window_end" in new_df:
        new_df["window_end"] = pd.to_datetime(new_df["window_end"])
    if "vwap" in new_df:
        new_df["vwap"] = pd.to_numeric(new_df["vwap"], errors="coerce")
    if "total_volume" in new_df:
        new_df["total_volume"] = pd.to_numeric(new_df["total_volume"], errors="coerce")

    st.session_state.metrics_df = pd.concat(
        [st.session_state.metrics_df, new_df], ignore_index=True
    ).drop_duplicates(subset=["window_end", "symbol"])

# Process any new alerts
if new_alert_records:
    new_alerts_df = pd.DataFrame(new_alert_records)
    # Normalize types
    if "window_start" in new_alerts_df:
        new_alerts_df["window_start"] = pd.to_datetime(
            new_alerts_df["window_start"], errors="coerce"
        )
    if "window_end" in new_alerts_df:
        new_alerts_df["window_end"] = pd.to_datetime(
            new_alerts_df["window_end"], errors="coerce"
        )
    if "percent_change" in new_alerts_df:
        new_alerts_df["percent_change"] = pd.to_numeric(
            new_alerts_df["percent_change"], errors="coerce"
        )
    if "first_price" in new_alerts_df:
        new_alerts_df["first_price"] = pd.to_numeric(
            new_alerts_df["first_price"], errors="coerce"
        )
    if "last_price" in new_alerts_df:
        new_alerts_df["last_price"] = pd.to_numeric(
            new_alerts_df["last_price"], errors="coerce"
        )

    # Accumulate and dedupe
    st.session_state.alerts_df = pd.concat(
        [st.session_state.alerts_df, new_alerts_df], ignore_index=True
    ).drop_duplicates(subset=["window_end", "symbol"])

    # Immediate UI notifications for newly-seen alerts only
    for _, row in new_alerts_df.iterrows():
        # Build a stable key for deduplication; gracefully handle missing or invalid window_end
        val = row.get("window_end")
        if val is None or pd.isna(val):
            key = f"{row.get('symbol')}_{val}"
        else:
            we = pd.to_datetime(val, errors="coerce")
            if pd.isna(we):
                key = f"{row.get('symbol')}_{val}"
            else:
                key = f"{row.get('symbol')}_{we.isoformat()}"

        if key not in st.session_state.seen_alerts:
            st.session_state.seen_alerts.add(key)
            # Normalize percent value to float (safe for None / non-numeric)
            pct = row.get("percent_change")
            try:
                pct_val = float(pct) if pct is not None else 0.0
            except Exception:
                pct_val = 0.0
            first_price = row.get("first_price")
            last_price = row.get("last_price")
            st.warning(
                f"High Volatility — {row.get('symbol')}: {pct_val:.2f}% "
                f"({first_price} -> {last_price}) ending {row.get('window_end')}"
            )

df = st.session_state.metrics_df.dropna(subset=["window_end"])

if df.empty:
    st.info("Waiting for data from Kafka topic 'processed_metrics'...")
    st.stop()

# Sort for plotting
df = df.sort_values("window_end")

# Latest snapshot per symbol
latest = df.sort_values("window_end").groupby("symbol").tail(1)

col1, col2 = st.columns(2)
with col1:
    st.subheader("Latest VWAP per symbol")
    st.dataframe(
        latest[["symbol", "vwap", "total_volume", "window_end"]],
        use_container_width=True,
    )
with col2:
    st.subheader("Total records received")
    st.metric("Rows", len(df))

# Alerts section
st.subheader("Alerts")
if not st.session_state.alerts_df.empty:
    recent_alerts = st.session_state.alerts_df.sort_values(
        "window_end", ascending=False
    ).head(200)
    st.metric("Recent alerts", len(recent_alerts))
    st.dataframe(
        recent_alerts[
            ["window_end", "symbol", "percent_change", "first_price", "last_price"]
        ],
        use_container_width=True,
    )
else:
    st.write("No recent alerts")

st.subheader("VWAP over time")
vwap_pivot = df.pivot(index="window_end", columns="symbol", values="vwap")
st.line_chart(vwap_pivot)

st.subheader("Volume over time")
vol_pivot = df.pivot(index="window_end", columns="symbol", values="total_volume")
st.area_chart(vol_pivot)

st.subheader("Raw data")
st.dataframe(df.tail(200), use_container_width=True)
