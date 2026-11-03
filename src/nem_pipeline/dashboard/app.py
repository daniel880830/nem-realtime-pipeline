"""Streamlit UI. Run with ``nem-pipeline dashboard``."""

from __future__ import annotations

import paho.mqtt.client as mqtt
import pandas as pd
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from nem_pipeline import warehouse
from nem_pipeline.config import Settings
from nem_pipeline.dashboard.logic import FUEL_COLORS, LiveState, filter_facilities

st.set_page_config(page_title="NEM Real-Time Dashboard", layout="wide")
settings = Settings()
topics = settings.pipeline.publish


@st.cache_data
def load_facilities() -> pd.DataFrame:
    """Static facility table with the coordinates chosen by the last pipeline run."""
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        return warehouse.facilities_for_display(conn)


@st.cache_resource
def start_subscriber(host: str, port: int) -> LiveState:
    """Start one MQTT subscriber per server process and return its shared state."""
    state = LiveState()

    def on_connect(client, _userdata, _flags, reason_code, _properties=None):
        if reason_code == 0:
            client.subscribe([(topics.facility_topic, 0), (topics.market_topic, 0)])

    def on_message(_client, _userdata, msg):
        state.ingest(
            msg.topic,
            msg.payload,
            facility_topic=topics.facility_topic,
            market_topic=topics.market_topic,
        )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="nem-dashboard")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect_async(host, port, keepalive=60)
    client.loop_start()
    return state


st.title("NEM Real-Time Dashboard")
try:
    facilities = load_facilities()
except FileNotFoundError as exc:
    st.error(f"{exc}")
    st.stop()

state = start_subscriber(settings.mqtt_host, settings.mqtt_port)
st_autorefresh(interval=1000, key="refresh")

st.sidebar.header("Filters")
all_states = sorted(facilities["state"].dropna().unique())
states = st.sidebar.multiselect("State", all_states, default=all_states)
fuels = st.sidebar.multiselect("Fuel", list(FUEL_COLORS), default=list(FUEL_COLORS))
st.sidebar.caption(f"Broker {settings.mqtt_host}:{settings.mqtt_port}")
st.sidebar.caption(f"Rejected messages: {state.rejected}")

view = filter_facilities(state.facility_frame(facilities), states, fuels)
left, right = st.columns([3, 2])

with left:
    st.subheader(f"Facilities — {state.facility_timestamp or 'waiting for data'}")
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=view.fillna({"power_mw": 0}),
        get_position=["longitude", "latitude"],
        get_fill_color="color",
        get_radius=6000,
        radius_min_pixels=3,
        radius_max_pixels=9,
        pickable=True,
    )
    st.pydeck_chart(
        pdk.Deck(
            layers=[layer],
            initial_view_state=pdk.ViewState(latitude=-30.0, longitude=145.0, zoom=4),
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
            tooltip={
                "html": "<b>{facility_name}</b><br/>{fuel_type}<br/>Power: {power_mw} MW<br/>"
                "Emissions: {emissions_t} t",
            },
        ),
        width="stretch",
    )
    st.caption(f"{len(view)} of {len(facilities)} facilities shown")

with right:
    st.subheader(f"Market — {state.market_timestamp or 'waiting for data'}")
    market = state.market_frame()
    if market.empty:
        st.info("Waiting for market messages ...")
    else:
        st.dataframe(
            market.rename(
                columns={
                    "region": "Region",
                    "price_aud_mwh": "Price ($/MWh)",
                    "demand_mw": "Demand (MW)",
                }
            ),
            hide_index=True,
            width="stretch",
        )
        st.metric("Average price ($/MWh)", f"{market['price_aud_mwh'].mean():.2f}")
        st.metric("Total demand (MW)", f"{market['demand_mw'].sum():,.0f}")
    live = view.dropna(subset=["power_mw"])
    if not live.empty:
        st.subheader("Output by fuel (MW)")
        st.bar_chart(live.groupby("fuel_group")["power_mw"].sum().sort_values())
