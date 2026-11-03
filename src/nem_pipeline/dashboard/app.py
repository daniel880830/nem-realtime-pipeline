"""Streamlit UI. Run with ``nem-pipeline dashboard``."""

from __future__ import annotations

from datetime import datetime, timedelta

import altair as alt
import paho.mqtt.client as mqtt
import pandas as pd
import pydeck as pdk
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from nem_pipeline import warehouse
from nem_pipeline.config import Settings
from nem_pipeline.dashboard.logic import (
    FUEL_COLORS,
    LiveState,
    filter_facilities,
    prepare_emission_snapshot,
)

st.set_page_config(page_title="NEM Real-Time Dashboard", layout="wide")
settings = Settings()
topics = settings.pipeline.publish
MAP_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
INITIAL_VIEW = pdk.ViewState(latitude=-30.0, longitude=145.0, zoom=4)
STEP = timedelta(minutes=5)
PLAY_SPEEDS = {"5 min / tick": 1, "30 min / tick": 6, "1 h / tick": 12, "3 h / tick": 36}


@st.cache_data
def load_facilities() -> pd.DataFrame:
    """Static facility table with the coordinates chosen by the last pipeline run."""
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        return warehouse.facilities_for_display(conn)


@st.cache_data
def load_timeline() -> pd.DataFrame:
    """Emissions per state and interval for the whole extract."""
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        return warehouse.emission_timeline(conn)


@st.cache_data(max_entries=256)
def load_snapshot(timestamp: datetime) -> pd.DataFrame:
    """Facility totals at one interval."""
    with warehouse.connect(settings.warehouse_path, read_only=True) as conn:
        return warehouse.facility_snapshot(conn, pd.Timestamp(timestamp))


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


def render_live(facilities: pd.DataFrame, states: list[str], fuels: list[str]) -> None:
    """Live view fed by the MQTT subscriber."""
    state = start_subscriber(settings.mqtt_host, settings.mqtt_port)
    st_autorefresh(interval=1000, key="refresh")
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
                initial_view_state=INITIAL_VIEW,
                map_style=MAP_STYLE,
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


def _shift(delta: timedelta, lo: datetime, hi: datetime) -> None:
    """Move the history slider by ``delta``, clamped to the data range."""
    current = st.session_state["history_ts"]
    st.session_state["history_ts"] = min(max(current + delta, lo), hi)


def render_history(states: list[str], fuels: list[str]) -> None:
    """CO₂ history: drag the time slider (or press Play) to see emissions change."""
    timeline = load_timeline()
    if timeline.empty:
        st.info("The warehouse has no facility metrics yet. Run `nem-pipeline run` first.")
        return
    lo = timeline["timestamp"].min().to_pydatetime()
    hi = timeline["timestamp"].max().to_pydatetime()
    st.session_state.setdefault("history_ts", lo)

    st.sidebar.divider()
    st.sidebar.subheader("Playback")
    playing = st.sidebar.toggle("Play", key="playing")
    speed = st.sidebar.selectbox("Speed", list(PLAY_SPEEDS), index=2)
    if playing:
        st_autorefresh(interval=800, key="play_tick")
        nxt = st.session_state["history_ts"] + STEP * PLAY_SPEEDS[speed]
        st.session_state["history_ts"] = lo if nxt > hi else nxt

    st.subheader("CO₂ emissions over time")
    buttons = st.columns([1, 1, 6, 1, 1])
    buttons[0].button("-1 h", on_click=_shift, args=(-12 * STEP, lo, hi), width="stretch")
    buttons[1].button("-5 min", on_click=_shift, args=(-STEP, lo, hi), width="stretch")
    buttons[3].button("+5 min", on_click=_shift, args=(STEP, lo, hi), width="stretch")
    buttons[4].button("+1 h", on_click=_shift, args=(12 * STEP, lo, hi), width="stretch")
    with buttons[2]:
        selected: datetime = st.slider(
            "Time (NEM market time, UTC+10)",
            min_value=lo,
            max_value=hi,
            step=STEP,
            format="YYYY-MM-DD HH:mm",
            key="history_ts",
            label_visibility="collapsed",
        )

    timeline = timeline[timeline["state"].isin(states)]
    totals = timeline.groupby("timestamp")["emissions_t"].sum()
    now_total = float(totals.get(pd.Timestamp(selected), 0.0))
    hour_ago = pd.Timestamp(selected) - pd.Timedelta(hours=1)
    delta = now_total - float(totals[hour_ago]) if hour_ago in totals.index else None

    snapshot = load_snapshot(selected)
    snapshot = filter_facilities(prepare_emission_snapshot(snapshot), states, fuels)
    emitters = snapshot[snapshot["emissions_t"].fillna(0) > 0]

    left, right = st.columns([3, 2])
    with left:
        st.markdown(f"**{selected:%a %d %b %Y, %H:%M}** — circle area ∝ CO₂-e in this interval")
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=snapshot,
            get_position=["longitude", "latitude"],
            get_fill_color="color",
            get_radius="radius_m",
            radius_min_pixels=2,
            radius_max_pixels=40,
            stroked=True,
            get_line_color=[255, 255, 255, 120],
            line_width_min_pixels=1,
            pickable=True,
        )
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=INITIAL_VIEW,
                map_style=MAP_STYLE,
                tooltip={
                    "html": "<b>{facility_name}</b> ({state})<br/>{fuel_type}<br/>"
                    "CO₂-e: {emissions_label} t / 5 min<br/>Power: {power_label} MW",
                },
            ),
            width="stretch",
        )
        st.caption(
            f"{len(emitters)} emitting facilities, {len(snapshot) - len(emitters)} zero-emission"
        )

    with right:
        c1, c2 = st.columns(2)
        c1.metric(
            "CO₂-e this interval (t)",
            f"{now_total:,.0f}",
            delta=None if delta is None else f"{delta:+,.0f} t vs 1 h earlier",
            delta_color="inverse",
        )
        c2.metric("Equivalent hourly rate (t/h)", f"{now_total * 12:,.0f}")
        st.markdown("**Top emitters**")
        top = emitters.head(10)[["facility_name", "state", "fuel_type", "emissions_t", "power_mw"]]
        st.dataframe(
            top.rename(
                columns={
                    "facility_name": "Facility",
                    "state": "State",
                    "fuel_type": "Fuel",
                    "emissions_t": "CO₂-e (t)",
                    "power_mw": "Power (MW)",
                }
            ).style.format({"CO₂-e (t)": "{:,.1f}", "Power (MW)": "{:,.0f}"}),
            hide_index=True,
            width="stretch",
        )

    area = (
        alt.Chart(timeline)
        .mark_area(opacity=0.85)
        .encode(
            x=alt.X("timestamp:T", title=None),
            y=alt.Y("emissions_t:Q", stack=True, title="t CO₂-e per 5 min"),
            color=alt.Color("state:N", title="State"),
            tooltip=[
                alt.Tooltip("timestamp:T", format="%Y-%m-%d %H:%M"),
                "state:N",
                alt.Tooltip("emissions_t:Q", format=",.1f"),
            ],
        )
    )
    rule = (
        alt.Chart(pd.DataFrame({"timestamp": [pd.Timestamp(selected)]}))
        .mark_rule(color="#d62728", strokeWidth=2)
        .encode(x="timestamp:T")
    )
    st.altair_chart((area + rule).properties(height=260), width="stretch")


st.title("NEM Real-Time Dashboard")
try:
    facilities = load_facilities()
except FileNotFoundError as exc:
    st.error(f"{exc}")
    st.stop()

mode = st.sidebar.radio("View", ["Live (MQTT)", "CO₂ history"], horizontal=True)
st.sidebar.header("Filters")
all_states = sorted(facilities["state"].dropna().unique())
selected_states = st.sidebar.multiselect("State", all_states, default=all_states)
selected_fuels = st.sidebar.multiselect("Fuel", list(FUEL_COLORS), default=list(FUEL_COLORS))

if mode == "Live (MQTT)":
    render_live(facilities, selected_states, selected_fuels)
else:
    render_history(selected_states, selected_fuels)
