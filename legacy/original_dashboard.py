import json
import time
import threading
import warnings
from queue import Queue

import pandas as pd
import pydeck as pdk
import paho.mqtt.client as mqtt
import streamlit as st
from streamlit_autorefresh import st_autorefresh
from streamlit_folium import st_folium
import folium

warnings.filterwarnings("ignore", category=DeprecationWarning)

# Page Settings
st.set_page_config(page_title="NEM Real-Time Dashboard", layout="wide")
st.title("NEM Real-Time Dashboard")

# Constants
DEFAULT_CENTER = dict(lat=-25.0, lon=134.0, zoom=3)
FUEL_COLOR_HEX = {
    "Battery": "#ff0000",
    "Coal": "#000000",
    "Distillate": "#ff00ff",
    "Solar": "#ffa500",
    "Wind": "#00ffff",
    "Hydro": "#0000ff",
    "Bioenergy": "#00ff00",
    "Gas": "#808080",
    "Pumps": "#800080",
    "Unknown": "#aaaaaa"
}

# Utilities
@st.cache_data
def load_facilities(path: str = "power_with_geo.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]

    rename_map = {}
    for c in df.columns:
        cl = c.lower()
        if cl == "facility name":
            rename_map[c] = "Facility Name"
        elif cl == "latitude":
            rename_map[c] = "latitude"
        elif cl == "longitude":
            rename_map[c] = "longitude"
        elif cl == "fuel type":
            rename_map[c] = "Fuel Type"
        elif cl == "state":
            rename_map[c] = "State"
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    df = df.drop_duplicates(subset=["Facility Name"])
    df = df.dropna(subset=["latitude", "longitude"])
    if "Fuel Type" not in df.columns:
        df["Fuel Type"] = "Unknown"
    df["Fuel Type"] = df["Fuel Type"].fillna("Unknown")
    if "State" not in df.columns:
        df["State"] = "Unknown"
    return df


def hex_to_rgb(hex_color: str):
    hex_color = hex_color.lstrip("#")
    return [int(hex_color[i:i + 2], 16) for i in (0, 2, 4)]


def get_fuel_palette(fuel: str):
    return hex_to_rgb(FUEL_COLOR_HEX.get(fuel, FUEL_COLOR_HEX["Unknown"]))


def merge_live_into_facilities(base_df: pd.DataFrame, fac_buffer: dict) -> pd.DataFrame:
    df = base_df.copy()

    # if empty or missing core columns, create an empty, well-formed frame
    required_cols = ["Facility Name", "latitude", "longitude", "Fuel Type"]
    if df.empty or any(col not in df.columns for col in required_cols):
        for col in required_cols + ["Timestamp", "Power(MW)", "Emissions(t)", "color"]:
            if col not in df.columns:
                df[col] = pd.Series(dtype="object")
        return df

    df["Timestamp"] = df["Facility Name"].apply(lambda n: fac_buffer.get(n, {}).get("Timestamp", "-"))
    df["Power(MW)"] = df["Facility Name"].apply(lambda n: fac_buffer.get(n, {}).get("Power(MW)", "N/A"))
    # Emissions: default to 0 if its null
    df["Emissions(t)"] = df["Facility Name"].apply(
        lambda n: fac_buffer.get(n, {}).get("Emissions(t)", 0)
        if fac_buffer.get(n, {}).get("Emissions(t)") not in [None, "", "N/A"]
        else 0
    )
    df["Fuel Type"] = df["Fuel Type"].apply(lambda f: f if f in FUEL_COLOR_HEX else "Unknown")
    df["color"] = df["Fuel Type"].apply(get_fuel_palette)
    return df


# Data
FACILITIES = load_facilities("power_with_geo.csv")

# Sidebar Controls
st.sidebar.header("MQTT Connection Settings")
broker_host = st.sidebar.text_input("Broker Host", "test.mosquitto.org")
broker_port = st.sidebar.number_input("Port", 1883, step=1)
topic_facility = st.sidebar.text_input("Facility Topic", "facilities/metrics_info")

st.sidebar.divider()

# State Filter
states = sorted(FACILITIES["State"].dropna().unique()) if "State" in FACILITIES.columns else []
selected_states = st.sidebar.multiselect(
    "Filter by State",
    states,
    default=states if states else []
)

# Fuel Type Filter
available_fuels = list(FUEL_COLOR_HEX.keys())
if "Unknown" in available_fuels:
    available_fuels.remove("Unknown")
    available_fuels.append("Unknown")
selected_fuels = st.sidebar.multiselect(
    "Filter by Fuel Type",
    available_fuels,
    default=available_fuels
)

# Session State
if "msg_queue" not in st.session_state:
    st.session_state["msg_queue"] = Queue()
if "fac_buffer" not in st.session_state:
    st.session_state["fac_buffer"] = {}
if "mqtt_started" not in st.session_state:
    st.session_state["mqtt_started"] = False
if "selected_facility" not in st.session_state:
    st.session_state["selected_facility"] = None

msg_queue: Queue = st.session_state["msg_queue"]
fac_buffer: dict = st.session_state["fac_buffer"]

# MQTT Setup
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.subscribe(topic_facility)
    else:
        print(f"[MQTT] Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        msg_queue.put(payload)
    except Exception as e:
        print("[MQTT] decode error:", e)

def mqtt_loop():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker_host, int(broker_port), 60)
    client.loop_forever()

if not st.session_state["mqtt_started"]:
    threading.Thread(target=mqtt_loop, daemon=True).start()
    st.session_state["mqtt_started"] = True

# Ingest incoming MQTT messages
def drain_queue_into_buffer():
    while not msg_queue.empty():
        payload = msg_queue.get()
        records = payload.get("data", [])
        if isinstance(records, dict):
            records = [records]
        for rec in records:
            name = rec.get("Facility Name") or rec.get("facility") or rec.get("name")
            if not name:
                continue
            ts = rec.get("Timestamp") or payload.get("timestamp")
            power = (
                rec.get("Power(MW)")
                or rec.get("Power")
                or rec.get("POWER")
                or rec.get("Value")
            )
            emis = rec.get("Emissions(t)") or rec.get("EMISSIONS") or None
            fuel = rec.get("Fuel Type", "Unknown")
            fac_buffer[name] = {
                "Timestamp": ts,
                "Power(MW)": power,
                "Emissions(t)": emis,
                "Fuel Type": fuel
            }

drain_queue_into_buffer()
st_autorefresh(interval=500, key="refresh_counter")

# Layout
left_col, right_col = st.columns([1, 1])

# LEFT: Dynamic map
with left_col:
    st.markdown("### Facility Map")

    # If either filter is entirely unselected -> show a clean empty map
    if len(selected_fuels) == 0 or (states and len(selected_states) == 0):
        empty_deck = pdk.Deck(
            layers=[],
            initial_view_state=pdk.ViewState(
                latitude=DEFAULT_CENTER["lat"],
                longitude=DEFAULT_CENTER["lon"],
                zoom=DEFAULT_CENTER["zoom"]
            ),
            map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
        )
        st.pydeck_chart(empty_deck, width="stretch", key=f"deck_{int(time.time() // 3)}")


    else:
        # apply filter
        # Fuel filter with partial match
        filtered_rows = []
        for _, row in FACILITIES.iterrows():
            fuel = str(row.get("Fuel Type", "Unknown"))
            matched_fuel = None
            for f in selected_fuels:
                if f.lower() in fuel.lower():
                    matched_fuel = f
                    break
            if matched_fuel:
                new_row = row.copy()
                new_row["Fuel Type"] = matched_fuel
                filtered_rows.append(new_row)
        fac_base = pd.DataFrame(filtered_rows) if filtered_rows else FACILITIES.iloc[0:0].copy()

        # State filter
        if "State" in fac_base.columns and selected_states:
            fac_base = fac_base[fac_base["State"].isin(selected_states)]

        # empty filter -> clean map
        if fac_base.empty:
            empty_deck = pdk.Deck(
                layers=[],
                initial_view_state=pdk.ViewState(
                    latitude=DEFAULT_CENTER["lat"],
                    longitude=DEFAULT_CENTER["lon"],
                    zoom=DEFAULT_CENTER["zoom"]
                ),
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            )
            st.pydeck_chart(empty_deck, width="stretch")
        else:
            # Merge live data
            fac_display = merge_live_into_facilities(fac_base, fac_buffer)
            fac_display["color"] = fac_display["Fuel Type"].apply(get_fuel_palette)
            fac_display["radius_px"] = 8

            layer = pdk.Layer(
                "ScatterplotLayer",
                data=fac_display,
                get_position=["longitude", "latitude"],
                get_fill_color="color",
                get_radius="radius_px",
                radius_units="pixels",
                pickable=True,
                auto_highlight=True
            )

            view_state = pdk.ViewState(
                latitude=float(fac_display["latitude"].mean()),
                longitude=float(fac_display["longitude"].mean()),
                zoom=DEFAULT_CENTER["zoom"]
            )

            tooltip = {
                "html": (
                    "<b>{Facility Name}</b><br/>"
                    "Fuel: {Fuel Type}<br/>"
                    "Power: {Power(MW)} MW<br/>"
                    "Emissions: {Emissions(t)} tCO₂<br/>"
                    "Time: {Timestamp}"
                ),
                "style": {"backgroundColor": "steelblue", "color": "white"}
            }

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            )
            st.pydeck_chart(deck, width="stretch")

    # Hide Folium layer to capture click
    fmap = folium.Map(location=[-25, 134], zoom_start=4, tiles=None)
    fmap.get_root().html.add_child(folium.Element("<style>.leaflet-container{height:0px!important;}</style>"))
    click_map = st_folium(fmap, width=1, height=1, returned_objects=["last_clicked"], key="hidden_click_map")

    if click_map and click_map.get("last_clicked"):
        lat_c = click_map["last_clicked"]["lat"]
        lon_c = click_map["last_clicked"]["lng"]

        # pick nearest among currently visible markers
        if "fac_display" in locals() and not fac_display.empty:
            distances = (fac_display["latitude"] - lat_c).abs() + (fac_display["longitude"] - lon_c).abs()
            nearest = fac_display.loc[distances.idxmin(), "Facility Name"]
            if st.session_state.get("selected_facility") != nearest:
                st.session_state["selected_facility"] = nearest
                st.session_state["clicked_time"] = time.time()
                st_autorefresh(interval=500, limit=1, key=f"click_refresh_{int(time.time())}")

# RIGHT: Market Data
with right_col:
    st.markdown("### Market Data")

    # latest timestamp from MQTT buffer
    timestamps = [
        pd.to_datetime(v.get("Timestamp"))
        for v in st.session_state["fac_buffer"].values()
        if v.get("Timestamp") not in [None, "-", ""]
    ]
    latest_ts = max(timestamps) if timestamps else None

    @st.cache_data
    def load_market(path="market_data.csv"):
        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]
        time_cols = [c for c in df.columns if "time" in c.lower() or "date" in c.lower()]
        if time_cols:
            df.rename(columns={time_cols[0]: "timestamp"}, inplace=True)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        else:
            st.warning("No timestamp-like column found in market_data.csv.")
            df["timestamp"] = pd.NaT
        return df

    market_df = load_market("market_data.csv")

    if latest_ts is not None:
        window = market_df[
            market_df["timestamp"].between(latest_ts - pd.Timedelta(minutes=5),
                                           latest_ts + pd.Timedelta(minutes=5))
        ]
        if not window.empty:
            closest_time = window["timestamp"].iloc[(window["timestamp"] - latest_ts).abs().argsort()].iloc[0]
            same_time = market_df[market_df["timestamp"] == closest_time]
            st.markdown(f"**Timestamp:** {closest_time.strftime('%Y-%m-%d %H:%M')}")

            display_df = same_time[["region", "Price($/MWh)", "Demand(MW)"]].copy()
            display_df.columns = ["Region", "Price ($/MWh)", "Demand (MW)"]
            display_df = display_df.sort_values("Region")

            st.dataframe(
                display_df.style.format({
                    "Price ($/MWh)": "{:.2f}",
                    "Demand (MW)": "{:,.0f}"
                }),
                width='stretch',
                height=210
            )

            avg_price = display_df["Price ($/MWh)"].mean()
            total_demand = display_df["Demand (MW)"].sum()
            st.markdown(f"**Average Price:** {avg_price:.2f} $/MWh  \n**Total Demand:** {total_demand:,.0f} MW")
    else:
        st.info("Waiting for live MQTT timestamp to show market data...")
