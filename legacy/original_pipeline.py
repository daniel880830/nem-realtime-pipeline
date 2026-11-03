import os
import threading # concurrent data publishing.
os.environ["OPENELECTRICITY_API_KEY"] = "<REDACTED - set OPENELECTRICITY_API_KEY>" #API key for accessing the OpenElectricity data.

#main libraries for performing Task 1 - 3
from openelectricity import OEClient
from openelectricity.types import DataMetric, NetworkCode, DataInterval, UnitStatusType, MarketMetric
from datetime import datetime
import json
import paho.mqtt.publish as publish
import pandas as pd
import time
import requests
import duckdb
import re
from rapidfuzz import process, fuzz
import googlemaps
from tqdm import tqdm
from bs4 import BeautifulSoup
from io import StringIO

#region Database creation as part of Assignment 1
class DatabaseGeneration():
    def __init__(self, google_api_key, db_path='assignment1.duckdb'):
        """Initialize the DatabaseGeneration class"""
        self.db_path = db_path
        self.conn = None
        self.gmaps = googlemaps.Client(key=google_api_key)
        self.core_summary = None
        self.power_dfs = None
        
        # URLs and options
        self.PS_URL = "https://cer.gov.au/markets/reports-and-data/large-scale-renewable-energy-data"
        self.CER_PAGE_URL = "https://cer.gov.au"
        self.station_options = {'accredited', 'committed', 'probable'}
    
    # Web scraping general function
    @staticmethod
    def get_html(url):
        """Get HTML content from URL"""
        webpage = requests.get(url).text
        content = None
        if webpage is not None:
            content = BeautifulSoup(webpage, 'html.parser')
        return content
    
    # Get response from a URL
    @staticmethod
    def get_response(url):
        """Get response from URL with retry logic"""
        while True:
            try:
                response = requests.get(url)
                if response.text is not None:
                    return response.text
            except Exception as e:
                print(f"Failure in connection: {e}")
                time.sleep(1)
    
    def open_connection(self):
        """Open connection to DuckDB database"""
        self.conn = duckdb.connect(database=self.db_path)
        try:
            self.conn.execute("INSTALL spatial; LOAD spatial;")
            print("Spatial extension loaded successfully.")
        except Exception as e:
            print("Spatial extension unavailable, fallback to basic mode:", e)
        return self.conn
    
    def close_connection(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
    
    # Web scraping CER
    def find_cer_dataset(self, url=None, base_url=None, options=None):
        """Find CER dataset URLs"""
        url = url or self.PS_URL
        base_url = base_url or self.CER_PAGE_URL
        options = options or self.station_options
        
        soup = self.get_html(url)
        csv_paths = []
        for item in soup.find_all('div', 'vue-datavis-wrapper'):
            for sub_item in item.find_all('div', 'cer-accordion__body'):
                a = sub_item.find(
                    'a',
                    href=lambda x: x and any(opt in x for opt in options))
                if a:
                    csv_paths.append(base_url + a['href'])
        return csv_paths
    
    def get_datasets(self, csv_paths):
        """Download datasets from CSV paths"""
        power_dfs = []
        for path in csv_paths:
            csv_response = self.get_response(path)
            df = pd.read_csv(StringIO(csv_response))
            power_dfs.append(df)
        return power_dfs
    
    def fetch_power_station_data(self):
        """Fetch all power station data from CER"""
        power_station_paths = self.find_cer_dataset()
        self.power_dfs = self.get_datasets(power_station_paths)
        return self.power_dfs
    
    def cer_integration(self, power_dfs=None):
        """Integrate CER datasets"""
        if power_dfs is None:
            power_dfs = self.power_dfs
        
        ac, cm, pr = power_dfs
        rename_map = {
            "Power station name": "Project Name",
            "Installed capacity (MW)": "MW Capacity",
            "Fuel Source (s)": "Fuel Source",
            "State": "State",
            "State ": "State"
        }
        
        def standardize(df, status):
            df = df.rename(columns=rename_map)
            df = df[["Project Name", "State", "MW Capacity", "Fuel Source"]].copy()
            df["status"] = status
            return df
        
        ac_core = standardize(ac, "accredited")
        cm_core = standardize(cm, "committed")
        pr_core = standardize(pr, "probable")
        
        return pd.concat([ac_core, cm_core, pr_core], ignore_index=True)
    
    def cer_cleaning(self, df, export=True):
        """Clean CER data"""
        df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        df["Project Name"] = df["Project Name"].str.split("-").str[0].str.strip()
        keys = ['Project Name', 'State']
        dupmask = df.duplicated(keys, keep=False)
        duplicates = df.loc[dupmask].sort_values(keys)
        
        # Aggregate duplicates
        numeric_cols = df.select_dtypes(include="number").columns.difference(keys)
        non_numeric_cols = df.columns.difference(numeric_cols.union(keys))
        agg_rules = {**{col: "first" for col in non_numeric_cols},
                    **{col: "mean" for col in numeric_cols}}
        df_clean = df.groupby(keys, as_index=False).agg(agg_rules)
        
        if export:
            df_clean.to_csv("core_summary.csv", index=False)
            print("Exported core_summary.csv")
        
        return df_clean
    
    def get_coordinates_google(self, address):
        """Get coordinates using Google Maps API"""
        try:
            geocode_result = self.gmaps.geocode(address)
            if geocode_result:
                location = geocode_result[0]['geometry']['location']
                lat = location['lat']
                lon = location['lng']
                return lat, lon
        except Exception as e:
            print(f"Error on {address}: {e}")
        return None, None
    
    def geocode_projects(self, df=None, sleep=0.05):
        """Geocode projects using Google Maps API"""
        if df is None:
            df = self.core_summary
        
        latitudes, longitudes = [], []
        
        for addr in tqdm(df["Project Name"] + ", " + df["State"] + ", Australia",
                        desc="Google Geocoding", unit="site"):
            lat, lon = self.get_coordinates_google(addr)
            latitudes.append(lat)
            longitudes.append(lon)
            time.sleep(sleep)
        
        df = df.copy()
        df["latitude"] = latitudes
        df["longitude"] = longitudes
        
        return df
    
    def create_powerStation_schema(self, df=None):
        """Create power station schema in database"""
        if df is None:
            df = self.core_summary
        
        if self.conn is None:
            raise Exception("Database connection not open. Call open_connection() first.")
        
        # Drop existing tables and sequences
        for table in ['powerStation_fact_table', 'powerStation_dim_table']:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        for seq in ['station_id_seq']:
            self.conn.execute(f"DROP SEQUENCE IF EXISTS {seq}")
        
        self.conn.execute("CREATE SEQUENCE station_id_seq START 1")
        
        # Create dimension table
        self.conn.execute("""
            CREATE TABLE powerStation_dim_table
            (
                station_id  INTEGER DEFAULT nextval('station_id_seq'),
                projectName VARCHAR NOT NULL,
                state       VARCHAR NOT NULL,
                fuel_source VARCHAR NOT NULL,
                status      VARCHAR NOT NULL,
                UNIQUE (projectName, state),
                PRIMARY KEY (station_id)
            )
        """)
        
        # Create fact table
        self.conn.execute("""
            CREATE TABLE powerStation_fact_table
            (
                station_id INTEGER,
                mw_capacity DOUBLE,
                location   GEOMETRY,
                FOREIGN KEY (station_id) REFERENCES powerStation_dim_table (station_id),
                PRIMARY KEY (station_id)
            )
        """)
        
        # Insert into dimension table
        dim_cols = ['Project Name', 'State', 'Fuel Source', 'status']
        dim_df = df[dim_cols].drop_duplicates().reset_index(drop=True)
        
        self.conn.register("powerStation_dim_df", dim_df)
        self.conn.execute("""
            INSERT INTO powerStation_dim_table (projectName, state, fuel_source, status)
            SELECT *
            FROM powerStation_dim_df
        """)
        
        dim_table = self.conn.execute("SELECT * FROM powerStation_dim_table").fetchdf()
        powerStation_fact_df = df.merge(
            dim_table,
            left_on=['Project Name', 'State'],
            right_on=['projectName', 'state'],
            how='inner'
        )
        
        powerStation_fact_df = powerStation_fact_df[['station_id', 'MW Capacity', 'longitude', 'latitude']]
        powerStation_fact_df.rename(columns={"MW Capacity": "mw_capacity"}, inplace=True)
        
        self.conn.register("powerStation_fact_df", powerStation_fact_df)
        print(f"Fact table columns: {powerStation_fact_df.columns.tolist()}")
        
        self.conn.execute("""
            INSERT INTO powerStation_fact_table (station_id, mw_capacity, location)
            SELECT station_id, mw_capacity, ST_Point2D(longitude, latitude)
            FROM powerStation_fact_df
        """)
        
        print("Successfully created PowerStation fact and dimension tables with spatial location.")
        return True
    
    def run_pipeline(self):
        print("Starting ETL pipeline...")
        self.open_connection()
        self.fetch_power_station_data()
        self.core_summary = self.cer_integration()
        self.core_summary = self.cer_cleaning(self.core_summary)
        self.core_summary = self.geocode_projects()
        self.create_powerStation_schema()
        return self.core_summary

# Usage example
# Initialize the class
db_gen = DatabaseGeneration(
    google_api_key="<REDACTED - Google Maps key>",
    db_path="assignment1.duckdb"
)
#endregion Database creation as part of Assignment 1

nemClient = OEClient() #OpenElectricity Python Client 

# OpenElectricity facility Codes extraction for code cleanliness
def get_facility_codes():
    facility_codes_file = 'facility_codes.json'
    if os.path.exists(facility_codes_file) and os.path.getsize(facility_codes_file) > 0:
        with open(facility_codes_file, 'r') as file:
            facility_codes = json.load(file)
    else:
        fac_data_response = nemClient.get_facilities(
            status_id=[UnitStatusType.OPERATING, UnitStatusType.COMMITTED],
            network_id=["NEM"]
        )
        facility_codes = {facility.name: facility.code for facility in fac_data_response.data}
        with open(facility_codes_file, "w") as file:
            json.dump(facility_codes, file, indent=4)
        print("Facility Codes Cached")
    return facility_codes

#Task 1: Extracting the power facilities power and emission data using the OpenElectricity API 
# this is done with the help of OpenElectricty Python Library that implements API data extraction.
def extract_power_and_emissions_data(facilities, f_codes, n, slice_size, obs_date):
    date_start, date_end = obs_date['start'], obs_date['end']
    data = []
    for i in range(0, n, slice_size):
        fac_metrics_response = nemClient.get_facility_data(
            network_code="NEM",
            facility_code=f_codes[i:i + slice_size],
            metrics=[DataMetric.POWER, DataMetric.EMISSIONS],
            interval="5m",
            date_start=date_start,
            date_end=date_end
        )

        for facility in facilities.data:
            for series in fac_metrics_response.data:
                for result in series.results:
                    if result.columns.unit_code in [unit.code for unit in facility.units]:
                        for data_pt in result.data:
                            unit = next(u for u in facility.units if u.code == result.columns.unit_code)
                            data.append({
                                'Facility Name': facility.name,
                                'Facility Code': facility.code,
                                'Unit Code': unit.code,
                                'Fuel Type': unit.fueltech_id.value.replace("_", " ").title(),
                                'Timestamp': data_pt.timestamp.strftime("%Y-%m-%d %H:%M"),
                                'Metric': series.metric,
                                'Value': data_pt.value,
                                'Measure Unit': series.unit
                            })
    return data

# Task 1 (Optional): Extracting market information for every network region in NEM (NSW, QLD, VIC, TAS, SA)
def extract_market_data(obs_date):
    date_start, date_end = obs_date['start'], obs_date['end']
    response = nemClient.get_market(
        network_code="NEM",
        metrics=[MarketMetric.PRICE, MarketMetric.DEMAND],
        interval="5m",
        date_start=date_start,
        date_end=date_end,
        primary_grouping="network_region"
    )

    records = []
    for timeseries in response.data:
        for result in timeseries.results:
            region = result.name.split("_")[-1]
            for dp in result.data:
                records.append({
                    "Timestamp": dp.timestamp,
                    "region": region,
                    "metric": timeseries.metric,
                    "value": dp.value,
                    "unit": timeseries.unit
                })
    return pd.DataFrame(records)

#Controller function to extract and save power facility data
def get_facility_metrics(obs_date):
    """Get facility-level power/emission metrics and save to CSV."""
    facility_codes = get_facility_codes()
    f_codes = [facility_codes[name] for name in facility_codes]
    facilities = nemClient.get_facilities(network_id=["NEM"])

    n = len(facilities.data)
    slice_size = max(1, n // 20)
    data = extract_power_and_emissions_data(facilities, f_codes, n, slice_size, obs_date)

    df = pd.DataFrame(data)
    df.to_csv("power_df.csv", index=False)
    print(f"Saved power_df.csv with {len(df)} rows.")

#Controller function to extract and save market data
def get_market_data(obs_date):
    """Get market metrics and save to CSV."""
    df = extract_market_data(obs_date)
    df.to_csv("market_data.csv", index=False)
    print(f"Saved market_data.csv with {len(df)} rows.")

#Task 2: Caching the data retrieved from the API's into CSVs and cleaning the 
#said CSV to avoid inconsistency and irregularly values being sent during publishing
def csv_cleaning():
    """Clean and reformat extracted CSVs for downstream use."""
    power_df = pd.read_csv("power_df.csv")
    market_df = pd.read_csv("market_data.csv")

    power_temp = power_df[power_df['Metric'] == 'power']
    market_temp = market_df[market_df['metric'] == 'price'] 

    power_temp = power_temp.copy()  
    power_temp.loc[:, 'Power(MW)'] = power_df.query("Metric == 'power'")['Value'].values
    power_temp.loc[:, 'Emissions(t)'] = power_df.query("Metric == 'emissions'")['Value'].values
    power_temp = power_temp.dropna()
    power_temp = power_temp[~((power_temp['Power(MW)'] == 0) & (power_temp['Emissions(t)'] == 0))]
    power_temp = power_temp.drop(columns=['Value', 'Metric', 'Measure Unit'])
    
    
    market_temp = market_temp.copy()
    market_temp.loc[:, 'Price($/MWh)'] = market_df.query("metric == 'price'")['value'].values
    market_temp.loc[:, 'Demand(MW)'] = market_df.query("metric == 'demand'")['value'].values
    #market_temp = market_temp.drop(columns=['Unnamed: 0'])
    market_temp['Timestamp'] = market_temp['Timestamp'].str.replace(r'\+10:00$', '', regex=True)

    power_temp.to_csv('power_df.csv', index=False)
    market_temp.to_csv('market_data.csv', index=False)
    print("CSV's have been cleaned and updated")

# Normalize the projectName from DuckDb and the Facility Name from the power_df.csv for joining the two files.
def normalize_name(name: str) -> str:
    """Normalize facility/project names for better fuzzy matching."""
    if pd.isna(name):
        return ""
    name = re.sub(r"[^A-Za-z0-9 ]+", "", str(name)).lower()
    for term in [
        "solar farm", "wind farm", "power station", "power plant", "energy",
        "battery", "ps", "wf", "sf", "farm", "plant", "station"
    ]:
        name = name.replace(term, "")
    return re.sub(r"\s+", " ", name).strip()

#method to enrich the csv data with the database data that provides the longitude and latitude of each power facility
#here we try to find a similarity feature between the two tables and join them based on it.
def merge_with_duckdb_location_fuzzy(
    db_path="assignment1.duckdb",
    csv_path="power_df.csv",
    output_path="power_with_geo.csv"
):
    """Fuzzy match facility names with DuckDB project coordinates."""
    conn = duckdb.connect(database=db_path)
    conn.execute("INSTALL spatial; LOAD spatial;")
    tables = conn.execute("SHOW TABLES").fetchall()
    df_db = conn.execute("""
        SELECT projectName, State, ST_Y(location) AS latitude, ST_X(location) AS longitude
        FROM powerStation_fact_table
        JOIN powerStation_dim_table USING (station_id)
        WHERE location IS NOT NULL
    """).fetchdf()
    conn.close()

    df_power = pd.read_csv(csv_path)
    df_power["Facility Name"] = df_power["Facility Name"].astype(str).str.strip()

    df_db["clean_name"] = df_db["projectName"].apply(normalize_name)
    df_power["clean_name"] = df_power["Facility Name"].apply(normalize_name)
    # names_db = df_db["clean_name"].tolist()
    latitudes, longitudes, scores = [], [], []
    coord_names = (
        df_db.drop_duplicates(subset=["clean_name"])
            .set_index("clean_name")[["state", "latitude", "longitude"]]
            .to_dict(orient="index")
    )
    db_names = list(coord_names)
    
    match_vector = process.cdist(
        df_power["clean_name"],
        db_names,
        scorer=fuzz.partial_ratio,
        workers=-1,
        score_cutoff=70
    )
    
    fallback_mask = match_vector.max(axis=1) < 70
    fallback_names = [df_power['clean_name'].iloc[i] for i in range(len(df_power)) if fallback_mask[i]]
    if fallback_names:
        fallback_scores = process.cdist(
            fallback_names,
            db_names,
            scorer=fuzz.token_sort_ratio,
            workers=-1,
            score_cutoff=65,
        )
        match_vector[fallback_mask] = fallback_scores
    
    best_match_idx = match_vector.argmax(axis=1)
    best_scores = match_vector.max(axis=1)
    best_names = [db_names[i] if s > 0 else None for i, s in zip(best_match_idx, best_scores)]

    df_power["match_score"] = best_scores
    df_power['state'] = [coord_names.get(n, {}).get("state") for n in best_names]
    df_power["latitude"] = [coord_names.get(n, {}).get("latitude") for n in best_names]
    df_power["longitude"] = [coord_names.get(n, {}).get("longitude") for n in best_names]

    matched = df_power["latitude"].notna().sum()
    print(f"Fuzzy matched facilities: {matched}/{len(df_power)} ({matched / len(df_power) * 100:.2f}%)")
    df_power = df_power.dropna()
    
    df_power.to_csv(output_path, index=False)
    print(f"Saved merged dataset: {output_path}")
    return df_power

# MQTT Publisher
#mqtt publisher on localhost for the per-facility metrics
def publish_data(df, mqtt_topic, broker, port, stop_event):
    timestamp_grouped = df.groupby('Timestamp')
    
    while not stop_event.is_set():
        for timestamp, group in timestamp_grouped:
            data = group.to_dict(orient='records')
            
            for i, record in enumerate(data, start=1):
                record["index"] = i
            
            payload = {
                "timestamp": str(timestamp),
                "data": data
            }
            
            json_payload = json.dumps(payload)
            publish.single(mqtt_topic, json_payload, hostname=broker, port=port)
            print(f"Published to {mqtt_topic}: {json_payload}")
            print()
            
            time.sleep(0.1) # 0.1s delay after every publish call
        time.sleep(60)   #60s delay before restart if the publisher is not interrupted
    
    if not stop_event.is_set():
        time.sleep(60) #if a stop event is called the publisher sleeps for 60s before closing for a graceful exit

stop_event = threading.Event()

def mqtt_publisher():
    MQTT_BROKER = "test.mosquitto.org"
    MQTT_PORT = 1883
    POWER_MQTT_TOPIC = "facilities/metrics_info"
    MARKET_MQTT_TOPIC = "market/metrics_info"
    
    power_df = pd.read_csv("power_with_geo.csv")
    market_df = pd.read_csv("market_data.csv")
    
    power_thread = threading.Thread(
        target=publish_data,
        args = (power_df, POWER_MQTT_TOPIC, MQTT_BROKER, MQTT_PORT, stop_event),
        daemon = False
    )
    
    market_thread = threading.Thread(
        target=publish_data,
        args = (market_df, MARKET_MQTT_TOPIC, MQTT_BROKER, MQTT_PORT, stop_event),
        daemon = False
    )
    
    threads = [power_thread, market_thread]
    
    try:
        power_thread.start()
        market_thread.start()
        
        power_thread.join()
        market_thread.join()
    except KeyboardInterrupt:
        stop_event.set()
        
        for thread in threads:
            thread.join(timeout=5)
        print("All threads stopped")


#  Main workflow
#ETL Data Workflow for duckdb
core_summary = db_gen.run_pipeline()
db_gen.close_connection()

#publisher workflow
obs_date = {"start": datetime(2025, 10, 1), "end": datetime(2025, 10, 8)}
get_facility_metrics(obs_date)
get_market_data(obs_date)
csv_cleaning()
merge_with_duckdb_location_fuzzy()
mqtt_publisher()
