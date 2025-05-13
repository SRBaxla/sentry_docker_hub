from influxdb_client import InfluxDBClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# InfluxDB configuration
INFLUX_URL    = "http://localhost:8086"  # Set to your actual InfluxDB URL
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN")
INFLUX_ORG    = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def load_feature(symbol: str, field: str, duration: str = "-1m") -> pd.Series:
    """
    Loads a specific feature (field) for a given symbol from InfluxDB.
    
    :param symbol: The symbol to fetch data for.
    :param field: The field (e.g., "high", "low", "open", "close") to fetch.
    :param duration: The time range to query, default is "-1m" (last minute).
    :return: A pandas Series with the requested data.
    """
    # Construct the Flux query with dynamic symbol, field, and duration
    flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {duration})  # Use the dynamic duration
        |> filter(fn: (r) => r["_measurement"] == "candles")
        |> filter(fn: (r) => r["_field"] == "{field}")  # Dynamic field
        |> filter(fn: (r) => r["symbol"] == "{symbol}")  # Dynamic symbol
        |> aggregateWindow(every: 1s, fn: mean)
        |> yield(name: "mean")
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    try:
        # Query data from InfluxDB and load into a DataFrame
        df = query_api.query_data_frame(flux)
        
        if df.empty:
            print(f"No data found for {symbol} with field {field} within the duration {duration}.")
            return pd.Series()  # Return an empty Series if no data
        
        # Return the data as a pandas Series, indexed by datetime
        return pd.Series(df["_value"].values, index=pd.to_datetime(df["_time"]))
    
    except Exception as e:
        print(f"Error querying InfluxDB: {e}")
        return pd.Series()  # Return an empty Series in case of error
