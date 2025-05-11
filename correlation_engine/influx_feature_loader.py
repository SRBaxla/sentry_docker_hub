from influxdb_client import InfluxDBClient
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
# INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def load_feature(symbol: str, field: str, duration: str = "-1m") -> pd.Series:
    flux = f'''
        from(bucket: "Sentry")
    |> range(start: -1m)
    |> filter(fn: (r) => r["_measurement"] == "candles")
    |> filter(fn: (r) => r["_field"] == "high")
    |> filter(fn: (r) => r["symbol"] == "CHZTRY")
    |> aggregateWindow(every: 1s, fn: mean)
    |> yield(name: "mean")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    df = query_api.query_data_frame(flux)
    if df.empty:
        return pd.Series()
    return pd.Series(df["_value"].values, index=pd.to_datetime(df["_time"]))
