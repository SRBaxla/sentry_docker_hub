from influxdb_client import InfluxDBClient
import pandas as pd
import os

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def load_feature(symbol: str, field: str, duration: str = "-1m") -> pd.Series:
    flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {duration})
        |> filter(fn: (r) => r["_measurement"] == "candles")
        |> filter(fn: (r) => r["_field"] == "{field}")
        |> filter(fn: (r) => r["symbol"] == "{symbol}")
        |> aggregateWindow(every: 1s, fn: mean)
        |> yield(name: "mean")
    '''
    df = query_api.query_data_frame(flux)
    if df.empty:
        return pd.Series()
    return pd.Series(df["_value"].values, index=pd.to_datetime(df["_time"]))
