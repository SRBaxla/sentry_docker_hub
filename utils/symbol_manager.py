from influxdb_client import InfluxDBClient
import os

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def get_all_active_symbols() -> list:
    flux = f'''
        import "influxdata/influxdb/schema"
        schema.tagValues(
          bucket: "{INFLUX_BUCKET}",
          tag: "symbol",
          predicate: (r) => r._measurement == "candles",
          start: -15m
        )
    '''
    result = query_api.query_data_frame(flux)
    return result["_value"].dropna().unique().tolist()
