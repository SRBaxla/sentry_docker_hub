from influxdb_client import InfluxDBClient, Point
import os

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

def get_active_symbols_from_influx() -> list:
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

def write_symbol_metadata(symbol, keywords, client):
    point = (
        Point("symbol_metadata")
        .tag("symbol", symbol)
        .tag("created_by", "system")
        .field("keywords", ",".join(keywords))
    )
    write_api = client.write_api()
    write_api.write(bucket="Sentry", record=point)
    print(f"✅ Metadata written for {symbol}")

def read_symbol_metadata(client):
    query_api = client.query_api()
    flux = '''
    from(bucket: "Sentry")
    |> range(start: -30d)
    |> filter(fn: (r) => r["_measurement"] == "symbol_metadata")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    df = query_api.query_data_frame(flux)
    if not df.empty:
        return dict(zip(df["symbol"], df["keywords"]))
    else:
        return {}

# # Usage:
# symbol_keywords = read_symbol_metadata(client)
