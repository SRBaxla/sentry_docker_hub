from influxdb_client import InfluxDBClient, Point
import os
from influxdb_client.client.write_api import SYNCHRONOUS

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

def process_new_symbols(all_symbols):
    existing_symbols = get_existing_symbols()
    new_symbols = set(all_symbols) - existing_symbols
    for symbol in new_symbols:
        # Generate keywords and buzzwords as needed
        keywords = generate_keywords_for_symbol(symbol)
        buzzwords = generate_buzzwords_for_symbol(symbol)
        write_symbol_metadata(symbol, keywords, buzzwords=buzzwords)

def get_existing_symbols():
    query_api = client.query_api()
    flux_query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "{INFLUX_BUCKET}", tag: "symbol")
    '''
    result = query_api.query_data_frame(flux_query)
    if not result.empty:
        return set(result["_value"].dropna().unique())
    return set()


def write_symbol_metadata(symbol, keywords, trust_score=None, buzzwords=None, source="system"):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    point = (
        Point("symbol_metadata")
        .tag("symbol", symbol)
        .tag("source", source)
        .field("keywords", ",".join(keywords))
    )
    if trust_score is not None:
        point.field("trust_score", trust_score)
    if buzzwords:
        point.field("buzzwords", ",".join(buzzwords))
    write_api.write(bucket=INFLUX_BUCKET, record=point)
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

# # Example_ usage:
# write_symbol_metadata(
#     symbol="BTCUSDT",
#     keywords=["Bitcoin", "BTC", "Satoshi"],
#     trust_score=0.95,
#     buzzwords=["ETF", "Bullish", "BlackRock"]
# )