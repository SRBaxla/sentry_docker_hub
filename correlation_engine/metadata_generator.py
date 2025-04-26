# correlation_engine/metadata_generator.py

import os
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# ─── CONFIG ────────────────────────────────────────────────────────────────
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "Sentry")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")

COMMON_BUZZWORDS = ["ETF","DeFi","SEC","Bullish","Bearish","Pump","Crash","Rally","Halving"]
CHUNK_SIZE       = 500   # tune for memory/throughput

# ─── INFLUX CLIENTS ─────────────────────────────────────────────────────────
influx    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx.write_api(write_options=WriteOptions(
    batch_size=CHUNK_SIZE,
    flush_interval=10_000,
    jitter_interval=2_000,
    retry_interval=5_000
))
query_api = influx.query_api()

# ─── SYMBOL STREAM ──────────────────────────────────────────────────────────
def fetch_symbols():
    flux = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
      bucket: "{INFLUX_BUCKET}",
      tag: "symbol"
    )
    '''
    for table in query_api.query_stream(flux):
        for rec in table.records:
            yield rec.get_value()

# ─── KEYWORD GENERATION ────────────────────────────────────────────────────
_coin_list = None
def load_coins():
    global _coin_list
    if _coin_list is None:
        _coin_list = requests.get(
            "https://api.coingecko.com/api/v3/coins/list", timeout=10
        ).json()
    return _coin_list

def generate_keywords(symbol):
    base = symbol.replace("USDT","").replace("BUSD","").replace("USD","").upper()
    name = base.capitalize()
    for c in load_coins():
        if c["symbol"].upper() == base:
            name = c["name"]
            break
    kws = [name, base, f"{base}/USD", f"{name} price", f"{name} news"]
    return kws + COMMON_BUZZWORDS

# ─── MAIN WORKFLOW ─────────────────────────────────────────────────────────
def run_metadata_generation():
    now = datetime.now(timezone.utc).isoformat()
    buffer = []

    # Generate and batch-write symbol metadata
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sym, kws in zip(fetch_symbols(), pool.map(generate_keywords, fetch_symbols())):
            pt = (
                Point("symbol_metadata")
                .tag("symbol", sym)
                .field("keywords", ",".join(kws))
                .time(now, WritePrecision.S)
            )
            buffer.append(pt)
            if len(buffer) >= CHUNK_SIZE:
                write_api.write(bucket=INFLUX_BUCKET, record=buffer)
                buffer.clear()

    # Flush any remaining
    if buffer:
        write_api.write(bucket=INFLUX_BUCKET, record=buffer)

    influx.close()
    print("✅ Symbol metadata successfully written to InfluxDB.")

if __name__ == "__main__":
    run_metadata_generation()
