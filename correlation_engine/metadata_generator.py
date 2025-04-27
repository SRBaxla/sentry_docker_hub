# correlation_engine/correlation_and_metadata.py

import os
import requests
from datetime import datetime, timezone
from itertools import combinations

import pandas as pd
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions
# parallelize keyword generation lightly
from concurrent.futures import ThreadPoolExecutor
from utils.influx_writer import get_influx_client

# ─── CONFIG ────────────────────────────────────────────────────────────────
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "Sentry")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sentry")

COMMON_BUZZWORDS = ["ETF","DeFi","SEC","Bullish","Bearish","Pump","Crash","Rally","Halving"]
CHUNK_SIZE       = 500     # tune for your environment
CORR_WINDOW      = "60m"   # how far back to pull closes for correlation

# ─── CLIENT SETUP ─────────────────────────────────────────────────────────
client    = get_influx_client()
query_api = client.query_api()
opts = WriteOptions(
    write_type="batching",
    batch_size=500,
    flush_interval=5_000,      # flush every 5 seconds
    jitter_interval=2_000,
    max_retries=3,
    max_close_wait=2_000       # wait up to 2 seconds when closing
)
write_api = client.write_api(write_options=opts)

# ─── STEP 1: GET ALL SYMBOLS ───────────────────────────────────────────────
def fetch_symbols():
    """Stream all distinct symbols from symbol_metadata in Influx."""
    flux = f'''
      import "influxdata/influxdb/schema"
      schema.tagValues(
        bucket: "{INFLUX_BUCKET}",
        tag: "symbol"
      )
    '''
    # Each item from query_stream is already a FluxRecord
    for rec in query_api.query_stream(flux):
        yield rec.get_value()

# ─── STEP 2: PULL & PIVOT CLOSE SERIES ────────────────────────────────────
def fetch_close_pivot(window: str) -> pd.DataFrame:
    flux = f'''
    from(bucket:"{INFLUX_BUCKET}")
      |> range(start: -{window})
      |> filter(fn: (r) => r["_measurement"] == "candles" and r["_field"] == "close")
      |> keep(columns: ["_time", "symbol", "_value"])        // ← only these three!
      |> pivot(rowKey: ["_time"], columnKey: ["symbol"], valueColumn: "_value")
      |> fill(usePrevious: true)
    '''
    df = query_api.query_data_frame(flux)

    # Safely set the time index:
    if "_time" in df.columns:
        df = df.set_index("_time")
    elif "time" in df.columns:
        df = df.set_index("time")

    return df  # now purely numeric symbol columns



# ─── STEP 3: COMPUTE & WRITE CORRELATIONS ──────────────────────────────────
def compute_and_write_correlations(df: pd.DataFrame):
    now = datetime.now(timezone.utc).isoformat()
    corr = df.corr()  # pandas computes Pearson by default

    buffer = []
    for s1, s2 in combinations(corr.columns, 2):
        val = corr.at[s1, s2]
        pt = (
            Point("correlation_snapshot")
            .tag("pair", f"{s1}-{s2}")
            .field("correlation", float(val))
            .time(now, WritePrecision.S)
        )
        buffer.append(pt)
        if len(buffer) >= CHUNK_SIZE:
            write_api.write(bucket=INFLUX_BUCKET, record=buffer)
            buffer.clear()

    # flush remaining
    if buffer:
        write_api.write(bucket=INFLUX_BUCKET, record=buffer)

# ─── STEP 4: GENERATE & WRITE KEYWORD METADATA ────────────────────────────
_coin_list = None
def load_coins():
    global _coin_list
    if _coin_list is None:
        _coin_list = requests.get(
            "https://api.coingecko.com/api/v3/coins/list", timeout=10
        ).json()
    return _coin_list

def generate_keywords(symbol: str):
    base = symbol.rstrip("USDTBUSD")
    name = base.capitalize()
    for c in load_coins():
        if c["symbol"].upper() == base.upper():
            name = c["name"]
            break
    kws = [name, base.upper(), f"{base.upper()}/USD", f"{name} price", f"{name} news"]
    return kws + COMMON_BUZZWORDS

def write_symbol_metadata(symbols):
    now = datetime.now(timezone.utc).isoformat()
    buffer = []
    
    with ThreadPoolExecutor(max_workers=8) as pool:
        for sym, kws in zip(symbols, pool.map(generate_keywords, symbols)):
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

    if buffer:
        write_api.write(bucket=INFLUX_BUCKET, record=buffer)

# ─── MAIN ENTRYPOINT ───────────────────────────────────────────────────────
def main():
    # 1) Grab symbols
    symbols = list(fetch_symbols())
    print(f"[+] Found {len(symbols)} symbols")

    # 2) Pull & pivot close series
    df = fetch_close_pivot(CORR_WINDOW)
    print(f"[+] Retrieved close data shape {df.shape}")
    # after your existing pivot…
    # keep only columns whose dtype is numeric
    numeric_cols = df.select_dtypes(include="number").columns
    df = df[numeric_cols]


    # 3) Compute & write pairwise correlations
    compute_and_write_correlations(df)
    print("[+] Correlation snapshots written")

    # 4) Generate & write symbol keyword metadata
    write_symbol_metadata(symbols)
    print("[+] Symbol metadata written")

        # ─── CLEANUP ─────────────────────────────
    write_api.close()   # stop the background flush thread
    client.close()      # close the HTTP client

if __name__ == "__main__":
    main()
