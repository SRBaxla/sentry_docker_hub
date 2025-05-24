import os
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from typing import List, Dict, Any
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import logging

# ---- CONFIG ----
load_dotenv()
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")
CORR_BUCKET = os.getenv("CORR_BUCKET", "CoinCorrelations")
DURATION = os.getenv("CORR_WINDOW", "-60m")
PAIR_CHUNK_SIZE = int(os.getenv("PAIR_CHUNK_SIZE", 100))  # Number of pairs processed per chunk

# ---- LOGGING ----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("coin-correlation-api")

# ---- INFLUXDB SETUP ----
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

app = FastAPI()

def fetch_symbols() -> List[str]:
    flux = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
        bucket: "{INFLUX_BUCKET}",
        tag: "symbol"
    )
    '''
    try:
        symbols = [rec.get_value() for rec in query_api.query_stream(flux)]
        logger.info(f"Fetched {len(symbols)} symbols.")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        return []

def load_feature(symbol: str, field: str, duration: str = DURATION) -> pd.Series:
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: {duration})
    |> filter(fn: (r) => r["_measurement"] == "candles")
    |> filter(fn: (r) => r["_field"] == "{field}")
    |> filter(fn: (r) => r["symbol"] == "{symbol}")
    |> aggregateWindow(every: 1m, fn: mean)
    |> yield(name: "mean")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        df = query_api.query_data_frame(flux)
        if df.empty:
            return pd.Series(dtype=float)
        return pd.Series(df["_value"].values, index=pd.to_datetime(df["_time"]))
    except Exception as e:
        logger.error(f"Error querying InfluxDB for {symbol} {field}: {e}")
        return pd.Series(dtype=float)

def fetch_coingecko_metadata() -> Dict[str, str]:
    """Fetch all CoinGecko coins once for efficient metadata lookup."""
    try:
        coins = requests.get("https://api.coingecko.com/api/v3/coins/list", timeout=10).json()
        return {c["symbol"].upper(): c.get("name", "") for c in coins if isinstance(c, dict)}
    except Exception as e:
        logger.warning(f"Could not fetch CoinGecko metadata: {e}")
        return {}

def generate_keywords(symbol: str, cg_metadata: Dict[str, str]) -> List[str]:
    COMMON_BUZZWORDS = ["ETF", "DeFi", "SEC", "Bullish", "Bearish", "Pump", "Crash", "Rally", "Halving"]
    base = symbol.rstrip("USDTBUSD")
    name = cg_metadata.get(base.upper(), base.capitalize())
    kws = [name, base.upper(), f"{base.upper()}/USD", f"{name} price", f"{name} news"]
    return kws + COMMON_BUZZWORDS

def compute_correlations() -> (List[Dict[str, Any]], List[str], Dict[str, list], Dict[str, str]):
    symbols = fetch_symbols()
    if not symbols:
        return [], [], {}, {}

    cg_metadata = fetch_coingecko_metadata()

    price_df = pd.DataFrame({sym: load_feature(sym, "close") for sym in symbols}).dropna(axis=1, how='all')
    volume_df = pd.DataFrame({sym: load_feature(sym, "volume") for sym in symbols}).dropna(axis=1, how='all')
    keywords = {sym: generate_keywords(sym, cg_metadata) for sym in symbols}

    valid_symbols = list(set(price_df.columns) & set(volume_df.columns))
    price_df = price_df[valid_symbols]
    volume_df = volume_df[valid_symbols]

    results = []
    pairs = [(a, b) for i, a in enumerate(valid_symbols) for b in valid_symbols[i+1:]]
    for a, b in pairs:
        df = pd.DataFrame({
            f"{a}_price": price_df[a],
            f"{b}_price": price_df[b],
            f"{a}_volume": volume_df[a],
            f"{b}_volume": volume_df[b],
        }).dropna()
        if df.empty:
            continue
        price_corr = df[f"{a}_price"].corr(df[f"{b}_price"])
        volume_corr = df[f"{a}_volume"].corr(df[f"{b}_volume"])
        a_candles = [{"time": t.isoformat(), "price": p, "volume": v}
                     for t, p, v in zip(df.index, df[f"{a}_price"], df[f"{a}_volume"])]
        b_candles = [{"time": t.isoformat(), "price": p, "volume": v}
                     for t, p, v in zip(df.index, df[f"{b}_price"], df[f"{b}_volume"])]
        results.append({
            "pair": f"{a}-{b}",
            "price_corr": round(float(price_corr), 4),
            "volume_corr": round(float(volume_corr), 4),
            "a_symbol": a,
            "b_symbol": b,
            "a_keywords": keywords[a],
            "b_keywords": keywords[b],
            "a_volume": df[f"{a}_volume"].tolist(),
            "b_volume": df[f"{b}_volume"].tolist(),
            "a_candles": a_candles,
            "b_candles": b_candles
        })
    return results, valid_symbols, keywords, cg_metadata

def write_correlations_to_influx(results: List[Dict[str, Any]]):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []
    for result in results:
        point = (
            Point("coin_correlation")
            .tag("pair", result["pair"])
            .tag("a_symbol", result["a_symbol"])
            .tag("b_symbol", result["b_symbol"])
            .field("price_corr", result["price_corr"])
            .field("volume_corr", result["volume_corr"])
        )
        points.append(point)
        # Write in batches of 500 for memory/resource efficiency
        if len(points) >= 500:
            write_api.write(bucket=CORR_BUCKET, org=INFLUX_ORG, record=points)
            points = []
    if points:
        write_api.write(bucket=CORR_BUCKET, org=INFLUX_ORG, record=points)
    write_api.close()
    logger.info(f"Wrote {len(results)} correlations to bucket '{CORR_BUCKET}'.")
    
def write_coin_metadata_to_influx(symbols: List[str], keywords: Dict[str, list], cg_metadata: Dict[str, str]):
    from influxdb_client import Point, WritePrecision
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []
    for sym in symbols:
        name = cg_metadata.get(sym.upper(), sym)
        point = (
            Point("coin_metadata")
            .tag("symbol", sym)
            .tag("name", name)
            .field("keywords", ", ".join(keywords[sym]))
        )
        points.append(point)
        if len(points) >= 500:
            write_api.write(bucket=CORR_BUCKET, org=INFLUX_ORG, record=points)
            points = []
    if points:
        write_api.write(bucket=CORR_BUCKET, org=INFLUX_ORG, record=points)
    write_api.close()
    logger.info(f"Wrote metadata for {len(symbols)} coins to bucket '{CORR_BUCKET}'.")

    
    
@app.get("/correlations")
def get_correlations(background_tasks: BackgroundTasks):
    results, symbols, keywords, cg_metadata = compute_correlations()
    background_tasks.add_task(write_correlations_to_influx, results)
    background_tasks.add_task(write_coin_metadata_to_influx, symbols, keywords, cg_metadata)
    return {"count": len(results), "results": results[:10]}

@app.get("/")
def root():
    return {"message": "Coin correlation API is running."}
