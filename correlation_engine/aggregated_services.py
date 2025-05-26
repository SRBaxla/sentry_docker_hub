import os
import pandas as pd
import requests
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
from typing import List, Dict, Any, Tuple
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
META_BUCKET = os.getenv("META_BUCKET", "CoinMetadata")
DURATION = os.getenv("CORR_WINDOW", "-60m")
PAIR_CHUNK_SIZE = int(os.getenv("PAIR_CHUNK_SIZE", 100))
SYMBOLS_CSV = os.getenv("SYMBOLS_CSV", "symbols.csv")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("coin-correlation-api")

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

app = FastAPI()

def load_symbol_mapping() -> pd.DataFrame:
    try:
        df = pd.read_csv(SYMBOLS_CSV)
        logger.info(f"Loaded {len(df)} symbol mappings from {SYMBOLS_CSV}.")
        return df
    except Exception as e:
        logger.error(f"Failed to load symbol mapping CSV: {e}")
        return pd.DataFrame(columns=["symbol", "base_coin", "quote_coin"])

def fetch_symbols_from_influx() -> List[str]:
    flux = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
        bucket: "{INFLUX_BUCKET}",
        tag: "symbol"
    )
    '''
    try:
        symbols = [rec.get_value() for rec in query_api.query_stream(flux)]
        logger.info(f"Fetched {len(symbols)} symbols from InfluxDB.")
        return symbols
    except Exception as e:
        logger.error(f"Error fetching symbols: {e}")
        return []

def load_candles(symbol: str, duration: str = DURATION) -> pd.DataFrame:
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
    |> range(start: {duration})
    |> filter(fn: (r) => r["_measurement"] == "candles")
    |> filter(fn: (r) => r["symbol"] == "{symbol}")
    |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    try:
        df = query_api.query_data_frame(flux)
        if df.empty:
            return pd.DataFrame()
        df = df.set_index(pd.to_datetime(df["_time"]))
        return df[["open", "high", "low", "close", "volume"]]
    except Exception as e:
        logger.error(f"Error loading OHLCV for {symbol}: {e}")
        return pd.DataFrame()

def fetch_coingecko_metadata() -> Dict[str, str]:
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

def compute_correlations(
    symbols_df: pd.DataFrame,
    chunk_size: int = PAIR_CHUNK_SIZE
) -> Tuple[List[Dict[str, Any]], List[str]]:
    influx_symbols = fetch_symbols_from_influx()
    symbols_df = symbols_df[symbols_df['symbol'].isin(influx_symbols)]

    results = []
    all_symbols = []

    for quote, group in symbols_df.groupby('quote_coin'):
        group_symbols = group['symbol'].tolist()
        if len(group_symbols) < 2:
            continue

        candles = {sym: load_candles(sym) for sym in group_symbols}
        candles = {k: v for k, v in candles.items() if not v.empty}
        valid_symbols = list(candles.keys())
        if len(valid_symbols) < 2:
            continue

        all_symbols.extend(valid_symbols)
        pairs = [(a, b) for i, a in enumerate(valid_symbols) for b in valid_symbols[i+1:]]

        for chunk_start in range(0, len(pairs), chunk_size):
            chunk = pairs[chunk_start:chunk_start+chunk_size]
            for a, b in chunk:
                merged = pd.merge(
                    candles[a].add_prefix(f"{a}_"),
                    candles[b].add_prefix(f"{b}_"),
                    left_index=True, right_index=True, how="inner"
                )
                if merged.empty:
                    continue

                price_corr = merged[f"{a}_close"].corr(merged[f"{b}_close"])
                volume_corr = merged[f"{a}_volume"].corr(merged[f"{b}_volume"])

                a_candles = merged[[f"{a}_open", f"{a}_high", f"{a}_low", f"{a}_close", f"{a}_volume"]].rename(
                    columns=lambda x: x.replace(f"{a}_", "")
                ).reset_index().to_dict(orient="records")
                b_candles = merged[[f"{b}_open", f"{b}_high", f"{b}_low", f"{b}_close", f"{b}_volume"]].rename(
                    columns=lambda x: x.replace(f"{b}_", "")
                ).reset_index().to_dict(orient="records")

                results.append({
                    "pair": f"{a}-{b}",
                    "quote_coin": quote,
                    "price_corr": round(float(price_corr), 4),
                    "volume_corr": round(float(volume_corr), 4),
                    "a_symbol": a,
                    "b_symbol": b,
                    "a_candles": a_candles,
                    "b_candles": b_candles
                })
    return results, list(set(all_symbols))

def write_correlations_to_influx(results: List[Dict[str, Any]]):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []
    for result in results:
        point = (
            Point("coin_correlation")
            .tag("pair", result["pair"])
            .tag("a_symbol", result["a_symbol"])
            .tag("b_symbol", result["b_symbol"])
            .tag("quote_coin", result["quote_coin"])
            .field("price_corr", result["price_corr"])
            .field("volume_corr", result["volume_corr"])
        )
        points.append(point)
        if len(points) >= 500:
            write_api.write(bucket=CORR_BUCKET, org=INFLUX_ORG, record=points)
            points = []
    if points:
        write_api.write(bucket=CORR_BUCKET, org=INFLUX_ORG, record=points)
    write_api.close()
    logger.info(f"Wrote {len(results)} correlations to bucket '{CORR_BUCKET}'.")

def write_coin_metadata_to_influx(symbol: str, cg_metadata: Dict[str, str]):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    keywords = generate_keywords(symbol, cg_metadata)
    name = cg_metadata.get(symbol.upper(), symbol)
    point = (
        Point("coin_metadata")
        .tag("symbol", symbol)
        .tag("name", name)
        .field("keywords", ", ".join(keywords))
    )
    write_api.write(bucket=META_BUCKET, org=INFLUX_ORG, record=point)
    write_api.close()
    logger.info(f"Wrote metadata for {symbol} to bucket '{META_BUCKET}'.")

async def periodic_metadata_generator():
    symbols_df = load_symbol_mapping()
    cg_metadata = fetch_coingecko_metadata()
    influx_symbols = fetch_symbols_from_influx()
    symbols = symbols_df[symbols_df['symbol'].isin(influx_symbols)]['symbol'].unique().tolist()
    while True:
        logger.info("Starting periodic metadata generation...")
        tasks = []
        for symbol in symbols:
            loop = asyncio.get_event_loop()
            # Run each metadata write in parallel
            tasks.append(loop.run_in_executor(None, write_coin_metadata_to_influx, symbol, cg_metadata))
        await asyncio.gather(*tasks)
        logger.info("Completed one round of metadata generation. Sleeping 5 minutes.")
        await asyncio.sleep(300)  # 5 minutes

@app.on_event("startup")
async def startup_event():
    # Start the periodic metadata generator in the background
    asyncio.create_task(periodic_metadata_generator())

@app.get("/correlations")
def get_correlations(background_tasks: BackgroundTasks):
    symbols_df = load_symbol_mapping()
    results, symbols = compute_correlations(symbols_df)
    background_tasks.add_task(write_correlations_to_influx, results)
    return {"count": len(results), "results": results[:10]}

@app.get("/")
def root():
    return {"message": "Coin correlation API is running."}
