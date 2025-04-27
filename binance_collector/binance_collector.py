# binance_collector.py
import os
import time
import requests
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from utils.symbol_manager import get_active_binance_symbols
from fastapi import FastAPI
from utils.influx_writer import async_write_batches, get_influx_client
import asyncio
from fastapi import BackgroundTasks, Query

load_dotenv()


INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

app=FastAPI()

BASE_URL = "https://api.binance.com"
OHLC_ENDPOINT = "/api/v3/klines"

SYMBOLS = get_active_binance_symbols()

client = get_influx_client()
write_api = client.write_api(write_options=SYNCHRONOUS)

def fetch_ohlcv(symbol, interval="1m", start=None, end=None, limit=1000):
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start:
        params["startTime"] = int(start.timestamp() * 1000)
    if end:
        params["endTime"] = int(end.timestamp() * 1000)

    try:
        r = requests.get(BASE_URL + OHLC_ENDPOINT, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] {symbol} fetch failed: {e}")
        return []

async def store_to_influx(symbol, ohlcv_data):
    points = []
    
    for candle in ohlcv_data:
        ts = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc).isoformat()
        
        point = (
            Point("candles")
            .tag("symbol", symbol)
            .field("open", float(candle[1]))
            .field("high", float(candle[2]))
            .field("low",  float(candle[3]))
            .field("close", float(candle[4]))
            .field("volume", float(candle[5]))
            .time(ts, WritePrecision.S)
        )
        
        points.append(point)
        print(f"[✓] {symbol} @ {ts} | {datetime.now().isoformat()}")

    await async_write_batches(data_points=points, bucket_name="Sentry")
    
    del points
    await asyncio.sleep(0.01)  # Let garbage collector catch up
    
def sync_store_to_influx(symbol, ohlcv_data):
    points = []
    for candle in ohlcv_data:
        ts = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc).isoformat()

        point = (
            Point("candles")
            .tag("symbol", symbol)
            .field("open", float(candle[1]))
            .field("high", float(candle[2]))
            .field("low", float(candle[3]))
            .field("close", float(candle[4]))
            .field("volume", float(candle[5]))
            .time(ts, WritePrecision.S)
        )
        points.append(point)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)



@app.get("/")
def home():
    return{"status":"ok","msg":"Binance Collector"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/live")
def run_live():
    for symbol in SYMBOLS:
        data = fetch_ohlcv(symbol, limit=1000)
        if data:
            # Use direct sync write here
            sync_store_to_influx(symbol, data)
        else:
            print(f"[!] No data received for {symbol}")

    print("[✓] Binance live snapshot written successfully.")



@app.get("/backfill")
async def run_backfill(background_tasks: BackgroundTasks, days: int = Query(7, description="Number of days to backfill")):
    background_tasks.add_task(run_backfill_worker, days)
    return {"message": "Backfill started in background!"}

async def run_backfill_worker(days: int):
    interval_minutes = 1
    delta = timedelta(minutes=interval_minutes)
    tasks = []

    for symbol in SYMBOLS:
        end = datetime.now()
        start = end - timedelta(days=int(days))

        while start < end:
            next_end = min(start + delta * 1000, end)
            candles = fetch_ohlcv(symbol, start=start, end=next_end)

            if candles:
                tasks.append(store_to_influx(symbol, candles))

            start += delta * len(candles) if candles else delta * 1000
            await asyncio.sleep(0.01)

    if tasks:
        await asyncio.gather(*tasks)

    print("[✓] Binance backfill completed successfully.")
