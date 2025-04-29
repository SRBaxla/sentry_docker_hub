# binance_collector.py
import os
import time
import math
import requests
import asyncio
from datetime import datetime, timedelta, timezone
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from utils.symbol_manager import get_active_binance_symbols
from fastapi import FastAPI, Query
from utils.influx_writer import async_write_batches, get_influx_client

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

# Shared Influx client (not used directly here)
_client = get_influx_client()
        
async def backfill_all_symbols(
    days: int = 7,
    interval: str = "1m",
    page_limit: int = 1000,
    concurrency: int = 5
):
    """
    Backfill historical candles for all active symbols in parallel.
    Adds per-symbol ETA logging.
    """
    symbols         = get_active_binance_symbols()
    end_time        = datetime.now(timezone.utc)
    start_time      = end_time - timedelta(days=days)
    total_minutes   = days * 24 * 60
    pages_per_symbol= math.ceil(total_minutes / page_limit)
    total_pages     = pages_per_symbol * len(symbols)

    sem          = asyncio.Semaphore(concurrency)
    page_counter = 0
    counter_lock = asyncio.Lock()
    start_all    = time.perf_counter()
    

    async def _backfill_symbol(symbol: str):
        async with sem:
            nonlocal page_counter
            cursor = start_time
            page_index = 0
            symbol_start = time.perf_counter()

            while cursor < end_time:
                page_index += 1
                next_cursor = min(cursor + timedelta(minutes=page_limit), end_time)

                # Fetch one page of raw klines
                candles = fetch_ohlcv(
                    symbol,
                    interval=interval,
                    start=cursor,
                    end=next_cursor,
                    limit=page_limit
                )
                if not candles:
                    break

                # Write Points to InfluxDB
                await store_to_influx(symbol, candles)

                # # ETA logging
                # elapsed = time.perf_counter() - symbol_start
                # eta = elapsed / page_index * (pages_per_symbol - page_index)
                # print(f"[->] {symbol}: page {page_index}/{pages_per_symbol}, ETA {eta:.1f}s")
                # update overall counter & compute ETAs
                async with counter_lock:
                    page_counter += 1
                    elapsed_all = time.perf_counter() - start_all
                    eta_all     = (elapsed_all / page_counter)*(total_pages - page_counter)

                elapsed_sym = time.perf_counter() - symbol_start
                eta_sym     = (elapsed_sym / page_index)*(pages_per_symbol - page_index)
                print(
                  f"[->] {symbol}: page {page_index}/{pages_per_symbol}, "
                  f"ETA {eta_sym:.1f}s | overall {page_counter}/{total_pages}, ETA {eta_all:.1f}s"
                )
                # Advance past last candle timestamp
                last_ts_ms = candles[-1][0]
                cursor = datetime.fromtimestamp(last_ts_ms/1000, tz=timezone.utc) + timedelta(milliseconds=1)

    await asyncio.gather(*( _backfill_symbol(sym) for sym in symbols ))



async def live_backfill_loop(days: int, concurrency: int):
    # 1) initial backfill
    await backfill_all_symbols(days=days, concurrency=concurrency)

    symbols = get_active_binance_symbols()
    while True:
        # compute the 1-min window that just closed
        now        = datetime.now(timezone.utc)
        last_min   = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        next_min   = last_min + timedelta(minutes=1)

        # 2) fetch that last‐minute candle REST-style for each symbol
        tasks = []
        for sym in symbols:
            candles = fetch_ohlcv(
                sym,
                interval="1m",
                start=last_min,
                end=next_min,
                limit=1
            )
            if candles:
                tasks.append(store_to_influx(sym, candles))

        # write them in parallel
        if tasks:
            await asyncio.gather(*tasks)

        # 3) wait until just after the next minute closes
        sleep_secs = (next_min + timedelta(seconds=1) - datetime.now(timezone.utc)).total_seconds()
        await asyncio.sleep(max(sleep_secs, 0))

def fetch_ohlcv(symbol, interval="1m", start=None, end=None, limit=1000):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
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
    await async_write_batches(data_points=points, bucket_name=INFLUX_BUCKET)
    await asyncio.sleep(0.01)  # slight pause for GC

async def run_backfill_worker(days: int):
    print(f"[BACKFILL] worker kicked off for last {days} days")
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



@app.get("/backfill-all")
async def backfill_route(
    days: int = Query(7, ge=1),
    concurrency: int = Query(5, ge=1, le=20)
):
    await backfill_all_symbols(days=days, concurrency=concurrency)
    return {"message": f"Backfilled {days} days for {len(get_active_binance_symbols())} symbols."}

@app.get("/")
def home():
    return {"status": "ok", "msg": "Binance Collector"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/live")
def run_live():
    for symbol in SYMBOLS:
        data = fetch_ohlcv(symbol, limit=1000)
        if data:
            sync_store_to_influx(symbol, data)
        else:
            print(f"[!] No data received for {symbol}")
    print("[✓] Binance live snapshot written successfully.")

@app.get("/backfill-sync")
async def backfill_sync(days: int = Query(1)):
    print("[SYNC] starting blocking backfill")
    await run_backfill_worker(days)
    print("[SYNC] blocking backfill complete")
    return {"message": "sync backfill done"}


@app.get("/live-backfill")
async def start_live_backfill(
    days: int = Query(7, ge=1, description="Days of history to seed"),
    concurrency: int = Query(5, ge=1, le=20, description="Parallel symbols")
):
    """
    Kick off a combined backfill + live loop:
    1) backfill last `days` days of 1m candles
    2) then every minute fetch the just-closed 1m candle
       and write it (gap-free) into InfluxDB
    """
    # fire-and-forget
    asyncio.create_task(live_backfill_loop(days, concurrency))
    return {
        "message": (
            f"Started live+backfill loop: "
            f"{days}d history, {concurrency} concurrent symbols."
        )
    }
# Entry point
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("binance_collector:app", host="0.0.0.0", port=8000, reload=True)
