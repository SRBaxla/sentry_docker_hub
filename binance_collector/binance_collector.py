# binance_collector.py
import os
import time
import math
import requests
import asyncio
from datetime import datetime, timedelta, timezone
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from dotenv import load_dotenv
from utils.symbol_manager import get_active_binance_symbols
from fastapi import FastAPI, Query
from utils.influx_writer import async_write_batches, get_influx_client
from contextlib import asynccontextmanager
from influxdb_client.client.query_api import QueryApi
from datetime import datetime, timedelta, timezone
from influxdb_client.rest import ApiException
import urllib3
import aiohttp
import json
from typing import List, Optional, Tuple, Dict

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
BACKFILL_CONCURRENCY = 5
RETRY_DELAY = 5

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Read your env-driven settings once
    days = int(os.getenv("BACKFILL_DAYS", "30"))
    # Kick off tasks in background
    asyncio.create_task(run_all(days))
    yield

app = FastAPI(lifespan=lifespan)

BASE_URL = "https://api.binance.com"
OHLC_ENDPOINT = "/api/v3/klines"

SYMBOLS = get_active_binance_symbols()

client = get_influx_client()
write_api = client.write_api(write_options=ASYNCHRONOUS)
query_api: QueryApi = client.query_api()

live_candle_buffers: Dict[str, List[dict]] = {}
last_minute_timestamp: Dict[str, Optional[int]] = {}

async def process_websocket_message(symbol: str, message: str):
    """Processes incoming websocket tick data."""
    global live_candle_buffers, last_minute_timestamp
    try:
        data = json.loads(message)
        if 't' in data and 'p' in data and 'q' in data:
            timestamp_ms = data['t']
            price = float(data['p'])
            volume = float(data['q'])
            current_minute_ts = timestamp_ms // 60000 * 60000  # Floor to the nearest minute

            if symbol not in live_candle_buffers:
                live_candle_buffers[symbol] = []
                last_minute_timestamp[symbol] = None

            if last_minute_timestamp[symbol] is not None and current_minute_ts > last_minute_timestamp[symbol]:
                # Aggregate and write the previous minute's data
                if live_candle_buffers[symbol]:
                    open_price = live_candle_buffers[symbol][0]['price']
                    close_price = live_candle_buffers[symbol][-1]['price']
                    high_price = max(tick['price'] for tick in live_candle_buffers[symbol])
                    low_price = min(tick['price'] for tick in live_candle_buffers[symbol])
                    total_volume = sum(tick['volume'] for tick in live_candle_buffers[symbol])
                    close_time = live_candle_buffers[symbol][-1]['timestamp']

                    point = (
                        Point("candles")
                        .tag("symbol", symbol)
                        .time(datetime.datetime.fromtimestamp(last_minute_timestamp[symbol] / 1000, tz=datetime.timezone.utc))
                        .field("open", open_price)
                        .field("high", high_price)
                        .field("low", low_price)
                        .field("close", close_price)
                        .field("volume", total_volume)
                        .field("close_time", close_time) # You might need to adjust this
                    )
                    await async_write_batches(data_points=[point], bucket_name=INFLUX_BUCKET)
                    print(f"[WS] Wrote 1-minute candle for {symbol} at {datetime.datetime.fromtimestamp(last_minute_timestamp[symbol] / 1000)}")
                    live_candle_buffers[symbol] = []  # Clear buffer for the new minute

            live_candle_buffers[symbol].append({'timestamp': timestamp_ms, 'price': price, 'volume': volume})
            last_minute_timestamp[symbol] = current_minute_ts

    except json.JSONDecodeError as e:
        print(f"[ERROR] Error decoding websocket message for {symbol}: {e}")
    except Exception as e:
        print(f"[ERROR] Error processing websocket message for {symbol}: {e}")

async def websocket_listener(symbol: str):
    """Listens to the 1-second tick websocket for a specific symbol."""
    ws_url = f"{BINANCE_WEBSOCKET_URL}/{symbol.lower()}@trade"
    async with aiohttp.ClientSession() as session:
        while True:  # Keep trying to connect
            try:
                async with session.ws_connect(ws_url) as ws:
                    print(f"[WS] Websocket connected for {symbol}")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await process_websocket_message(symbol, msg.data)
                        elif msg.type == aiohttp.WSMsgType.CLOSED or msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"[WS] Websocket disconnected for {symbol}: {ws.close_code}, {ws.exception}")
                            break
            except aiohttp.ClientConnectionError as e:
                print(f"[ERROR] Connection error for {symbol}: {e}")
            except Exception as e:
                print(f"[ERROR] Unexpected error for {symbol}: {e}")
            print(f"[WS] Attempting to reconnect to websocket for {symbol} in 5 seconds...")
            await asyncio.sleep(5)

async def fetch_ohlcv(
    symbol: str,
    interval: str = "1m",
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 1000
) -> Optional[List[dict]]:
    """Fetches OHLCV data from Binance API with retries."""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time:
        params["startTime"] = start_time
    if end_time:
        params["endTime"] = end_time

    for attempt in range(3):  # Retry up to 3 times
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BASE_URL + OHLC_ENDPOINT, params=params) as response:
                    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
                    data = await response.json()
                    return data
        except aiohttp.ClientError as e:
            print(f"[ERROR] API request failed for {symbol} (attempt {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        except json.JSONDecodeError as e:
            print(f"[ERROR] Error decoding JSON for {symbol}: {e}")
            return None
        except Exception as e:
            print(f"[ERROR] Unexpected error while fetching {symbol}: {e}")
            return None
    return None

async def store_to_influx(symbol: str, ohlcv_data: List[list]):
    """Stores OHLCV data to InfluxDB."""
    points = []
    for candle in ohlcv_data:
        point = (
            Point("candles")
            .tag("symbol", symbol)
            .time(datetime.datetime.fromtimestamp(candle[0] / 1000, tz=datetime.timezone.utc))
            .field("open", float(candle[1]))
            .field("high", float(candle[2]))
            .field("low", float(candle[3]))
            .field("close", float(candle[4]))
            .field("volume", float(candle[5]))
            .field("close_time", int(candle[6]))
            .field("quote_asset_volume", float(candle[7]))
            .field("number_of_trades", int(candle[8]))
            .field("taker_buy_base_asset_volume", float(candle[9]))
            .field("taker_buy_quote_asset_volume", float(candle[10]))
        )
        points.append(point)

    try:
        await async_write_batches(data_points=points, bucket_name=INFLUX_BUCKET)
        print(f"[API] Stored {len(points)} candles for {symbol} to InfluxDB.")
    except Exception as e:
        print(f"[ERROR] Error writing to InfluxDB for {symbol}: {e}")

def get_missing_minutes(
    symbol: str,
    query_api: QueryApi,
    bucket: str,
    days: int
) -> list[datetime]:
    """
    Return a sorted list of 1-minute interval start times that have zero points
    in the last `days` days for this symbol.
    """
    flux = f'''
    from(bucket: "{bucket}")
      |> range(start: -{days}d)
      |> filter(fn: (r) =>
           r._measurement == "candles"
        and r.symbol == "{symbol}"
        and r._field == "close")
      |> aggregateWindow(
           every: 1m,
           fn: count,
           createEmpty: true
        )
      |> filter(fn: (r) => r._value == 0)
      |> keep(columns: ["_time"])
    '''
    missing = []
    tables = query_api.query(flux)
    for table in tables:
        for rec in table.records:
            missing.append(rec.get_time().replace(tzinfo=timezone.utc))
    return sorted(missing)

def coalesce_ranges(times: list[datetime]) -> list[tuple[datetime, datetime]]:
    """
    Given sorted minute-start times, coalesce contiguous minutes into (start,end) ranges.
    Each end is exclusive.
    """
    if not times:
        return []
    ranges = []
    start = prev = times[0]
    for t in times[1:]:
        if t == prev + timedelta(minutes=1):
            prev = t
        else:
            ranges.append((start, prev + timedelta(minutes=1)))
            start = prev = t
    ranges.append((start, prev + timedelta(minutes=1)))
    return ranges

async def backfill_historical_data(symbol: str, start_time: datetime, end_time: datetime):
    """Backfills historical 1-minute OHLCV data for a given symbol and time range."""

    print(f"[BACKFILL] Starting backfill for {symbol} from {start_time} to {end_time}")

    # 1. Ensure both start_time and end_time are timezone-aware (UTC)
    if start_time.tzinfo is None or start_time.utcoffset() is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    if end_time.tzinfo is None or end_time.utcoffset() is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    current_start = start_time  # Initialize current_start with timezone-aware start_time

    while current_start < end_time:
        next_interval = current_start + timedelta(minutes=1000)
        # 2. Ensure fetch_end is also timezone-aware (UTC)
        fetch_end = min(next_interval, end_time)  # min() should now work correctly

        # 3. Log the datetimes we're using (for debugging)
        print(f"  [DEBUG] current_start: {current_start}, fetch_end: {fetch_end}, end_time: {end_time}")

        raw_data = await fetch_ohlcv(
            symbol=symbol,
            interval="1m",
            start_time=int(current_start.timestamp() * 1000),
            end_time=int(fetch_end.timestamp() * 1000),
        )

        if raw_data:
            await store_to_influx(symbol, raw_data)

        current_start = next_interval  # Move to the next interval
        await asyncio.sleep(1)

    print(f"[BACKFILL] Backfill for {symbol} from {start_time} to {end_time} complete.")
    
async def run_backfiller(symbols: List[str], backfill_start_str: str = "2025-04-08T00:00:00Z"):
    """Runs the initial backfilling for specified symbols."""
    start_time_utc = datetime.fromisoformat(backfill_start_str.replace("Z", "+00:00"))
    now_utc = datetime.utcnow()
    backfill_tasks = [asyncio.create_task(backfill_historical_data(symbol, start_time_utc, now_utc)) for symbol in symbols]
    await asyncio.gather(*backfill_tasks)
    print("[BACKFILL] Initial backfill completed.")

async def periodic_backfill_check(symbols: List[str], backfill_lookback: timedelta = timedelta(hours=1)):
    """Periodically checks for and backfills missing data."""
    while True:
        now_utc = datetime.datetime.utcnow()
        backfill_start = now_utc - backfill_lookback
        print(f"[BACKFILL] Starting periodic backfill check for symbols: {symbols}, looking back {backfill_lookback}.")
        backfill_tasks = []
        for symbol in symbols:
            missing_ranges = [
              (m, m + timedelta(minutes=1))
              for m in get_missing_minutes(symbol, query_api, INFLUX_BUCKET, 1)
            ]
            coalesced_ranges = coalesce_ranges(missing_ranges)
            print(f"[BACKFILL] Found {len(missing_ranges)} missing intervals for {symbol}, coalesced into {len(coalesced_ranges)} ranges.")
            for start, end in coalesced_ranges:
                start_dt = start #datetime.datetime.fromtimestamp(start / 1000, tz=datetime.timezone.utc)
                end_dt = end #datetime.datetime.fromtimestamp(end / 1000, tz=datetime.timezone.utc)
                print(f"[BACKFILL] Backfilling {symbol} from {start_dt} to {end_dt}")
                backfill_tasks.append(asyncio.create_task(backfill_historical_data(symbol, start_dt, end_dt)))
                await asyncio.sleep(1) # Be mindful of rate limits

        if backfill_tasks:
            await asyncio.gather(*backfill_tasks)
        print("[BACKFILL] Periodic backfill check completed. Waiting for the next cycle.")
        await asyncio.sleep(60 * 60) # Check every hour

async def run_all(days: int):
    """Runs all collectors: websockets and backfilling."""
    await run_backfiller(SYMBOLS, backfill_start_str="2024-01-01T00:00:00Z")
    websocket_tasks = [asyncio.create_task(websocket_listener(symbol)) for symbol in SYMBOLS]
    periodic_backfill_task = asyncio.create_task(periodic_backfill_check(SYMBOLS, timedelta(days=1)))

    await asyncio.gather(*websocket_tasks)
    await periodic_backfill_task

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)