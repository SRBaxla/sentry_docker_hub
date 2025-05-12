import os
import time
import math
import requests
import asyncio
import datetime
import random
import traceback
import json
import urllib3
import aiohttp
from contextlib import asynccontextmanager
from typing import List, Optional, Tuple, Dict

from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
from influxdb_client.rest import ApiException
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from collections import defaultdict

from utils.symbol_manager import get_active_binance_symbols
from utils.influx_writer import async_write_batches, get_influx_client
import logging
import sys
import uvicorn
import copy
from uvicorn.config import LOGGING_CONFIG # Uvicorn's default logging config

# Get a logger for your application. Using a specific name is good practice.
# This logger can be used throughout your application.
logger = logging.getLogger("binance_collector_app")
# Set a default level for your app's logger (can be overridden by handler config)
logger.setLevel(logging.INFO)

# Load environment variables
load_dotenv()

# Settings from the environment or defaults
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
BACKFILL_CONCURRENCY = 5
RETRY_DELAY = 5
background_tasks = []

# Tick buffer
tick_buffer = defaultdict(list)

# Compute OHLC from tick list
def compute_ohlc(ticks):
    prices = [tick['price'] for tick in ticks]
    return {
        'open': prices[0],
        'high': max(prices),
        'low': min(prices),
        'close': prices[-1],
        'volume': sum(tick['volume'] for tick in ticks)
    }

# Live Tick Handler
async def handle_tick(symbol, tick):
    current_minute = datetime.datetime.utcnow().replace(second=0, microsecond=0)
    tick_buffer[(symbol, current_minute)].append(tick)

# Candle Writer
async def candle_writer():
    while True:
        await asyncio.sleep(1)
        now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        expired_keys = [k for k in list(tick_buffer.keys()) if k[1] < now] # Iterate over a copy
        for key in expired_keys:
            symbol, minute = key
            ticks = tick_buffer.pop(key)
            if ticks:
                ohlc = compute_ohlc(ticks)
                point = (
                    Point("candles")
                    .tag("symbol", symbol)
                    .time(minute, write_precision=WritePrecision.S)
                    .field("open", ohlc['open'])
                    .field("high", ohlc['high'])
                    .field("low", ohlc['low'])
                    .field("close", ohlc['close'])
                    .field("volume", ohlc['volume'])
                    .field("close_time", int(minute.timestamp())) # Approximate close time
                )
                await async_write_batches(data_points=[point], bucket_name=INFLUX_BUCKET)
                logger.info(f"[CANDLE_WRITER] Wrote 1-minute candle for {symbol} at {minute}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    days = int(os.getenv("BACKFILL_DAYS", "30"))

    # Start your background tasks
    # backfill_task = asyncio.create_task(run_backfiller(SYMBOLS, days))
    websocket_tasks = [asyncio.create_task(websocket_listener(symbol)) for symbol in SYMBOLS]
    candle_writer_task = asyncio.create_task(candle_writer())

    # background_tasks.append(backfill_task)
    background_tasks.extend(websocket_tasks)
    background_tasks.append(candle_writer_task)

    try:
        yield  # Let the app run
    finally:
        logger.info("[SHUTDOWN] Cancelling background tasks...")
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        logger.info("[SHUTDOWN] All background tasks cancelled.")

app = FastAPI(lifespan=lifespan)

BASE_URL = "https://api.binance.com"
OHLC_ENDPOINT = "/api/v3/klines"
SYMBOLS = get_active_binance_symbols()

client = get_influx_client()
write_api = client.write_api(write_options=ASYNCHRONOUS)
query_api: QueryApi = client.query_api()

async def process_websocket_message(symbol: str, message: str):
    try:
        data = json.loads(message)
        if 't' in data and 'p' in data and 'q' in data:
            timestamp_ms = data['t']
            price = float(data['p'])
            volume = float(data['q'])
            tick = {'price': price, 'volume': volume}
            await handle_tick(symbol, tick)

    except json.JSONDecodeError as e:
        logger.info(f"[ERROR] Error decoding websocket message for {symbol}: {e}")
    except Exception as e:
        logger.info(f"[ERROR] Error processing websocket message for {symbol}: {e}")
        traceback.print_exc()

async def websocket_listener(symbol: str):
    """Listens to Binance websocket for 1-second trade data for a given symbol."""
    ws_url = f"{BINANCE_WEBSOCKET_URL}/{symbol.lower()}@trade"
    session = None
    ws = None
    while True:  # Keep trying to connect
        try:
            session = aiohttp.ClientSession()
            async with session.ws_connect(ws_url) as ws:
                logger.info(f"[WS] Websocket connected for {symbol}")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await process_websocket_message(symbol, msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.info(f"[WS] Websocket disconnected for {symbol}: {ws.close_code}, {ws.exception()}")
                        break
        except aiohttp.ClientConnectionError as e:
            logger.info(f"[ERROR] Connection error for {symbol}: {e}")
        except asyncio.CancelledError:
            logger.info(f"[WS] Websocket listener cancelled for {symbol}")
            break  # Exit the while loop on cancellation
        except Exception as e:
            logger.info(f"[ERROR] Unexpected error for {symbol}: {e}")
            traceback.print_exc()
        finally:
            if ws and not ws.closed:
                await ws.close()
            if session and not session.closed:
                await session.close()

        if not ws or ws.closed:
            jitter = random.uniform(0, 3)
            logger.info(f"[WS] Attempting to reconnect to websocket for {symbol} in {RETRY_DELAY + jitter:.2f} seconds...")
            await asyncio.sleep(RETRY_DELAY + jitter)

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

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(BASE_URL + OHLC_ENDPOINT, params=params) as response:
                    response.raise_for_status()  # Raise error for 4xx/5xx responses
                    data = await response.json()
                    return data
        except aiohttp.ClientError as e:
            logger.info(f"[ERROR] API request failed for {symbol} (attempt {attempt + 1}): {e}")
            if attempt < 2:
                await asyncio.sleep(RETRY_DELAY * (attempt + 1))
        except json.JSONDecodeError as e:
            logger.info(f"[ERROR] Error decoding JSON for {symbol}: {e}")
            return None
        except Exception as e:
            logger.info(f"[ERROR] Unexpected error while fetching {symbol}: {e}")
            traceback.print_exc()
            return None
    return None

async def store_to_influx(symbol: str, ohlcv_data: List[list]):
    """Stores fetched OHLCV data to InfluxDB."""
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
        logger.info(f"[API] Stored {len(points)} candles for {symbol} to InfluxDB.")
    except Exception as e:
        logger.info(f"[ERROR] Error writing to InfluxDB for {symbol}: {e}")
        traceback.print_exc()

def get_missing_minutes(symbol: str, query_api: QueryApi, bucket: str, days: int) -> List[datetime.datetime]:
    """
    Returns a sorted list of 1-minute interval start times that have zero points
    in the last `days` days for the given symbol.
    """
    flux = f'''
    from(bucket: "{bucket}")
      |> range(start: -{days}d)
      |> filter(fn: (r) =>
          r._measurement == "candles"
        and r.symbol == "{symbol}"
        and r._field == "close")
      |> aggregateWindow(every: 1m, fn: count, createEmpty: true)
      |> filter(fn: (r) => r._value == 0)
      |> keep(columns: ["_time"])
    '''
    missing = []
    tables = query_api.query(flux)
    for table in tables:
        for rec in table.records:
            missing.append(rec.get_time().replace(tzinfo=datetime.timezone.utc))
    return sorted(missing)

def coalesce_ranges(times: List[datetime.datetime]) -> List[Tuple[datetime.datetime, datetime.datetime]]:
    """
    Given sorted minute-start times, coalesce contiguous minutes into (start, end) ranges.
    Each end is exclusive.
    """
    if not times:
        return []
    ranges = []
    start = prev = times[0]
    for t in times[1:]:
        if t == prev + datetime.timedelta(minutes=1):
            prev = t
        else:
            ranges.append((start, prev + datetime.timedelta(minutes=1)))
            start = prev = t
    ranges.append((start, prev + datetime.timedelta(minutes=1)))
    return ranges

async def backfill_historical_data(symbol: str, start_time: datetime.datetime, end_time: datetime.datetime):
    try:
        logger.info(f"[BACKFILL] Starting for {symbol} from {start_time} to {end_time}")

        # Ensure both start_time and end_time are timezone-aware (UTC)
        if start_time.tzinfo is None or start_time.utcoffset() is None:
            start_time = start_time.replace(tzinfo=datetime.timezone.utc)
        if end_time.tzinfo is None or end_time.utcoffset() is None:
            end_time = end_time.replace(tzinfo=datetime.timezone.utc)

        candles = await fetch_ohlcv(symbol, int(start_time.timestamp() * 1000), int(end_time.timestamp() * 1000))
        points = []

        if candles:
            for candle in candles:
                open_time = candle[0]
                point = (
                    Point("candles")
                    .tag("symbol", symbol)
                    .time(datetime.datetime.fromtimestamp(open_time / 1000, tz=datetime.timezone.utc))
                    .field("open", float(candle[1]))
                    .field("high", float(candle[2]))
                    .field("low", float(candle[3]))
                    .field("close", float(candle[4]))
                    .field("volume", float(candle[5]))
                    .field("close_time", candle[6])
                    # Adding other fields as well
                    .field("quote_asset_volume", float(candle[7]))
                    .field("number_of_trades", int(candle[8]))
                    .field("taker_buy_base_asset_volume", float(candle[9]))
                    .field("taker_buy_quote_asset_volume", float(candle[10]))
                )
                points.append(point)

            if points:
                await async_write_batches(data_points=points, bucket_name=INFLUX_BUCKET)
                logger.info(f"[BACKFILL] Wrote {len(points)} candles for {symbol}")

    except asyncio.CancelledError:
        logger.info(f"[CANCELLED] Backfill cancelled for {symbol}")
        raise  # Re-raise to let the task exit cleanly

    except Exception as e:
        logger.info(f"[ERROR] Exception in backfill_historical_data for {symbol}: {e}")
        traceback.print_exc()

async def limited_backfill(symbol, start, end, semaphore):
    async with semaphore:
        await backfill_historical_data(symbol, start, end)

async def backfill_by_chunks(symbol, start, end, days=5, max_concurrent=3):
    semaphore = asyncio.Semaphore(max_concurrent)
    tasks = []
    current = start
    while current < end:
        chunk_end = min(current + datetime.timedelta(days=days), end)
        tasks.append(asyncio.create_task(limited_backfill(symbol, current, chunk_end, semaphore)))
        current = chunk_end
    await asyncio.gather(*tasks)

async def run_backfiller(symbols: List[str], days: int):
    """Runs the initial backfilling for specified symbols."""
    now = datetime.datetime.now()
    backfill_start = now - datetime.timedelta(days=days)
    await asyncio.gather(*(backfill_by_chunks(symbol, backfill_start, now) for symbol in symbols))
    logger.info("[BACKFILL] Initial backfill completed.")

async def periodic_backfill_check(symbols: List[str], backfill_lookback: datetime.timedelta = datetime.timedelta(hours=1)):
    """Periodically checks for and backfills missing data."""
    while True:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        backfill_start = now_utc - backfill_lookback
        logger.info(f"[BACKFILL] Starting periodic backfill check for symbols: {symbols}, looking back {backfill_lookback}.")
        backfill_tasks = []
        for symbol in symbols:
            missing_ranges = [(m, m + datetime.timedelta(minutes=1))
                              for m in get_missing_minutes(symbol, query_api, INFLUX_BUCKET, 1)]
            coalesced_ranges = coalesce_ranges([r[0] for r in missing_ranges])
            logger.info(f"[BACKFILL] Found {len(missing_ranges)} missing intervals for {symbol}, coalesced into {len(coalesced_ranges)} ranges.")
            for start, end in coalesced_ranges:
                logger.info(f"[BACKFILL] Backfilling {symbol} from {start} to {end}")
                backfill_tasks.append(asyncio.create_task(backfill_historical_data(symbol, start, end)))
                await asyncio.sleep(1)  # Adjust to respect rate limits

        if backfill_tasks:
            await asyncio.gather(*backfill_tasks)
        logger.info("[BACKFILL] Periodic backfill check completed. Waiting for the next cycle.")
        await asyncio.sleep(60 * 60)  # Run every hour

async def run_all(days: int):
    """Runs all collectors: initial backfilling, websockets, and periodic backfill."""
    # Initial backfill is now triggered in the lifespan
    websocket_tasks = [asyncio.create_task(websocket_listener(symbol)) for symbol in SYMBOLS]
    periodic_backfill_task = asyncio.create_task(periodic_backfill_check(SYMBOLS, datetime.timedelta(days=1)))
    await asyncio.gather(*websocket_tasks)
    await periodic_backfill_task

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":


    # --- Define Log Directory and Files ---
    LOG_DIR = "logs"  # Create a 'logs' subdirectory for log files
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    APP_LOG_FILE = os.path.join(LOG_DIR, "app_main.log")     # For your app, uvicorn errors, and uvicorn operational logs
    ACCESS_LOG_FILE = os.path.join(LOG_DIR, "app_access.log") # For uvicorn access (HTTP request) logs

    # --- Create a Deep Copy of Uvicorn's Default Logging Config ---
    # This ensures we start with Uvicorn's standard formatters and console handlers
    log_config = copy.deepcopy(LOGGING_CONFIG)

    # --- Define File Handlers ---
    # These handlers will write logs to files and include log rotation.
    log_config["handlers"]["app_file_handler"] = {
        "formatter": "default",  # Use Uvicorn's 'default' formatter
        "class": "logging.handlers.RotatingFileHandler",
        "filename": APP_LOG_FILE,
        "maxBytes": 10 * 1024 * 1024,  # 10 MB per file
        "backupCount": 5,           # Keep 5 backup files
        "encoding": "utf-8",
    }
    log_config["handlers"]["access_file_handler"] = {
        "formatter": "access",  # Use Uvicorn's 'access' formatter
        "class": "logging.handlers.RotatingFileHandler",
        "filename": ACCESS_LOG_FILE,
        "maxBytes": 10 * 1024 * 1024,  # 10 MB per file
        "backupCount": 5,           # Keep 5 backup files
        "encoding": "utf-8",
    }

    # --- Add File Handlers to Uvicorn's Loggers ---
    # Uvicorn's operational/error logs (usually logged via 'uvicorn' or 'uvicorn.error' logger)
    # The 'uvicorn' logger is often the parent for 'uvicorn.error'
    if "uvicorn" in log_config["loggers"]:
        log_config["loggers"]["uvicorn"]["handlers"].append("app_file_handler")
    # If uvicorn.error has its own specific configuration, add there too
    if "uvicorn.error" in log_config["loggers"]:
        if "handlers" not in log_config["loggers"]["uvicorn.error"]:
            log_config["loggers"]["uvicorn.error"]["handlers"] = []
        log_config["loggers"]["uvicorn.error"]["handlers"].append("app_file_handler")


    # Uvicorn's access logs
    if "uvicorn.access" in log_config["loggers"]:
        log_config["loggers"]["uvicorn.access"]["handlers"].append("access_file_handler")

    # --- Configure Your Application-Specific Logger ---
    # (Assuming you used `logger = logging.getLogger("binance_collector_app")` in Step 1)
    YOUR_APP_LOGGER_NAME = "binance_collector_app" # Use the same name you chose in Step 1
    log_config["loggers"][YOUR_APP_LOGGER_NAME] = {
        "handlers": ["default", "app_file_handler"],  # Output to console (Uvicorn's default) AND your app file
        "level": "INFO",                           # Or your desired level (DEBUG, WARNING, etc.)
        "propagate": False, # Important: Prevent logs from being passed to the root logger if root also has handlers, to avoid duplicates.
    }

    # Optional: If you want general Python logs (not from Uvicorn or your app logger)
    # that go to the root logger to also be written to your app file:
    if "root" in log_config:
        log_config["root"]["handlers"].append("app_file_handler")
    else:
        log_config["root"] = {
            "handlers": ["default", "app_file_handler"], # 'default' is Uvicorn's console handler
            "level": "INFO",
        }
    # Windows-specific fix: Use SelectorEventLoop on Windows for aiodns
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=log_config)