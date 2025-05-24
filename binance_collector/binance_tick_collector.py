import os
import sys
import json
import asyncio
import random
import logging
import traceback
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from typing import Dict, List, Any, Optional, DefaultDict
from collections import defaultdict

from influxdb_client import Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from dotenv import load_dotenv
import aiohttp

from utils.symbol_manager import get_active_binance_symbols

# --- Load Environment ---
load_dotenv()

# --- Logging Setup ---
logger = logging.getLogger("binance_collector_app")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Environment Variables ---
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
RETRY_DELAY = 5

INFLUX_URL   = os.getenv("INFLUX_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG   = os.getenv("INFLUX_ORG")
INFLUX_BUCKET= "Ticks"

if not INFLUX_TOKEN or not INFLUX_ORG:
    logger.error("INFLUX_TOKEN or INFLUX_ORG environment variables are not set.")
    sys.exit(1)

# --- Symbol Management ---
SYMBOLS = get_active_binance_symbols()
ACTIVE_SYMBOLS: Dict[str, bool] = {symbol: True for symbol in SYMBOLS}
background_tasks: List[asyncio.Task] = []

# --- Global Async InfluxDB Client ---
async_influx_client: Optional[InfluxDBClientAsync] = None

# --- Candle Aggregation State ---
# {symbol: {minute_timestamp: [tick, tick, ...]}}
tick_buffer: DefaultDict[str, DefaultDict[datetime, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))

def get_minute_timestamp(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)

# --- Candle Existence Check ---
async def candle_exists(symbol: str, minute_ts: datetime) -> bool:
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {minute_ts.isoformat()}Z, stop: {(minute_ts + timedelta(minutes=1)).isoformat()}Z)
        |> filter(fn: (r) => r._measurement == "candles" and r.symbol == "{symbol}")
        |> limit(n:1)
    '''
    tables = await async_influx_client.query_api().query(query)
    for table in tables:
        for _ in table.records:
            return True
    return False

# --- Tick Writer ---
async def handle_tick(symbol: str, tick: Dict[str, Any], timestamp: datetime):
    # Write tick to InfluxDB
    point = (
        Point("ticks")
        .tag("symbol", symbol)
        .time(timestamp, write_precision=WritePrecision.S)
        .field("price", tick['price'])
        .field("volume", tick['volume'])
    )
    write_api = async_influx_client.write_api()
    await write_api.write(bucket=INFLUX_BUCKET, record=[point])
    logger.info(f"[TICK] Wrote tick for {symbol} at {timestamp}: price={tick['price']}, volume={tick['volume']}")

    # Buffer tick for candle aggregation
    minute_ts = get_minute_timestamp(timestamp)
    tick_buffer[symbol][minute_ts].append(tick)

# --- Candle Aggregator ---
async def candle_aggregator():
    while True:
        now = datetime.now(timezone.utc).replace(tzinfo=timezone.utc)
        cutoff = get_minute_timestamp(now)
        for symbol in list(tick_buffer.keys()):
            for minute_ts in list(tick_buffer[symbol].keys()):
                if minute_ts < cutoff:
                    ticks = tick_buffer[symbol].pop(minute_ts)
                    if ticks:
                        prices = [t['price'] for t in ticks]
                        volumes = [t['volume'] for t in ticks]
                        candle_point = (
                            Point("candles")
                            .tag("symbol", symbol)
                            .time(minute_ts, write_precision=WritePrecision.S)
                            .field("open", prices[0])
                            .field("high", max(prices))
                            .field("low", min(prices))
                            .field("close", prices[-1])
                            .field("volume", sum(volumes))
                        )
                        # Prevent duplicate candle write
                        if not await candle_exists(symbol, minute_ts):
                            write_api = async_influx_client.write_api()
                            await write_api.write(bucket=INFLUX_BUCKET, record=[candle_point])
                            logger.info(f"[CANDLE] Wrote 1m candle for {symbol} at {minute_ts}")
                        else:
                            logger.info(f"[CANDLE] Skipped duplicate for {symbol} at {minute_ts}")
        await asyncio.sleep(5)  # Check every 5 seconds

# --- WebSocket Handling ---
async def process_websocket_message(symbol: str, message: str):
    try:
        data = json.loads(message)
        if 'T' in data and 'p' in data and 'q' in data:
            timestamp = datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc)
            price = float(data['p'])
            volume = float(data['q'])
            tick = {'price': price, 'volume': volume}
            await handle_tick(symbol, tick, timestamp)
        logger.debug(f"[DEBUG] Raw message for {symbol}: {message}")
    except json.JSONDecodeError as e:
        logger.warning(f"[ERROR] JSON decoding error for {symbol}: {e}")
    except Exception as e:
        logger.warning(f"[ERROR] Error processing message for {symbol}: {e}")
        traceback.print_exc()

async def websocket_listener(symbol: str):
    ws_url = f"{BINANCE_WEBSOCKET_URL}/{symbol.lower()}@trade"
    async with aiohttp.ClientSession() as session:
        while True:
            if not ACTIVE_SYMBOLS.get(symbol, False):
                await asyncio.sleep(1)
                continue
            ws = None
            try:
                async with session.ws_connect(ws_url) as ws:
                    logger.info(f"[WS] WebSocket connected for {symbol}")
                    async for msg in ws:
                        if not ACTIVE_SYMBOLS.get(symbol, False):
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await process_websocket_message(symbol, msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning(f"[WS] Disconnected for {symbol}: {ws.close_code}, {ws.exception()}")
                            break
            except aiohttp.ClientConnectionError as e:
                logger.warning(f"[ERROR] Connection error for {symbol}: {e}")
            except asyncio.CancelledError:
                logger.info(f"[WS] WebSocket listener cancelled for {symbol}")
                break
            except Exception as e:
                logger.warning(f"[ERROR] Unexpected error for {symbol}: {e}")
                traceback.print_exc()
            finally:
                if ws and not ws.closed:
                    await ws.close()
            jitter = random.uniform(0, 3)
            logger.info(f"[WS] Reconnecting for {symbol} in {RETRY_DELAY + jitter:.2f} seconds...")
            await asyncio.sleep(RETRY_DELAY + jitter)

# --- App Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global async_influx_client
    async_influx_client = InfluxDBClientAsync(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )
    websocket_tasks = [asyncio.create_task(websocket_listener(symbol)) for symbol in SYMBOLS]
    candle_task = asyncio.create_task(candle_aggregator())
    background_tasks.extend(websocket_tasks)
    background_tasks.append(candle_task)
    try:
        yield
    finally:
        logger.info("[SHUTDOWN] Cancelling background tasks...")
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        if async_influx_client:
            await async_influx_client.__aexit__(None, None, None)
        logger.info("[SHUTDOWN] All background tasks cancelled.")

# --- FastAPI App ---
app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

@app.post("/symbol/{symbol}/enable")
async def enable_symbol(symbol: str):
    ACTIVE_SYMBOLS[symbol.upper()] = True
    return JSONResponse(content={"status": f"Symbol {symbol.upper()} enabled"})

@app.post("/symbol/{symbol}/disable")
async def disable_symbol(symbol: str):
    ACTIVE_SYMBOLS[symbol.upper()] = False
    return JSONResponse(content={"status": f"Symbol {symbol.upper()} disabled"})

@app.post("/symbols/enable_all")
async def enable_all_symbols():
    for symbol in ACTIVE_SYMBOLS.keys():
        ACTIVE_SYMBOLS[symbol] = True
    return JSONResponse(content={"status": "All symbols enabled"})

@app.post("/symbols/disable_all")
async def disable_all_symbols():
    for symbol in ACTIVE_SYMBOLS.keys():
        ACTIVE_SYMBOLS[symbol] = False
    return JSONResponse(content={"status": "All symbols disabled"})

@app.get("/symbol/status")
async def symbol_status():
    return ACTIVE_SYMBOLS

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
