import os
import sys
import json
import time
import asyncio
import random
import logging
import datetime
import traceback
import aiohttp

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from typing import List, Dict

from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from dotenv import load_dotenv

from utils.symbol_manager import get_active_binance_symbols
from utils.influx_writer import async_write_batches, get_influx_client

load_dotenv()

logger = logging.getLogger("binance_collector_app")
logger.setLevel(logging.INFO)

INFLUX_BUCKET = "Sentry"
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
RETRY_DELAY = 5

SYMBOLS = get_active_binance_symbols()
ACTIVE_SYMBOLS: Dict[str, bool] = {symbol: True for symbol in SYMBOLS}
background_tasks = []

client = get_influx_client()
write_api = client.write_api(write_options=ASYNCHRONOUS)

# --- Tick Writer ---
async def handle_tick(symbol, tick, timestamp):
    point = (
        Point("ticks")
        .tag("symbol", symbol)
        .time(timestamp, write_precision=WritePrecision.S)
        .field("price", tick['price'])
        .field("volume", tick['volume'])
    )
    await async_write_batches(data_points=[point], bucket_name=INFLUX_BUCKET)
    logger.info(f"[TICK] Wrote tick for {symbol} at {timestamp}: price={tick['price']}, volume={tick['volume']}")

# --- WebSocket Handling ---
async def process_websocket_message(symbol: str, message: str):
    try:
        data = json.loads(message)
        if 't' in data and 'p' in data and 'q' in data:
            timestamp = datetime.datetime.fromtimestamp(data['t'] / 1000, tz=datetime.timezone.utc)
            price = float(data['p'])
            volume = float(data['q'])
            tick = {'price': price, 'volume': volume}
            await handle_tick(symbol, tick, timestamp)
    except json.JSONDecodeError as e:
        logger.info(f"[ERROR] Error decoding websocket message for {symbol}: {e}")
    except Exception as e:
        logger.info(f"[ERROR] Error processing websocket message for {symbol}: {e}")
        traceback.print_exc()

async def websocket_listener(symbol: str):
    ws_url = f"{BINANCE_WEBSOCKET_URL}/{symbol.lower()}@trade"
    session = None
    ws = None
    while True:
        if not ACTIVE_SYMBOLS.get(symbol, False):
            await asyncio.sleep(1)
            continue
        try:
            session = aiohttp.ClientSession()
            async with session.ws_connect(ws_url) as ws:
                logger.info(f"[WS] Websocket connected for {symbol}")
                async for msg in ws:
                    if not ACTIVE_SYMBOLS.get(symbol, False):
                        break
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await process_websocket_message(symbol, msg.data)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.info(f"[WS] Websocket disconnected for {symbol}: {ws.close_code}, {ws.exception()}")
                        break
        except aiohttp.ClientConnectionError as e:
            logger.info(f"[ERROR] Connection error for {symbol}: {e}")
        except asyncio.CancelledError:
            logger.info(f"[WS] Websocket listener cancelled for {symbol}")
            break
        except Exception as e:
            logger.info(f"[ERROR] Unexpected error for {symbol}: {e}")
            traceback.print_exc()
        finally:
            if ws and not ws.closed:
                await ws.close()
            if session and not session.closed:
                await session.close()
        jitter = random.uniform(0, 3)
        logger.info(f"[WS] Reconnecting to websocket for {symbol} in {RETRY_DELAY + jitter:.2f} seconds...")
        await asyncio.sleep(RETRY_DELAY + jitter)

# --- App Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    websocket_tasks = [asyncio.create_task(websocket_listener(symbol)) for symbol in SYMBOLS]
    background_tasks.extend(websocket_tasks)
    try:
        yield
    finally:
        logger.info("[SHUTDOWN] Cancelling background tasks...")
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
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

@app.get("/symbol/status")
async def symbol_status():
    return ACTIVE_SYMBOLS

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
