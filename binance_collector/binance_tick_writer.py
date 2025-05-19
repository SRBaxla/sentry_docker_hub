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
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from collections import defaultdict

from utils.symbol_manager import get_active_binance_symbols
from utils.influx_writer import async_write_batches, get_influx_client
import logging
import sys
import uvicorn
from uvicorn.config import LOGGING_CONFIG

logger = logging.getLogger("binance_collector_app")
logger.setLevel(logging.INFO)

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"
BINANCE_WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
RETRY_DELAY = 5
background_tasks = []

# Live Tick Handler
async def handle_tick(symbol, tick, timestamp):
    point = (
        Point("candles")
        .tag("symbol", symbol)
        .time(timestamp, write_precision=WritePrecision.S)
        .field("open", tick['price'])
        .field("high", tick['price'])
        .field("low", tick['price'])
        .field("close", tick['price'])
        .field("volume", tick['volume'])
    )
    await async_write_batches(data_points=[point], bucket_name=INFLUX_BUCKET)
    logger.info(f"[TICK] Wrote tick for {symbol} at {timestamp}")

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

app = FastAPI(lifespan=lifespan)

SYMBOLS = get_active_binance_symbols()
client = get_influx_client()

async def process_websocket_message(symbol: str, message: str):
    try:
        data = json.loads(message)
        if 't' in data and 'p' in data and 'q' in data:
            timestamp_s = int(data['t']) // 1000
            price = float(data['p'])
            volume = float(data['q'])
            tick = {'price': price, 'volume': volume}
            await handle_tick(symbol, tick, timestamp_s)
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
        logger.info(f"[WS] Attempting to reconnect to websocket for {symbol} in {RETRY_DELAY + jitter:.2f} seconds...")
        await asyncio.sleep(RETRY_DELAY + jitter)

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    if sys.platform.startswith('win'):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    uvicorn.run(app, host="0.0.0.0", port=8002)
