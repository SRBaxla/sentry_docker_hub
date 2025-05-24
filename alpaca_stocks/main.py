import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetStatus
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
import logging
import sys
from typing import List, Optional

# Fix Windows event loop policy
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN_C")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "US_stocks"
MEASUREMENT = "US_stocks"

CONCURRENCY_LIMIT = 8
RETRY_LIMIT = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alpaca_backfill")

# --- Alpaca Clients ---
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
stock_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# --- FastAPI App ---
app = FastAPI()

async def fetch_all_active_us_equity_symbols() -> List[str]:
    """
    Fetch all active US equity symbols from Alpaca.
    """
    request = GetAssetsRequest(status=AssetStatus.ACTIVE)
    assets = await asyncio.to_thread(trading_client.get_all_assets, request)
    return [asset.symbol for asset in assets]

async def fetch_bars_with_retries(symbol: str, start: datetime, end: datetime) -> List:
    """
    Fetch 1m bars for a symbol from Alpaca with retries.
    """
    for attempt in range(RETRY_LIMIT):
        try:
            request = StockBarsRequest(
                symbol_or_symbols=symbol,
                start=start,
                end=end,
                timeframe=TimeFrame.Minute
            )
            bars_response = await asyncio.to_thread(stock_data_client.get_stock_bars, request)
            bars = bars_response.data.get(symbol, [])
            return bars
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed for {symbol}: {e}")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
    logger.error(f"Failed to fetch bars for {symbol} after {RETRY_LIMIT} attempts")
    return []

def bar_to_point(bar, measurement: str) -> Point:
    """
    Convert an Alpaca bar object to an InfluxDB Point.
    """
    ts = bar.timestamp
    # If bar.timestamp is not a datetime, convert from nanoseconds
    if not isinstance(ts, datetime):
        ts = datetime.fromtimestamp(ts / 1_000_000_000, tz=timezone.utc)
    point = (
        Point(measurement)
        .tag("symbol", bar.symbol)
        .field("open", float(bar.open))
        .field("high", float(bar.high))
        .field("low", float(bar.low))
        .field("close", float(bar.close))
        .field("volume", float(bar.volume))
        .field("trade_count", float(getattr(bar, "trade_count", 0)))
        .field("vwap", float(getattr(bar, "vwap", 0)))
        .time(ts, WritePrecision.S)
    )
    return point

async def backfill_worker(
    queue: asyncio.Queue,
    write_api,
    logger: logging.Logger
):
    """
    Background worker that fetches bars and writes them to InfluxDB.
    """
    while True:
        task = await queue.get()
        if task is None:  # Shutdown sentinel
            queue.task_done()
            break
        symbol, start, end = task
        logger.info(f"Backfilling {symbol} from {start} to {end}")

        bars = await fetch_bars_with_retries(symbol, start, end)
        logger.info(f"Fetched {len(bars)} bars for {symbol}")
        if not bars:
            logger.info(f"No data for {symbol} in given range.")
            queue.task_done()
            continue

        points = [bar_to_point(bar, MEASUREMENT) for bar in bars]

        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            logger.info(f"Written {len(points)} points for {symbol}")
        except Exception as e:
            logger.error(f"Failed to write data for {symbol}: {e}")
        queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context to manage backfill process.
    """
    logger.info("Starting backfill process")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    symbols = await fetch_all_active_us_equity_symbols()
    logger.info(f"Fetched {len(symbols)} active symbols.")

    # Adjust these dates as needed
    start_dt = datetime.now(timezone.utc) - timedelta(days=30)
    end_dt = datetime.now(timezone.utc) - timedelta(minutes=16)

    queue = asyncio.Queue()

    # Launch workers
    workers = [
        asyncio.create_task(backfill_worker(queue, write_api, logger))
        for _ in range(CONCURRENCY_LIMIT)
    ]

    # Enqueue tasks
    for symbol in symbols:
        await queue.put((symbol, start_dt, end_dt))

    # Wait for all tasks to finish
    await queue.join()

    # Send shutdown signals to workers
    for _ in workers:
        await queue.put(None)

    await asyncio.gather(*workers)

    logger.info("Backfill complete")
    yield

app.router.lifespan_context = lifespan

@app.get("/")
def root():
    return {"message": "Alpaca US stocks backfill service active."}
