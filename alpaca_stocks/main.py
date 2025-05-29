# Refactored version of the provided code using InfluxDB for persistent checkpointing and batching

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
from typing import List
import requests

# Fix Windows event loop policy
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Load environment variables
load_dotenv()

ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN_C")
INFLUX_ORG = os.getenv("INFLUX_ORG")
FMP_API_KEY = os.getenv("FMP_API_KEY")
INFLUX_BUCKET = "US_stocks"
MEASUREMENT = "US_stocks"
BATCH_DAYS = 7
CONCURRENCY_LIMIT = 8
RETRY_LIMIT = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alpaca_backfill")

# Alpaca Clients
trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
stock_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

# FastAPI App
app = FastAPI()

def fetch_company_name_fmp(symbol):
    url = f"https://financialmodelingprep.com/api/v3/search?query={symbol}&limit=1&apikey={FMP_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                return data[0].get("name")
    except Exception as e:
        logger.warning(f"FMP API error for {symbol}: {e}")
    return "Unknown"

async def fetch_all_active_us_equity_symbols() -> List[str]:
    request = GetAssetsRequest(status=AssetStatus.ACTIVE)
    assets = await asyncio.to_thread(trading_client.get_all_assets, request)
    return [asset.symbol for asset in assets]

async def fetch_bars_with_retries(symbol: str, start: datetime, end: datetime) -> List:
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
            await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch bars for {symbol} after {RETRY_LIMIT} attempts")
    return []

def bar_to_point(bar, measurement: str, company_name: str):
    ts = bar.timestamp
    if not isinstance(ts, datetime):
        ts = datetime.fromtimestamp(ts / 1_000_000_000, tz=timezone.utc)
    point = (
        Point(measurement)
        .tag("symbol", bar.symbol)
        .tag("company_name", company_name)
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

def get_last_timestamp(symbol: str, client: InfluxDBClient) -> datetime | None:
    query_api = client.query_api()
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -100y)
          |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT}")
          |> filter(fn: (r) => r["symbol"] == "{symbol}")
          |> keep(columns: ["_time"])
          |> sort(columns: ["_time"], desc: true)
          |> limit(n: 1)
    '''
    result = query_api.query(org=INFLUX_ORG, query=query)
    try:
        for table in result:
            for record in table.records:
                return record.get_time()
    except Exception as e:
        logger.warning(f"Could not retrieve last timestamp for {symbol}: {e}")
    return None

async def backfill_worker(queue: asyncio.Queue, write_api, logger: logging.Logger):
    while True:
        task = await queue.get()
        if task is None:
            queue.task_done()
            break
        symbol, start, end, company_name = task
        logger.info(f"Backfilling {symbol} from {start} to {end}")

        bars = await fetch_bars_with_retries(symbol, start, end)
        logger.info(f"Fetched {len(bars)} bars for {symbol}")
        if not bars:
            logger.info(f"No data for {symbol} in given range.")
            queue.task_done()
            continue

        points = [bar_to_point(bar, MEASUREMENT, company_name) for bar in bars]

        try:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            logger.info(f"Written {len(points)} points for {symbol}")
        except Exception as e:
            logger.error(f"Failed to write data for {symbol}: {e}")
        queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting backfill process")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    symbols = await fetch_all_active_us_equity_symbols()
    logger.info(f"Fetched {len(symbols)} active symbols.")

    now = datetime.now(timezone.utc) - timedelta(minutes=16)
    queue = asyncio.Queue()

    workers = [
        asyncio.create_task(backfill_worker(queue, write_api, logger))
        for _ in range(CONCURRENCY_LIMIT)
    ]

    for symbol in symbols:
        company_name = fetch_company_name_fmp(symbol)
        last_time = get_last_timestamp(symbol, client)
        start = last_time + timedelta(minutes=1) if last_time else datetime(2020, 1, 1, tzinfo=timezone.utc)
        while start < now:
            batch_end = min(start + timedelta(days=BATCH_DAYS), now)
            await queue.put((symbol, start, batch_end, company_name))
            start = batch_end

    await queue.join()
    for _ in workers:
        await queue.put(None)
    await asyncio.gather(*workers)

    logger.info("Backfill complete")
    yield

app.router.lifespan_context = lifespan

@app.get("/")
def root():
    return {"message": "Alpaca US stocks backfill service active."}

