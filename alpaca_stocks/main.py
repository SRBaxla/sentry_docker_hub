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
import pickle

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
MAPPING_MEASUREMENT = "US_stocks_mapping"
BATCH_DAYS = 7
CHUNK_SIZE = 5000
CONCURRENCY_LIMIT = 8
RETRY_LIMIT = 2
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # put your key in .env

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alpaca_backfill")

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
stock_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

app = FastAPI()

PICKLE_FILE = "symbol_name_map.pkl"

def load_symbol_name_map():
    if os.path.exists(PICKLE_FILE):
        with open(PICKLE_FILE, "rb") as f:
            return pickle.load(f)
    return {}

def save_symbol_name_map(symbol_name_map):
    with open(PICKLE_FILE, "wb") as f:
        pickle.dump(symbol_name_map, f)

def fetch_company_name_polygon(symbol: str) -> str:
    url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            name = data.get("results", {}).get("name")
            if name:
                return name
    except Exception as e:
        logger.warning(f"Polygon API error for {symbol}: {e}")
    return "Unknown"

def get_company_name(symbol: str, symbol_name_map: dict) -> str:
    if symbol in symbol_name_map:
        return symbol_name_map[symbol]

    name = fetch_company_name_polygon(symbol)
    symbol_name_map[symbol] = name
    save_symbol_name_map(symbol_name_map)
    return name

def write_mapping_to_influx(symbol_name_map, write_api):
    FIXED_TIMESTAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Epoch start, fixed timestamp
    points = []
    for symbol, name in symbol_name_map.items():
        point = (
            Point("US_stocks_mapping")
            .tag("symbol", symbol)
            .field("company_name", name)
            .time(FIXED_TIMESTAMP, WritePrecision.S)
        )
        points.append(point)
    try:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
        logger.info(f"Written {len(points)} company name mappings to InfluxDB with fixed timestamp.")
    except Exception as e:
        logger.error(f"Failed to write company name mappings to InfluxDB: {e}")

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
    return (
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

async def process_symbol(symbol: str, client: InfluxDBClient, write_api, semaphore: asyncio.Semaphore, symbol_name_map: dict):
    async with semaphore:
        company_name = get_company_name(symbol, symbol_name_map)
        last_time = get_last_timestamp(symbol, client)
        now = datetime.now(timezone.utc) - timedelta(minutes=16)
        start = last_time + timedelta(minutes=1) if last_time else datetime(2020, 1, 1, tzinfo=timezone.utc)

        while start < now:
            end = min(start + timedelta(days=BATCH_DAYS), now)
            bars = await fetch_bars_with_retries(symbol, start, end)
            if not bars:
                logger.info(f"No data for {symbol} from {start} to {end}")
                start = end
                continue

            points = [bar_to_point(bar, MEASUREMENT, company_name) for bar in bars]

            for i in range(0, len(points), CHUNK_SIZE):
                chunk = points[i:i+CHUNK_SIZE]
                try:
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=chunk)
                    logger.info(f"{symbol}: Wrote {len(chunk)} points from {start} to {end}")
                except Exception as e:
                    logger.error(f"{symbol}: Failed to write chunk: {e}")
            start = end

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting backfill process")
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    symbol_name_map = load_symbol_name_map()

    symbols = await fetch_all_active_us_equity_symbols()
    logger.info(f"Fetched {len(symbols)} active symbols.")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    tasks = [process_symbol(symbol, client, write_api, semaphore, symbol_name_map) for symbol in symbols]
    await asyncio.gather(*tasks)

    write_mapping_to_influx(symbol_name_map, write_api)

    logger.info("Backfill complete")
    yield

app.router.lifespan_context = lifespan

@app.get("/")
def root():
    return {"message": "Alpaca US stocks backfill service active."}
