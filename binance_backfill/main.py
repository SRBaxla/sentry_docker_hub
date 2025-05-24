import csv
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

CSV_PATH = "symbols.csv"
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"
TICKS_MEASUREMENT = "ticks"
CANDLES_MEASUREMENT = "candles"
BINANCE_ENDPOINT = "https://api.binance.com/api/v3/klines"

MAX_CONCURRENT_REQUESTS = 10
RATE_LIMIT_DELAY = 0.5  # seconds between chunk requests

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

def read_symbols_from_csv(csv_path: str) -> List[str]:
    """Read trading symbols from a CSV file."""
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        return [row['symbol'] for row in reader]

def to_flux_time(dt: datetime) -> str:
    """Convert datetime to RFC3339 string for Flux (always UTC, ending with Z)."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec='seconds') + "Z"

def get_latest_candle_time(symbol: str, client: InfluxDBClient) -> Optional[datetime]:
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "{CANDLES_MEASUREMENT}" and r.symbol == "{symbol}" and r._field == "close")
        |> keep(columns: ["_time"])
        |> sort(columns: ["_time"], desc: true)
        |> limit(n:1)
    '''
    result = client.query_api().query(query)
    for table in result:
        for record in table.records:
            # Ensure the returned datetime is UTC-aware
            t = record.get_time()
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            return t
    return None


def get_expected_timestamps(start: datetime, end: datetime) -> List[datetime]:
    """Generate a list of expected 1-minute timestamps between start and end."""
    current = start
    result = []
    while current <= end:
        result.append(current)
        current += timedelta(minutes=1)
    return result

def aggregate_ticks_to_candle(symbol: str, minute: datetime, client: InfluxDBClient) -> Optional[dict]:
    """Aggregate tick data for a given symbol and 1m window into a candle."""
    start = to_flux_time(minute)
    end = to_flux_time(minute + timedelta(minutes=1))
    query = f'''
        from(bucket: "Ticks")
        |> range(start: {start}, stop: {end})
        |> filter(fn: (r) => r._measurement == "{TICKS_MEASUREMENT}" and r.symbol == "{symbol}")
        |> sort(columns: ["_time"])
    '''
    result = client.query_api().query(query)
    prices = []
    volumes = []
    for table in result:
        for record in table.records:
            if record.get_field() == "price":
                prices.append(record.get_value())
            elif record.get_field() == "volume":
                volumes.append(record.get_value())
    if prices and volumes:
        open_ = prices[0]
        close = prices[-1]
        high = max(prices)
        low = min(prices)
        volume = sum(volumes)
        return {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "timestamp": minute
        }
    return None

def write_candle_to_influx(symbol: str, candle: dict, write_api):
    """Write a single candle to InfluxDB."""
    point = (
        Point(CANDLES_MEASUREMENT)
        .tag("symbol", symbol)
        .field("open", float(candle["open"]))
        .field("high", float(candle["high"]))
        .field("low", float(candle["low"]))
        .field("close", float(candle["close"]))
        .field("volume", float(candle["volume"]))
        .time(candle["timestamp"], WritePrecision.S)
    )
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=[point])

async def fetch_binance_klines_async(session: aiohttp.ClientSession, symbol: str, start_time: datetime, end_time: datetime) -> list:
    """Fetch klines from Binance for a symbol and time range."""
    async with semaphore:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": int(start_time.timestamp() * 1000),
            "endTime": int(end_time.timestamp() * 1000),
            "limit": 1000
        }
        try:
            async with session.get(BINANCE_ENDPOINT, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    text = await response.text()
                    print(f"[ERROR] Fetching {symbol}: {response.status} - {text}")
        except Exception as e:
            print(f"[EXCEPTION] Fetch for {symbol}: {e}")
        return []

def parse_binance_kline(kline, minute: datetime) -> dict:
    """Convert a Binance kline to a candle dict."""
    return {
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "timestamp": minute
    }

async def fill_gap(symbol: str, minute: datetime, client: InfluxDBClient, write_api, session: aiohttp.ClientSession):
    """Try to fill a missing candle from ticks, else from Binance."""
    try:
        candle = aggregate_ticks_to_candle(symbol, minute, client)
        if candle:
            write_candle_to_influx(symbol, candle, write_api)
            print(f"[FILL] {symbol} {minute}: Filled from ticks.")
            return
        klines = await fetch_binance_klines_async(session, symbol, minute, minute + timedelta(minutes=1))
        if klines:
            candle = parse_binance_kline(klines[0], minute)
            write_candle_to_influx(symbol, candle, write_api)
            print(f"[FILL] {symbol} {minute}: Filled from Binance.")
        else:
            print(f"[FILL] {symbol} {minute}: No data available from Binance.")
    except Exception as e:
        print(f"[ERROR] Exception filling {symbol} {minute}: {e}")

async def backfill_symbol_async(symbol: str, client: InfluxDBClient, write_api, start_dt: datetime, end_dt: datetime, session: aiohttp.ClientSession):
    """Backfill missing 1m candles for a symbol between start_dt and end_dt."""
    print(f"[BACKFILL] {symbol}: Filling from {start_dt} to {end_dt}...")
    expected = get_expected_timestamps(start_dt, end_dt)
    for minute in expected:
        await fill_gap(symbol, minute, client, write_api, session)
        await asyncio.sleep(RATE_LIMIT_DELAY)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[DEBUG] Lifespan started")
    try:
        symbols = read_symbols_from_csv(CSV_PATH)
        print(f"[DEBUG] Read {len(symbols)} symbols from CSV")

        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for symbol in symbols:
                latest = get_latest_candle_time(symbol, client)
                if latest:
                    start_dt = latest + timedelta(minutes=1)
                    if start_dt.tzinfo is None:
                        start_dt = start_dt.replace(tzinfo=timezone.utc)

                    print(f"[BACKFILL] {symbol}: Latest candle at {latest}, backfilling from {start_dt}")
                else:
                    print(f"[BACKFILL] {symbol}: No data found, skipping.")
                    continue  # Or set a default start date if you want to fill from scratch

                end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
                if end_dt > start_dt:
                    tasks.append(backfill_symbol_async(symbol, client, write_api, start_dt, end_dt, session))
                else:
                    print(f"[BACKFILL] {symbol}: No backfill needed (up to date).")
            if tasks:
                await asyncio.gather(*tasks)

        client.close()
        print("[BACKFILL] Complete.")
    except Exception as e:
        print(f"[ERROR] Exception in lifespan: {e}")
        raise
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Backfill service with local aggregation active."}
