import csv
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta, timezone
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv
load_dotenv()

# --- CONFIGURATION ---
CSV_PATH = "symbols.csv"
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"
MEASUREMENT = "candles"
BINANCE_ENDPOINT = "https://api.binance.com/api/v3/klines"
START_DATE = "2025-04-01"
END_DATE = str(datetime.now().date())

MAX_CONCURRENT_REQUESTS = 5
RATE_LIMIT_DELAY = 0.5  # seconds between chunk requests

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# --- HELPERS ---
def read_symbols_from_csv(csv_path):
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        return [row['symbol'] for row in reader]

def get_existing_timestamps(symbol, client, start, end):
    start_time = to_flux_time(start)
    end_time = to_flux_time(end)
    query = f'''
        from(bucket: "{INFLUX_BUCKET}")
        |> range(start: {start_time}, stop: {end_time})
        |> filter(fn: (r) => r._measurement == "candles" and r.symbol == "{symbol}" and r._field == "close")
    '''
    result = client.query_api().query(query)
    
    timestamps = {
        record.get_time()
        .replace(tzinfo=None, second=0, microsecond=0)  # Align to minute
        for table in result for record in table.records
    }
    return timestamps


def get_expected_timestamps(start, end):
    start = start.replace(second=0, microsecond=0)
    current = start
    result = []
    while current <= end:
        result.append(current)
        current += timedelta(minutes=1)
    return result

def to_flux_time(dt: datetime) -> str:
    return dt.isoformat() + "Z"

def write_to_influx(symbol, klines, write_api):
    points = []
    for k in klines:
        ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        point = (
            Point(MEASUREMENT)
            .tag("symbol", symbol)
            .field("open", float(k[1]))
            .field("high", float(k[2]))
            .field("low", float(k[3]))
            .field("close", float(k[4]))
            .field("volume", float(k[5]))
            .time(ts, WritePrecision.S)
        )
        points.append(point)
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

# --- ASYNC OPERATIONS ---
async def fetch_binance_klines_async(session, symbol, start_time, end_time):
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
                    print(f"Error fetching {symbol}: {response.status} - {text}")
        except Exception as e:
            print(f"Exception during fetch for {symbol}: {e}")
        return []

async def handle_chunk(symbol, start_t, end_t, session, write_api):
    klines = await fetch_binance_klines_async(session, symbol, start_t, end_t)
    if klines:
        write_to_influx(symbol, klines, write_api)

        # --- Verification ---
        written_times = {datetime.utcfromtimestamp(k[0] // 1000).replace(second=0, microsecond=0) for k in klines}
        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        existing = get_existing_timestamps(symbol, client, start_t, end_t)
        still_missing = sorted(written_times - existing)
        if still_missing:
            print(f"⚠️ After write, still missing {len(still_missing)} timestamps for {symbol}. Example: {still_missing[0]}")
        client.close()

    await asyncio.sleep(RATE_LIMIT_DELAY)


async def backfill_symbol_async(symbol, client, write_api, start_dt, end_dt, session):
    print(f"Backfilling {symbol}...")
    existing = get_existing_timestamps(symbol, client, start_dt, end_dt)
    expected = get_expected_timestamps(start_dt, end_dt)
    missing = [ts for ts in expected if ts not in existing]

    if not missing:
        print(f"No gaps found for {symbol}.")
        return

    print(f"Found {len(missing)} missing bars for {symbol}.")
    missing.sort()
    tasks = []
    for i in range(0, len(missing), 1000):
        chunk = missing[i:i + 1000]
        start_t = chunk[0]
        end_t = chunk[-1] + timedelta(minutes=1)
        tasks.append(asyncio.create_task(handle_chunk(symbol, start_t, end_t, session, write_api)))
    await asyncio.gather(*tasks)
    # Recheck post-fill
    final_existing = get_existing_timestamps(symbol, client, start_dt, end_dt)
    final_missing = [ts for ts in expected if ts not in final_existing]
    if final_missing:
        print(f"❌ Final check: {len(final_missing)} missing candles for {symbol}")
    else:
        print(f"✅ {symbol} backfilled completely. All candles present.")


# --- FASTAPI LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    symbols = read_symbols_from_csv(CSV_PATH)
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)
    start_dt = start_dt.replace(second=0, microsecond=0)
    end_dt = end_dt.replace(second=0, microsecond=0)

    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    async with aiohttp.ClientSession() as session:
        tasks = [
            backfill_symbol_async(symbol, client, write_api, start_dt, end_dt, session)
            for symbol in symbols
        ]
        await asyncio.gather(*tasks)

    client.close()
    print("Backfilling complete.")
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Backfill service active."}
