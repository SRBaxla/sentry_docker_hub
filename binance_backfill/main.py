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
from typing import List, Optional, Dict
import requests

load_dotenv()

CSV_PATH = "symbols.csv"
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = "Sentry"
INFLUX_BUCKET = "Sentry"
TICKS_MEASUREMENT = "ticks"
CANDLES_MEASUREMENT = "candles"
BINANCE_ENDPOINT = "https://api.binance.com/api/v3/klines"
COINPAPRIKA_API = "https://api.coinpaprika.com/v1/coins"

symbol_metadata: Dict[str, Dict[str, str]] = {}

def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

def load_symbol_metadata() -> None:
    if not os.path.exists(CSV_PATH):
        return
    with open(CSV_PATH, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol_metadata[row['symbol']] = {
                "base_coin": row['base_coin'],
                "quote_coin": row['quote_coin'],
                "project_name": row['project_name']
            }

def update_symbol_metadata(symbol: str, base: str, quote: str, name: str):
    symbol_metadata[symbol] = {
        "base_coin": base,
        "quote_coin": quote,
        "project_name": name
    }

def fetch_project_name_mapping() -> Dict[str, str]:
    try:
        resp = requests.get(COINPAPRIKA_API)
        if resp.status_code == 200:
            coins = resp.json()
            return {
                coin["symbol"]: coin["name"]
                for coin in coins if coin.get("type") == "coin" and coin.get("is_active", True)
            }
    except Exception as e:
        print(f"[ERROR] Fetching project names: {e}")
    return {}

def resolve_symbol_details(symbol: str, project_lookup: Dict[str, str]):
    if symbol in symbol_metadata:
        return
    known_quotes = ["USDT", "BTC", "ETH", "BNB", "TRY", "EUR", "USD", "FDUSD", "USDC", "BRL"]
    for quote in sorted(known_quotes, key=lambda x: -len(x)):
        if symbol.endswith(quote):
            base = symbol[:-len(quote)]
            name = project_lookup.get(base.upper(), base.upper())
            update_symbol_metadata(symbol, base.upper(), quote.upper(), name)
            print(f"[METADATA] Registered new symbol: {symbol} -> {base}/{quote}, project: {name}")
            return
    print(f"[WARN] Could not resolve base/quote for {symbol}")

def to_flux_time(dt: datetime) -> str:
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
            t = record.get_time()
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            return t
    return None

def get_expected_timestamps(start: datetime, end: datetime) -> List[datetime]:
    current = start
    result = []
    while current <= end:
        result.append(current)
        current += timedelta(minutes=1)
    return result

def aggregate_ticks_to_candle(symbol: str, minute: datetime, client: InfluxDBClient) -> Optional[dict]:
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
        return {
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": sum(volumes),
            "timestamp": minute
        }
    return None

async def fetch_coin_creation_date(session, coin_id):
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    try:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                genesis_date_str = data.get("genesis_date")  # e.g. "2013-04-28"
                if genesis_date_str:
                    # Convert to aware datetime at midnight UTC
                    return datetime.strptime(genesis_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            # fallback or no genesis_date provided
            return None
    except Exception as e:
        print(f"[ERROR] Fetching creation date for {coin_id}: {e}")
        return None

import aiohttp

coins_list = []

async def fetch_coingecko_coins_list():
    url = "https://api.coingecko.com/api/v3/coins/list"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                # data is a list of dicts like {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}
                return data
            else:
                print(f"[ERROR] Failed to fetch CoinGecko coins list: HTTP {resp.status}")
                return []
            
def write_candle_to_influx(symbol: str, candle: dict, write_api):
    meta = symbol_metadata.get(symbol, {})
    point = (
        Point("candles")
        .tag("symbol", symbol)
        .tag("base_coin", meta.get("base_coin", ""))
        .tag("quote_coin", meta.get("quote_coin", ""))
        .tag("project_name", meta.get("project_name", ""))
        .field("open", float(candle["open"]))
        .field("high", float(candle["high"]))
        .field("low", float(candle["low"]))
        .field("close", float(candle["close"]))
        .field("volume", float(candle["volume"]))
        .time(candle["timestamp"], WritePrecision.S)
    )
    try:
        write_api.write(bucket="Sentry", org="Sentry", record=[point], write_precision=WritePrecision.S)
        print(f"[WRITE SUCCESS] {symbol} at {candle['timestamp']}")
    except Exception as e:
        print(f"[WRITE ERROR] Failed to write point for {symbol}: {e}")

async def fetch_binance_klines_async(session: aiohttp.ClientSession, symbol: str, start_time: datetime, end_time: datetime) -> list:
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
                print(f"[ERROR] Fetching {symbol}: {response.status} - {await response.text()}")
    except Exception as e:
        print(f"[EXCEPTION] Fetch for {symbol}: {e}")
    return []

def parse_binance_kline(kline, minute: datetime) -> dict:
    return {
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "timestamp": minute
    }

async def fill_gap(symbol: str, minute: datetime, client: InfluxDBClient, write_api, session: aiohttp.ClientSession, fill_semaphore: asyncio.Semaphore):
    async with fill_semaphore:
        try:
            candle = aggregate_ticks_to_candle(symbol, minute, client)
            if candle:
                write_candle_to_influx(symbol, candle, write_api)
                print(f"[FILL] {symbol} {minute}: Filled from ticks.")
                await asyncio.sleep(0.3)  # delay to avoid rate limit
                return

            klines = await fetch_binance_klines_async(session, symbol, minute, minute + timedelta(minutes=1))
            if klines:
                candle = parse_binance_kline(klines[0], minute)
                write_candle_to_influx(symbol, candle, write_api)
                print(f"[FILL] {symbol} {minute}: Filled from Binance.")
            else:
                print(f"[FILL] {symbol} {minute}: No data available from Binance.")
            await asyncio.sleep(0.3)  # delay here too
        except Exception as e:
            print(f"[ERROR] Exception filling {symbol} {minute}: {e}")
            await asyncio.sleep(1)  # longer delay on error

async def backfill_symbol_async(symbol: str, client: InfluxDBClient, write_api, start_dt: datetime, end_dt: datetime, session: aiohttp.ClientSession, fill_semaphore: asyncio.Semaphore):
    expected_minutes = get_expected_timestamps(start_dt, end_dt)
    for chunk in chunked(expected_minutes, 500):
        tasks = [
            fill_gap(symbol, minute, client, write_api, session, fill_semaphore)
            for minute in chunk
        ]
        await asyncio.gather(*tasks)
        await asyncio.sleep(0.5)

def get_coingecko_id(symbol: str) -> str | None:
    symbol_lower = symbol.lower()
    for coin in coins_list:
        if coin['symbol'] == symbol_lower:
            return coin['id']
    return None


        

async def live_tick_processing():
    print("[LIVE] Starting live tick processing from Ticks bucket...")
    # Placeholder: implement your live tick streaming logic here
    while True:
        # Process live ticks here
        await asyncio.sleep(1)  # Adjust frequency as needed

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[LIFESPAN] Starting")

    try:
        global coins_list
        load_symbol_metadata()
        coins_list = await fetch_coingecko_coins_list()
        print(f"[INFO] Loaded {len(coins_list)} coins from CoinGecko")

        project_name_lookup = fetch_project_name_mapping()
        symbol_to_coingecko_id = {coin['symbol'].upper(): coin['id'] for coin in coins_list}

        client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        symbol_semaphore = asyncio.Semaphore(10)
        fill_semaphore = asyncio.Semaphore(20)

        async def throttled_backfill(symbol, start_dt, end_dt, session):
            async with symbol_semaphore:
                print(f"[START] Backfilling {symbol} from {start_dt} to {end_dt}")
                await backfill_symbol_async(symbol, client, write_api, start_dt, end_dt, session, fill_semaphore)
                print(f"[DONE] Backfilled {symbol}")

        async with aiohttp.ClientSession() as session:
            tasks = []
            if os.path.exists(CSV_PATH):
                with open(CSV_PATH, newline='') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        symbol = row["symbol"]
                        resolve_symbol_details(symbol, project_name_lookup)

                        base_coin = symbol.split("BTC")[0]  # TODO: Improve this logic
                        coingecko_id = symbol_to_coingecko_id.get(base_coin)

                        creation_time = None
                        if coingecko_id:
                            creation_time = await fetch_coin_creation_date(session, coingecko_id)

                        latest = get_latest_candle_time(symbol, client)
                        if latest:
                            start_dt = latest + timedelta(minutes=1)
                        elif creation_time:
                            start_dt = creation_time
                        else:
                            start_dt = datetime(2021, 1, 1, tzinfo=timezone.utc)

                        end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)

                        if start_dt < end_dt:
                            tasks.append(throttled_backfill(symbol, start_dt, end_dt, session))

            await asyncio.gather(*tasks)

        client.close()
        print("[LIFESPAN] Backfill complete. App ready.")

    except Exception as e:
        print(f"[ERROR] Exception in lifespan: {e}")
        raise

    yield  # Application runs after this block

    print("[LIFESPAN] Shutting down")

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"message": "Backfill service with automatic symbol metadata enrichment active."}
