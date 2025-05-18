import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
load_dotenv()


import os

ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
# Settings from the environment or defaults
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

CONCURRENCY_LIMIT = 10
BATCH_SIZE = 10

trading_client = TradingClient(ALPACA_API_KEY, ALPACA_SECRET_KEY, paper=True)
stock_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


app = FastAPI()

semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)


def write_to_influx_alpaca(symbol, bars, write_api):
    points = []
    for bar in bars:
        if isinstance(bar, dict):
            ts = datetime.fromisoformat(bar['t'].replace("Z", "+00:00"))
            o, h, l, c, v = bar['o'], bar['h'], bar['l'], bar['c'], bar['v']
        elif isinstance(bar, (list, tuple)):
            ts = datetime.fromisoformat(bar[0].replace("Z", "+00:00"))
            o, h, l, c, v = bar[1:6]
        else:
            raise ValueError(f"Unrecognized bar format: {bar}")

        point = (
            Point("stocks")
            .tag("symbol", symbol)
            .field("open", float(o))
            .field("high", float(h))
            .field("low", float(l))
            .field("close", float(c))
            .field("volume", float(v))
            .time(ts, WritePrecision.S)
        )
        points.append(point)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

async def backfill_symbol(symbol: str, start: datetime, end: datetime):
    async with semaphore:
        print(f"Backfilling {symbol} from {start} to {end}")
        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            start=start,
            end=end,
            timeframe=TimeFrame.Minute
        )
        bars = await asyncio.to_thread(stock_data_client.get_stock_bars, request)
        write_api= client.write_api()
        write_to_influx_alpaca(symbol, bars, write_api)
        for bar in bars.data.get(symbol, []):
            # Replace with InfluxDB write if needed
            print(f"{symbol} - {bar.t}: O:{bar.o}, H:{bar.h}, L:{bar.l}, C:{bar.c}, V:{bar.v}")


async def fetch_all_active_us_equity_symbols():
    request = GetAssetsRequest(status=AssetStatus.ACTIVE)
    assets = trading_client.get_all_assets(request)

    symbols = [asset.symbol for asset in assets]
    return symbols

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting backfill process")

    symbols = await fetch_all_active_us_equity_symbols()
  # run sync method in thread

    start_dt = datetime.now() - timedelta(days=5)
    end_dt = datetime.now() - timedelta(minutes=16)

    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        tasks = [backfill_symbol(sym, start_dt, end_dt) for sym in batch]
        await asyncio.gather(*tasks)

    print("Backfill complete")

    yield

app.router.lifespan_context = lifespan
