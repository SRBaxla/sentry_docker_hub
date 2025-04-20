# binance_collector.py
import os
import time
import requests
from datetime import datetime, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
# from trust_registry import get_trusted_sources

load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")

BASE_URL = "https://api.binance.com"
OHLC_ENDPOINT = "/api/v3/klines"

SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "SHIBUSDT", "DOTUSDT"
]

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

def fetch_ohlcv(symbol, interval="1m", start=None, end=None, limit=1000):
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start:
        params["startTime"] = int(start.timestamp() * 1000)
    if end:
        params["endTime"] = int(end.timestamp() * 1000)

    try:
        r = requests.get(BASE_URL + OHLC_ENDPOINT, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] {symbol} fetch failed: {e}")
        return []

def store_to_influx(symbol, ohlcv_data):
    # trusted_sources = get_trusted_sources()  # not used directly here yet, but placeholder for graph-aware push
    for candle in ohlcv_data:
        ts = datetime.utcfromtimestamp(candle[0] / 1000).isoformat()
        p = (
            Point("candles")
            .tag("symbol", symbol)
            .field("open", float(candle[1]))
            .field("high", float(candle[2]))
            .field("low",  float(candle[3]))
            .field("close",float(candle[4]))
            .field("volume", float(candle[5]))
            .time(ts, WritePrecision.S)
        )
        write_api.write(bucket=BUCKET, record=p)
        print(f"[✓] {symbol} @ {ts}")

def run_backfill(days=7):
    interval_minutes = 1
    delta = timedelta(minutes=interval_minutes)
    for symbol in SYMBOLS:
        end = datetime.utcnow()
        start = end - timedelta(days=days)

        while start < end:
            next_end = min(start + delta * 1000, end)
            candles = fetch_ohlcv(symbol, start=start, end=next_end)
            if candles:
                store_to_influx(symbol, candles)
            start += delta * len(candles) if candles else delta * 1000
            time.sleep(0.25)  # avoid rate limits

def run_live():
    for symbol in SYMBOLS:
        data = fetch_ohlcv(symbol, limit=1)
        if data:
            store_to_influx(symbol, data)
    print("[✓] Binance snapshot written")

if __name__ == "__main__":
    print("[MODE] Backfilling historical data...")
    run_backfill(days=7)
    print("[✓] Backfill complete.")
