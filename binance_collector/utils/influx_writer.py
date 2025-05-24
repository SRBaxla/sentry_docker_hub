# influx_writer.py

import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import WriteOptions
from asyncio import get_running_loop
from dotenv import load_dotenv
load_dotenv()
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import InfluxDBClient, BucketsApi

# Thread pool for offloading writes
executor = ThreadPoolExecutor(max_workers=15)

# Pull in your .env settings
INFLUX_URL   = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG   = os.getenv("INFLUX_ORG")
INFLUX_BUCKET= "Sentry"

# Build one shared client + write_api at import time
_client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)
# configure batching options (times are in milliseconds)
_write_opts = WriteOptions(
    write_type="batching",     # pass the literal string
    batch_size=500,
    flush_interval=1_000,      # flush every 5 s
    jitter_interval=2_000,     # random up to 2 s
    retry_interval=5_000,      # retry every 5 s
    max_retries=3,
    max_retry_delay=60_000,    # max backoff between retries
    max_retry_time=300_000,    # overall retry timeout
    max_close_wait=2_000,      # wait up to 2 s on shutdown
)
_write_api = _client.write_api(write_options=_write_opts)


buckets_api = _client.buckets_api()

def get_or_create_bucket(bucket_name):
    buckets = buckets_api.find_buckets().buckets
    for b in buckets:
        if b.name == bucket_name:
            return b
    # If not found, create it
    new_bucket = buckets_api.create_bucket(bucket_name=bucket_name, org=INFLUX_ORG)
    return new_bucket



def get_influx_client():
    """If you still need a raw client elsewhere."""
    return _client



async def async_write_batches(data_points, bucket_name=INFLUX_BUCKET):
    if not data_points:
        return

    async with InfluxDBClientAsync(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api()
        await write_api.write(bucket=bucket_name, record=data_points)
