import asyncio
from concurrent.futures import ThreadPoolExecutor
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import os

MAX_WORKERS = 4  # Number of threads
BATCH_SIZE = 5000  # Number of points per write

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = "Sentry"

def get_influx_client():
    return InfluxDBClient(
        url="http://influxdb:8086",  # Change if needed
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )

async def async_write_batches(data_points, bucket_name):
    if not data_points:
        return

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Split into manageable batches
    batches = [data_points[i:i+BATCH_SIZE] for i in range(0, len(data_points), BATCH_SIZE)]

    loop = asyncio.get_event_loop()
    tasks = []

    for batch in batches:
        task = loop.run_in_executor(executor, write_api.write, bucket_name, INFLUX_ORG, batch)
        tasks.append(task)

    await asyncio.gather(*tasks)

    # Clean up
    del batches
    client.close()
