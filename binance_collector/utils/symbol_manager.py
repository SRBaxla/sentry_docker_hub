# from influxdb_client import InfluxDBClient
import os
import requests
# INFLUX_URL = os.getenv("INFLUX_URL")
# INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
# INFLUX_ORG = os.getenv("INFLUX_ORG")
# INFLUX_BUCKET = "Sentry"

# client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
# query_api = client.query_api()

def get_active_binance_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url)
    data = response.json()

    symbols = [
        s['symbol'] for s in data['symbols']
        if s['status'] == 'TRADING'
    ]
    return symbols
    # result = query_api.query_data_frame(flux)
    # return result["_value"].dropna().unique().tolist()
