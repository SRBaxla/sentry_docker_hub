import os
import time
from datetime import datetime
from crawler import crawl_all
# from sentiment_analyzer.sentiment import analyze_sentiment
# from store_json import store_raw
from trust_registry import trust_registry
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
import requests
load_dotenv()

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")
ANALYZER_URL=os.getenv("ANALYZER_URL","http://sentiment_analyzer:8004/analyze")

app=FastAPI()

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Define keywords per coin
search_terms = {
    "BTCUSDT": ["bitcoin", "btc"],
    "ETHUSDT": ["ethereum", "eth"],
    "SOLUSDT": ["solana", "sol"],
    "XRPUSDT": ["xrp"],
    "DOGEUSDT": ["doge", "dogecoin"]
}

def get_sentiment(text: str, host: str = ANALYZER_URL) -> dict:
    try:
        endpoint = f"{host}"
        params = {"text": text}
        response = requests.get(endpoint, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error during sentiment analysis: {e}")
        return {"error": str(e)}

@app.get("/")
def home():
    return{"status":"ok","msg":"Sentry News Collector"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/run_collector")
def run_collector():
    all_items = []
    for symbol, terms in search_terms.items():
        for keyword in terms:
            results = crawl_all(query=keyword, limit=5)
            for item in results:
                item["symbol"] = symbol
                all_items.append(item)

    for item in all_items:
        source_id = item.get("source_id")
        if not trust_registry.is_trusted(source_id):
            print(f"[SKIP] {source_id} not trusted")
            continue

        sentiment = get_sentiment(item["title"] + "\n" + item.get("text", ""))
        ts = item["published"]

        p = (
            Point("news_sentiment")
            .tag("source", item["source"])
            .tag("symbol", item["symbol"])
            .field("positive", float(sentiment.get("positive", 0)))
            .field("neutral", float(sentiment.get("neutral", 0)))
            .field("negative", float(sentiment.get("negative", 0)))
            .time(ts, WritePrecision.S)
        )

        write_api.write(bucket=BUCKET, record=p)
        print(f"[✓] Collected {item['symbol']} from {item['source_id']} @ {ts}")