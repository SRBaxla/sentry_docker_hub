import os
import time
import logging
from datetime import datetime
from crawler import crawl_all
from trust_registry import trust_registry
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from fastapi import FastAPI
import requests

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("news_collector")

# --- Environment Setup ---
load_dotenv()
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")
ANALYZER_URL = os.getenv("ANALYZER_URL", "http://sentiment_analyzer:8004/analyze")

# --- FastAPI Setup ---
app = FastAPI()

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# --- Helper Functions ---
def get_dynamic_keywords():
    query_api = client.query_api()
    query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "symbol_metadata" and r._field == "keywords")
      |> last()
    '''
    try:
        result = query_api.query(query=query)
        keywords_by_symbol = {}
        for table in result:
            for record in table.records:
                symbol = record.values.get("symbol") or "unknown"
                raw_keywords = record.get_value()
                if isinstance(raw_keywords, str):
                    keywords_by_symbol[symbol] = [kw.strip() for kw in raw_keywords.split(',')]
        logger.info(f"Fetched dynamic keywords: {keywords_by_symbol}")
        return keywords_by_symbol
    except Exception as e:
        logger.error(f"Error fetching dynamic keywords: {e}")
        return {}

def get_sentiment(text: str, host: str = ANALYZER_URL) -> dict:
    try:
        endpoint = f"{host}"
        params = {"text": text}
        response = requests.get(endpoint, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error during sentiment analysis: {e}")
        return {"error": str(e)}

# --- API Endpoints ---
@app.get("/")
def home():
    return {"status": "ok", "msg": "Sentry News Collector"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/run_collector")
def run_collector():
    search_terms = get_dynamic_keywords()
    all_items = []

    for symbol, terms in search_terms.items():
        for keyword in terms:
            try:
                results = crawl_all(query=keyword, limit=5)
                for item in results:
                    item["symbol"] = symbol
                    all_items.append(item)
            except Exception as e:
                logger.error(f"Error crawling for keyword '{keyword}': {e}")

    for item in all_items:
        source_id = item.get("source_id")
        if not trust_registry.is_trusted(source_id):
            logger.warning(f"[SKIP] Untrusted source: {source_id}")
            continue

        news_title = item.get("title", "").strip()
        news_text = item.get("text", "").strip()
        logger.info(f"Preparing for sentiment analysis:\nHEADLINE: {news_title}\nTEXT: {news_text}")

        # Inspect the text for cleanliness
        if not news_title or not news_text:
            logger.warning(f"Skipping item due to missing title or text: {item}")
            continue

        sentiment = get_sentiment(news_title + "\n" + news_text)
        if "error" in sentiment:
            logger.error(f"Sentiment analysis failed for item: {item.get('url', 'N/A')}")
            continue

        ts = item.get("published")
        try:
            p = (
                Point("news_sentiment")
                .tag("source", item.get("source", "unknown"))
                .tag("symbol", item["symbol"])
                .field("positive", float(sentiment.get("positive", 0)))
                .field("neutral", float(sentiment.get("neutral", 0)))
                .field("negative", float(sentiment.get("negative", 0)))
                .field("trust_score", float(0.7))
                .field("author", item.get("author", "unknown"))
                .time(ts, WritePrecision.S)
            )
            write_api.write(bucket=BUCKET, record=p)
            logger.info(f"[✓] Collected {item['symbol']} from {item['source_id']} @ {ts}")
        except Exception as e:
            logger.error(f"Error writing to InfluxDB for item: {item.get('url', 'N/A')} | Error: {e}")

    return {"status": "completed", "collected": len(all_items)}

