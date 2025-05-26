import os
import logging
from datetime import datetime
from crawler import crawl_all_async
from trust_registry import trust_registry
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from fastapi import FastAPI, BackgroundTasks
import requests
import httpx
import asyncio

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("news_collector")

# --- Environment Setup ---
load_dotenv()
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("META_BUCKET", "CoinMetadata")
NEWS_BUCKET = os.getenv("NEWS_BUCKET", "NewsSentiment")
ANALYZER_URL = os.getenv("ANALYZER_URL", "http://sentiment_analyzer:8004/analyze")

# --- FastAPI Setup ---
app = FastAPI()
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

def get_dynamic_keywords():
    query_api = client.query_api()
    query = f'''
    from(bucket: "{BUCKET}")
    |> range(start: -24h)
    |> filter(fn: (r) => r._measurement == "coin_metadata" and r._field == "keywords")

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
    

def write_news_item(item, sentiment):
    try:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        ts = item.get("published")
        point = (
            Point("news_sentiment")
            .tag("source", item.get("source", "unknown"))
            .tag("source_id", item.get("source_id"))
            .tag("symbol", item.get("symbol"))
            .field("positive", float(sentiment.get("positive", 0)))
            .field("neutral", float(sentiment.get("neutral", 0)))
            .field("negative", float(sentiment.get("negative", 0)))
            .field("trust_score", float(item.get("trust_score", 0.7)))
            .field("headline", item.get("title", ""))
            .field("text", item.get("text", ""))
            .field("author", item.get("author", "unknown"))
            .field("url", item.get("url", ""))
            .time(ts, WritePrecision.S)
        )
        write_api.write(bucket=NEWS_BUCKET, record=point)
        write_api.close()
        logger.info(f"Wrote news for {item.get('symbol')} from {item.get('source_id')} @ {ts}")
    except Exception as e:
        logger.error(f"Error writing news item to InfluxDB: {e}")

async def get_sentiment_async(text: str, host: str = ANALYZER_URL) -> dict:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(host, params={"text": text})
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.error(f"Error during sentiment analysis: {e}")
        return {"error": str(e)}

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

def write_news_to_influx(news_items):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    count = 0
    for item in news_items:
        try:
            source_id = item.get("source_id")
            news_title = item.get("title", "").strip()
            news_text = item.get("text", "").strip()
            sentiment = item.get("sentiment", {})
            ts = item.get("published", datetime.utcnow().isoformat())
            point = (
                Point("news_sentiment")
                .tag("source", item.get("source", "unknown"))
                .tag("source_id", source_id)
                .tag("symbol", item.get("symbol"))
                .field("positive", float(sentiment.get("positive", 0)))
                .field("neutral", float(sentiment.get("neutral", 0)))
                .field("negative", float(sentiment.get("negative", 0)))
                .field("trust_score", float(item.get("trust_score", 0.7)))
                .field("headline", news_title)
                .field("text", news_text)
                .field("author", item.get("author", "unknown"))
                .field("url", item.get("url", ""))
                .time(ts, WritePrecision.S)
            )
            write_api.write(bucket=NEWS_BUCKET, record=point)
            count += 1
        except Exception as e:
            logger.error(f"Error writing news item to InfluxDB: {e}")
    write_api.close()
    logger.info(f"Wrote {count} news items to bucket '{NEWS_BUCKET}'.")

@app.get("/")
def home():
    return {"status": "ok", "msg": "Sentry News Collector"}

@app.get("/health")
def health():
    return {"status": "ok"}

# @app.get("/run_collector")
# def run_collector(background_tasks: BackgroundTasks):
#     search_terms = get_dynamic_keywords()
#     all_items = []

#     for symbol, terms in search_terms.items():
#         for keyword in terms:
#             try:
#                 results = crawl_all(query=keyword, limit=5)
#                 for item in results:
#                     item["symbol"] = symbol
#                     all_items.append(item)
#             except Exception as e:
#                 logger.error(f"Error crawling for keyword '{keyword}': {e}")

#     processed_items = []
#     for item in all_items:
#         source_id = item.get("source_id")
#         if not trust_registry.is_trusted(source_id):
#             logger.warning(f"[SKIP] Untrusted source: {source_id}")
#             continue

#         news_title = item.get("title", "").strip()
#         news_text = item.get("text", "").strip()
#         logger.info(f"Preparing for sentiment analysis:\nHEADLINE: {news_title}\nTEXT: {news_text}")

#         if not news_title or not news_text:
#             logger.warning(f"Skipping item due to missing title or text: {item}")
#             continue

#         sentiment = get_sentiment(news_title + "\n" + news_text)
#         if "error" in sentiment:
#             logger.error(f"Sentiment analysis failed for item: {item.get('url', 'N/A')}")
#             continue

#         item["sentiment"] = sentiment
#         item["trust_score"] = 0.7
#         processed_items.append(item)

#     # Write to InfluxDB in the background for speed
#     background_tasks.add_task(write_news_to_influx, processed_items)

#     return {"status": "completed", "collected": len(processed_items)}
from fastapi import APIRouter

from fastapi import APIRouter
import asyncio

@app.get("/run_collector_async")
async def run_collector_async():
    search_terms = get_dynamic_keywords()
    all_items = []

    # Async crawl for all keywords in parallel
    async def crawl_for_symbol(symbol, keyword):
        try:
            results = await crawl_all_async(query=keyword, limit=5)
            for item in results:
                item["symbol"] = symbol
            return results
        except Exception as e:
            logger.error(f"Error crawling for keyword '{keyword}': {e}")
            return []

    # Gather all crawling tasks
    crawl_tasks = [
        crawl_for_symbol(symbol, keyword)
        for symbol, terms in search_terms.items()
        for keyword in terms
    ]
    crawl_results = await asyncio.gather(*crawl_tasks)
    for results in crawl_results:
        all_items.extend(results)

    logger.info(f"[run_collector_async] Crawled {len(all_items)} total news/reddit items.")

    # Filter and prepare news items
    filtered_items = []
    for item in all_items:
        source_id = item.get("source_id")
        if not trust_registry.is_trusted(source_id):
            logger.warning(f"[SKIP] Untrusted source: {source_id}")
            continue
        news_title = item.get("title", "").strip()
        news_text = item.get("text", "").strip()
        if not news_title or not news_text:
            logger.warning(f"Skipping item due to missing title or text: {item}")
            continue
        item["trust_score"] = 0.7
        filtered_items.append(item)

    logger.info(f"[run_collector_async] {len(filtered_items)} items after filtering.")

    # Sentiment analysis and write, fully async
    async def process_item(item):
        sentiment = await get_sentiment_async(item["title"] + "\n" + item["text"])
        if "error" in sentiment:
            logger.error(f"Sentiment analysis failed for item: {item.get('url', 'N/A')}")
            return
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, write_news_item, item, sentiment)

    await asyncio.gather(*(process_item(item) for item in filtered_items))
    logger.info(f"[run_collector_async] Completed sentiment and write for {len(filtered_items)} items.")
    return {"status": "completed", "collected": len(filtered_items)}
