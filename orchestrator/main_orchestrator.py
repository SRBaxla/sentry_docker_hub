import os
import logging
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
scheduler = BlockingScheduler()

# Docker Compose service URLs
NEWS_URL     = os.getenv("NEWS_URL",     "http://local_news_collector:8001/run_collector")
NEO4J_URL    = os.getenv("NEO4J_URL",    "http://neo4j_sync:8003/sync")
BACKFILL_URL = os.getenv("BACKFILL_URL", "http://binance_collector:8002/backfill")
TRAIN_URL    = os.getenv("TRAIN_URL",    "http://model_training:8005/train")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def post(endpoint: str, name: str):
    try:
        logging.info(f"[{name}] ▶️ POST {endpoint}")
        resp = requests.post(endpoint, timeout=30)
        resp.raise_for_status()
        logging.info(f"[{name}] ✅ {resp.status_code}")
    except Exception as e:
        logging.error(f"[{name}] ❌ {e}")
        raise

def run_news_collector():
    post(NEWS_URL, "News")

def run_neo4j_ingestor():
    post(NEO4J_URL, "Neo4j")

def run_backfill():
    post(BACKFILL_URL, "Backfill")

def run_model_training():
    post(TRAIN_URL, "Model")

# Schedule: cron-style
scheduler.add_job(run_news_collector,     'cron', minute=0)        # Every hour
scheduler.add_job(run_neo4j_ingestor,     'cron', minute=5)        # 5 min past hour
scheduler.add_job(run_backfill,           'cron', hour=2, minute=30)  # Daily
scheduler.add_job(run_model_training,     'cron', hour=1, minute=0)   # Daily

if __name__ == "__main__":
    logging.info("[🧠] Sentry Orchestrator started with retries")
    scheduler.start()
