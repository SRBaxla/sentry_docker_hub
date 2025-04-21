import os
import logging
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO)
scheduler = BlockingScheduler()

# Read service URLs from env (set these in your docker‑compose)
NEWS_URL      = os.getenv("NEWS_URL",      "http://localhost:8001/run_collector")
NEO4J_URL     = os.getenv("NEO4J_URL",     "http://localhost:8003/sync")
BACKFILL_URL  = os.getenv("BACKFILL_URL",  "http://localhost:8002/backfill")
LIVE_URL      = os.getenv("LIVE_URL",      "http://localhost:8002/live")
TRAIN_URL     = os.getenv("TRAIN_URL",     "http://localhost:8005/train")

def post(endpoint: str, name: str):
    try:
        logging.info(f"[{name}] POST {endpoint}")
        resp = requests.get(endpoint, timeout=30)
        resp.raise_for_status()
        logging.info(f"[{name}] ✅ {resp.status_code}")
    except Exception as e:
        logging.error(f"[{name}] ❌ {e}")

def run_news_collector():
    post(NEWS_URL, "News")

def run_neo4j_ingestor():
    post(NEO4J_URL, "Neo4j")

def run_backfill():
    post(BACKFILL_URL, "Backfill")

def run_model_training():
    post(TRAIN_URL, "Model")

# ─── Schedule ────────────────────────────────────────────────────────────
# Every hour on the hour
scheduler.add_job(run_news_collector, 'cron', minute=0)
# 5 minutes past each hour
scheduler.add_job(run_neo4j_ingestor,   'cron', minute=5)
# Daily backfill at 02:30
scheduler.add_job(run_backfill,         'cron', hour=2, minute=30)
# Nightly training at 01:00
scheduler.add_job(run_model_training,   'cron', hour=1, minute=0)

if __name__ == "__main__":
    logging.info("[🧠] Sentry Orchestrator started (HTTP mode)")
    scheduler.start()
