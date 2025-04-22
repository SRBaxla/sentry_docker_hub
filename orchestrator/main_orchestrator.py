import os
import logging
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
scheduler = BlockingScheduler()

# URLs from .env or fallback
NEWS_URL     = os.getenv("NEWS_URL",     "http://local_news_collector:8001/run_collector")
NEO4J_URL    = os.getenv("NEO4J_URL",    "http://neo4j_sync:8003/sync")
BACKFILL_URL = os.getenv("BACKFILL_URL", "http://binance_collector:8002/backfill")
TRAIN_URL    = os.getenv("TRAIN_URL",    "http://model_training:8005/train")
LIVE_URL     = os.getenv("LIVE_URL",     "http://binance_collector:8002/live")
ANALYZER_URL = os.getenv("ANALYZER_URL", "http://sentiment_analyzer:8004/analyze")

# Pair names with URLs
services = [
    ("News", NEWS_URL),
    ("Neo4j", NEO4J_URL),
    ("Backfill", BACKFILL_URL),
    ("Model", TRAIN_URL),
    ("Live", LIVE_URL),
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def trigger(endpoint: str, name: str):
    try:
        logging.info(f"[{name}] ▶️ GET {endpoint}")
        resp = requests.get(endpoint, timeout=30)
        resp.raise_for_status()
        logging.info(f"[{name}] ✅ {resp.status_code}")
    except Exception as e:
        logging.error(f"[{name}] ❌ {e}")
        raise

def precheck_services(abort_on_failure=True):
    critical_failures = []
    for name, url in services:
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "ok":
                raise ValueError("Non-ok status")
            logging.info(f"[{name}] ✅ Healthy")
        except Exception as e:
            logging.warning(f"[{name}] ⚠️ Unreachable or unhealthy: {e}")
            critical_failures.append(name)

    if critical_failures:
        logging.error(f"🚨 Critical: The following services failed health check: {', '.join(critical_failures)}")
        if abort_on_failure:
            logging.warning("Startup aborted. Fix health checks and retry.")
            return False
        else:
            logging.warning("Continuing startup despite health check failures.")
    return True

# --- Individual Triggers ---
def run_news_collector(): trigger(NEWS_URL, "News")
def run_sentiment_analyzer(): trigger(ANALYZER_URL, "Analyzer")
def run_neo4j_ingestor(): trigger(NEO4J_URL, "Neo4j")
def run_backfill(): trigger(BACKFILL_URL, "Backfill")
def run_model_training(): trigger(TRAIN_URL, "Model")

# --- Cron Schedule ---
scheduler.add_job(run_news_collector,     'cron', minute=0)
scheduler.add_job(run_neo4j_ingestor,     'cron', minute=5)
scheduler.add_job(run_backfill,           'cron', hour=2, minute=30)
scheduler.add_job(run_model_training,     'cron', hour=1, minute=0)

if __name__ == "__main__":
    logging.info("[🧠] Sentry Orchestrator starting scheduler...")

    # 🔁 Change to False to *always continue*
    if precheck_services(abort_on_failure=False):
        scheduler.start()
    else:
        logging.warning("Scheduler aborted due to critical failures.")
