import os
import logging
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.symbol_manager import get_active_symbols_from_influx

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
scheduler = BlockingScheduler()

# --- Service Endpoints ---
SERVICE_ENDPOINTS = {
    "News":         os.getenv("NEWS_URL",     "http://local_news_collector:8001/run_collector"),
    "Neo4j":        os.getenv("NEO4J_URL",    "http://neo4j_sync:8003/sync"),
    "Backfill":     os.getenv("BACKFILL_URL", "http://binance_collector:8002/backfill"),
    "Live":         os.getenv("LIVE_URL",     "http://binance_collector:8002/live"),
    "Analyzer":     os.getenv("ANALYZER_URL", "http://sentiment_analyzer:8004/analyze"),
    "Correlation":  os.getenv("CORR_URL",     "http://correlation-engine:8000/correlate"),
    "Sentiment":    os.getenv("SENTI_URL",    "http://sentiment-engine:8000/sync_sentiment"),
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def trigger_get(endpoint: str, name: str):
    try:
        logging.info(f"[{name}] ▶️ GET {endpoint}")
        resp = requests.get(endpoint, timeout=30)
        resp.raise_for_status()
        logging.info(f"[{name}] ✅ {resp.status_code}")
    except Exception as e:
        logging.error(f"[{name}] ❌ {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def trigger_post(endpoint: str, name: str, payload: dict | list):
    try:
        logging.info(f"[{name}] ▶️ POST {endpoint}")
        resp = requests.post(endpoint, json=payload, timeout=30)
        resp.raise_for_status()
        logging.info(f"[{name}] ✅ {resp.status_code}")
    except Exception as e:
        logging.error(f"[{name}] ❌ {e}")
        raise

@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=5, max=20))
def precheck_services(abort_on_failure=True):
    critical_failures = []
    for name, url in SERVICE_ENDPOINTS.items():
        ping_url = url.replace("/run_collector", "/ping").replace("/sync", "/ping").replace("/correlate", "/ping")
        try:
            resp = requests.get(ping_url, timeout=5)
            if resp.status_code == 200:
                logging.info(f"[{name}] ✅ Healthy")
            else:
                raise Exception(f"{resp.status_code} {resp.text}")
        except Exception as e:
            logging.warning(f"[{name}] ⚠️ Unreachable or unhealthy: {e}")
            critical_failures.append(name)

    if critical_failures:
        logging.error(f"🚨 Critical: The following services failed health check: {', '.join(critical_failures)}")
        if abort_on_failure:
            logging.warning("Startup aborted. Fix health checks and retry.")
            return False
    return True

# --- Job Wrappers ---
def run_news_collector():
    trigger_get(SERVICE_ENDPOINTS["News"], "News")

def run_neo4j_ingestor():
    trigger_get(SERVICE_ENDPOINTS["Neo4j"], "Neo4j")

def run_backfill():
    trigger_get(SERVICE_ENDPOINTS["Backfill"], "Backfill")

def run_live_stream():
    trigger_get(SERVICE_ENDPOINTS["Live"], "Live")

def run_sentiment_analyzer():
    trigger_get(SERVICE_ENDPOINTS["Analyzer"], "Analyzer")

def run_correlation_engine():
    symbols = get_active_symbols_from_influx()
    trigger_post(SERVICE_ENDPOINTS["Correlation"], "Correlation", {
        "symbols": symbols,
        "duration": "-1m"
    })

def run_sentiment_engine():
    try:
        analyzer_output = requests.get(SERVICE_ENDPOINTS["Analyzer"].replace("/analyze", "/latest_sentiment"), timeout=10).json()
        if analyzer_output:
            trigger_post(SERVICE_ENDPOINTS["Sentiment"], "Sentiment", analyzer_output)
        else:
            logging.warning("[Sentiment] ⚠️ No fresh sentiment data available")
    except Exception as e:
        logging.error(f"[Analyzer→Sentiment] ❌ Failed to sync: {e}")

# --- Scheduler ---
scheduler.add_job(run_news_collector,     'cron', minute=0)
scheduler.add_job(run_neo4j_ingestor,     'cron', minute=1)
scheduler.add_job(run_backfill,           'cron', hour=2, minute=30)
scheduler.add_job(run_correlation_engine, 'cron', minute='*/1')
scheduler.add_job(run_sentiment_engine,   'cron', minute='*/2')

# --- Main ---
if __name__ == "__main__":
    logging.info("[🧠] Sentry Orchestrator starting scheduler...")
    if precheck_services(abort_on_failure=False):
        scheduler.start()
    else:
        logging.warning("Scheduler aborted due to critical failures.")
