# main.py — Sentry Background Orchestrator

import time
from datetime import datetime
from binance_collector.trust_registry import trust_registry

# Import custom run() functions — you must define these inside each respective module
from local_news_collector.collector import run_collector  # should crawl all news & reddit
from binance_collector.binance_collector import run_backfill, run_live
from neo4j_sync.neo4j_ingestor import push_to_neo4j
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))


# Timestamps for throttling
last_news_sync = 0
last_binance_sync = 0
last_neo4j_sync = 0
last_decay = 0

# Frequencies (seconds)
NEWS_INTERVAL = 1800         # 30 mins
BINANCE_INTERVAL = 60        # 1 min
NEO4J_SYNC_INTERVAL = 900    # 15 mins
DECAY_INTERVAL = 86400       # 1 day

print("[🚀] Sentry orchestrator started. Running background tasks...")

while True:
    now = time.time()

    try:
        if now - last_news_sync > NEWS_INTERVAL:
            print(f"[📰] Running sentiment collector at {datetime.utcnow().isoformat()}...")
            run_collector()
            last_news_sync = now

        if now - last_binance_sync > BINANCE_INTERVAL:
            print(f"[📉] Running Binance collector at {datetime.utcnow().isoformat()}...")
            run_binance_collector()
            last_binance_sync = now

        if now - last_neo4j_sync > NEO4J_SYNC_INTERVAL:
            print(f"[🔁] Syncing sentiment to Neo4j at {datetime.utcnow().isoformat()}...")
            push_to_neo4j()
            last_neo4j_sync = now

        if now - last_decay > DECAY_INTERVAL:
            print(f"[💤] Running trust decay at {datetime.utcnow().isoformat()}...")
            trust_registry.decay_scores()
            last_decay = now

    except Exception as e:
        print(f"[❌] Error occurred: {e}")

    time.sleep(5)  # Light polling loop