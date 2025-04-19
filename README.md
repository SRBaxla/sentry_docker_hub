# Sentry Docker Hub

This unified project, **Sentry Docker Hub**, bundles data collection, cleaning, storage, graph syncing, sentiment/news ingestion, model training/inference, trade execution, and backup into a single Docker‑composed solution.

## Project Structure

```
Sentry-Docker-Hub/
├── binance_collector/
│   ├── Dockerfile
│   ├── collector.py
│   ├── symbol_manager.py
│   └── cleaning.py
├── local_news_collector/
│   ├── fetcher.py
│   ├── sentiment.py
│   └── store_json.py
├── neo4j_sync/
│   ├── Dockerfile
│   └── syncer.py
├── model_training/
│   ├── Dockerfile
│   ├── live_train.py
│   └── historical_train.py
├── inference_service/
│   ├── Dockerfile
│   └── serve.py
├── trade_agent/
│   ├── Dockerfile
│   ├── executor.py
│   └── broker_adapter.py
├── influxdb/
│   └── config/
│       ├── influxdb.conf
│       └── backup.sh
├── backup_service/
│   ├── Dockerfile
│   └── backup_collector.py
├── docker-compose.yaml
├── .env
└── README.md
```
