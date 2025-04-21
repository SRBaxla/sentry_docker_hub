# 🧠 Sentry Docker Hub

A full-stack, containerized, AI-powered trading system with real-time data ingestion, sentiment analysis, model training, trade execution, and backup — all orchestrated with Docker Compose.

---

## 📦 Architecture Overview

This project unifies the following microservices:

- **`binance_collector/`**: Streams OHLCV crypto data, cleans and stores to InfluxDB.
- **`local_news_collector/`**: Fetches financial news, applies FinBERT sentiment analysis, stores insights.
- **`neo4j_sync/`**: Syncs data from InfluxDB to a Neo4j knowledge graph.
- **`model_training/`**: Trains ML models on historical/live price & sentiment data.
- **`inference_service/`**: Runs trained models for real-time decision-making.
- **`trade_agent/`**: Executes simulated trades with balance and tax logic.
- **`backup_service/`**: Periodically backs up InfluxDB and other data volumes.

---

## 🗃 Project Structure

