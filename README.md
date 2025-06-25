# 🧠 Sentry Docker Hub (On-going,On-hold)

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

```
sentry_docker_hub/
├── binance_collector/
├── local_news_collector/
├── neo4j_sync/
├── model_training/
├── inference_service/
├── trade_agent/
├── backup_service/
├── docker-compose.yaml
├── .env.example
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Clone the Repo
```bash
git clone https://github.com/SRBaxla/sentry_docker_hub.git
cd sentry_docker_hub
```

### 2. Configure Environment Variables
Create a `.env` file or copy `.env.example` and populate with your values:
```bash
cp .env.example .env
```

Example:
```
INFLUX_URL=http://influxdb:8086
INFLUX_TOKEN=your_token
INFLUX_ORG=sentry
```

### 3. Start Services
```bash
docker-compose up --build
```

---

## 📈 Services & Endpoints

| Service              | URL (Default)             | Description                            |
|----------------------|---------------------------|----------------------------------------|
| Binance Collector    | `:8001/health`            | Streams & stores 1s OHLCV data         |
| News Collector       | Local scheduler triggered | Scrapes + classifies financial news    |
| Neo4j Sync           | N/A                       | Syncs data to Neo4j graph              |
| Inference API        | `:8002/predict`           | Model inference endpoint               |
| Trade Agent          | `:8003/execute`           | Simulated trade engine                 |
| InfluxDB             | `:8086`                   | Time-series data storage               |
| Neo4j                | `:7474`, `:7687`          | Graph database                         |

---

## 🧪 Health Check & Testing

To check if services are live:

```bash
curl http://localhost:8001/health
```

You can add more `/health` endpoints per service for full observability.

---

## 🧠 Future Enhancements

- [ ] Add unit & integration tests
- [ ] Deploy to Kubernetes (Helm charts)
- [ ] Integrate Grafana dashboards
- [ ] Real-time anomaly detection

---

## 🤝 Contributions

Contributions are welcome! Feel free to fork and raise a pull request.

---

## 📜 License

MIT © 2025 [Sudeep Richard Baxla](https://github.com/SRBaxla)
