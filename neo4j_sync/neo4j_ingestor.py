# neo4j_ingestor.py (updated to log each push and handle new symbol-tagged sentiment)
import os
from datetime import timedelta
from dotenv import load_dotenv
from neo4j import GraphDatabase
from influxdb_client import InfluxDBClient
from fastapi import FastAPI

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")

app=FastAPI()

print(INFLUX_ORG)
print(INFLUX_TOKEN)
print(INFLUX_URL)
# Clients
db = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = influx.query_api()

def get_sentiment_rows(hours=24):
    flux = f'''
    from(bucket: "{BUCKET}")
      |> range(start: -{hours}h)
      |> filter(fn: (r) => r["_measurement"] == "news_sentiment")
      |> filter(fn: (r) => r["_field"] == "positive" or r["_field"] == "negative")
      |> keep(columns: ["_time", "_field", "_value", "symbol", "source"])
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    return query_api.query(flux)

@app.get("/")
def home():
    return{"status":"ok","msg":"Neo4j Synchronizor"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/sync")
def push_to_neo4j(records):
    with db.session() as session:
        for table in records:
            for row in table.records:
                print(row)
                coin = row["symbol"]
                source = row["source"]
                ts = row["_time"].isoformat()
                positive = float(row["positive"])
                negative = float(row["negative"])


                print(f"[→] {source} → {coin} | +{positive:.3f} / -{negative:.3f} @ {ts}")

                session.run('''
                    MERGE (c:Coin {symbol: $coin})
                    MERGE (s:Source {name: $source})
                    CREATE (s)-[:INFLUENCES {
                        at: datetime($time),
                        positive: $pos,
                        neutral: 0.0,
                        negative: $neg
                    }]->(c)
                ''', {
                    "coin": coin,
                    "source": source,
                    "time": ts,
                    "pos": positive,
                    "neg": negative
                })

# if __name__ == "__main__":
#     print("[↪] Fetching recent sentiment snapshots...")
#     records = get_sentiment_rows(hours=24)
#     print("[⇨] Pushing sentiment relationships to Neo4j...")
#     push_to_neo4j(records)
#     print("[✓] Done!")