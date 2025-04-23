# neo4j_ingestor.py (updated to log each push and handle new symbol-tagged sentiment)
import os
from datetime import timedelta
from dotenv import load_dotenv
from neo4j import GraphDatabase
from influxdb_client import InfluxDBClient
from fastapi import FastAPI
from neo4j import GraphDatabase
import os
from datetime import datetime

load_dotenv()

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASS"))
)

def push_correlations(edges):
    query = """
    UNWIND $edges AS edge
    MERGE (a:Coin {symbol: edge.source})
    MERGE (b:Coin {symbol: edge.target})
    MERGE (a)-[r:CORRELATED {
        type: edge.feature,
        timestamp: $now
    }]->(b)
    SET r.strength = edge.strength
    """
    with driver.session() as session:
        session.run(query, edges=edges, now=datetime.utcnow().isoformat())

def push_sentiment_edges(sentiments):
    query = """
    UNWIND $edges AS edge
    MERGE (c:Coin {symbol: edge.symbol})
    MERGE (m:Market {name: 'Crypto'})
    MERGE (c)-[s:SENTIMENT {
        polarity: edge.polarity,
        source: edge.source,
        timestamp: $now
    }]->(m)
    SET s.score = edge.score
    """
    with driver.session() as session:
        session.run(query, edges=sentiments, now=datetime.utcnow().isoformat())



NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS","sentry123")

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
BUCKET = os.getenv("INFLUX_BUCKET", "Sentry")

app=FastAPI()

# print(INFLUX_ORG)
# print(INFLUX_TOKEN)
# print(INFLUX_URL)
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
def sync(hours: int = 24):
    records = get_sentiment_rows(hours=hours)
    print(f"[⇨] Syncing {len(records)} Influx tables of sentiment to Neo4j...")

    with db.session() as session:
        for table in records:
            for row in table.records:
                try:
                    coin = row["symbol"]
                    source = row["source"]
                    ts = row["_time"].isoformat()
                    positive = float(row["positive"])
                    negative = float(row["negative"])

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
                except Exception as e:
                    print(f"[!] Skipped row due to error: {e}")

    return {"status": "ok", "synced_tables": len(records), "duration_hours": hours}

# if __name__ == "__main__":
#     print("[↪] Fetching recent sentiment snapshots...")
#     records = get_sentiment_rows(hours=24)
#     print("[⇨] Pushing sentiment relationships to Neo4j...")
#     push_to_neo4j(records)
#     print("[✓] Done!")