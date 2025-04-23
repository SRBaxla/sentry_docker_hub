from fastapi import FastAPI
from pydantic import BaseModel
from sentiment_sync import sync_sentiment_to_graph


app = FastAPI()

# In-memory cache
LATEST_RESULT = []

class Sentiment(BaseModel):
    symbols: list[str]
    polarity: str
    score: float
    source: str = "FinBERT"

@app.post("/sync_sentiment")
def sync_sentiment(items: list[Sentiment]):
    sentiment_edges = []
    for item in items:
        for symbol in item.symbols:
            sentiment_edges.append({
                "symbol": symbol,
                "polarity": item.polarity,
                "score": abs(item.score),
                "source": item.source
            })
    sync_sentiment_to_graph(sentiment_edges)
    return {"status": "success", "edges_created": len(sentiment_edges)}


@app.post("/analyze")
def analyze(data: list[Sentiment]):
    global LATEST_RESULT
    LATEST_RESULT = [d.dict() for d in data]
    return {"status": "ok", "count": len(LATEST_RESULT)}

@app.get("/latest_sentiment")
def latest():
    return LATEST_RESULT