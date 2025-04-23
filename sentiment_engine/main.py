from fastapi import FastAPI
from pydantic import BaseModel
from sentiment_sync import sync_sentiment_to_graph

app = FastAPI()

class AnalyzerOutput(BaseModel):
    symbols: list[str]
    polarity: str
    score: float
    source: str = "FinBERT"

@app.post("/sync_sentiment")
def sync_sentiment(items: list[AnalyzerOutput]):
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
