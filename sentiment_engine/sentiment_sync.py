from neo4j_sync.neo4j_ingestor import push_sentiment_edges
from datetime import datetime

def sync_sentiment_to_graph(sentiment_data):
    """
    sentiment_data = [
        {"symbol": "BTCUSDT", "polarity": "positive", "score": 0.82, "source": "FinBERT"},
        ...
    ]
    """
    push_sentiment_edges(sentiment_data)
