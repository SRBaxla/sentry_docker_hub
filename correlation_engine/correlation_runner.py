from influx_feature_loader import load_feature
from neo4j_sync.neo4j_ingestor import push_correlations
import itertools
import requests
import pandas as pd

FEATURES = ["high", "low", "open", "close", "volume"]

def generate_feature_correlations(symbols, duration="-1m"):
    for feature in FEATURES:
        series_data = {sym: load_feature(sym, feature, duration) for sym in symbols}
        df = pd.DataFrame(series_data).dropna(axis=1, how='any')
        corr_matrix = df.corr()

        edges = []
        for a, b in itertools.combinations(df.columns, 2):
            strength = corr_matrix.at[a, b]
            edges.append({
                "source": a,
                "target": b,
                "feature": feature,
                "strength": round(float(strength), 4)
            })
        requests.get(edges)
