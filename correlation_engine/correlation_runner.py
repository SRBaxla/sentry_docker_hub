import itertools
import requests
import pandas as pd
from influx_feature_loader import load_feature

FEATURES = ["high", "low", "open", "close", "volume"]

def generate_feature_correlations(symbols, duration="-1m"):
    """
    Generate feature correlations for the provided symbols and duration.
    Sends the resulting correlations to a remote server (e.g., Neo4j).
    """
    for feature in FEATURES:
        # Fetch feature data for each symbol
        series_data = {sym: load_feature(sym, feature, duration) for sym in symbols}
        
        # If series_data is empty or all symbols have missing data, skip processing
        if not series_data or all(len(data) == 0 for data in series_data.values()):
            print(f"Skipping feature {feature} due to missing data.")
            continue
        
        # Create DataFrame, dropping columns with any missing values
        df = pd.DataFrame(series_data).dropna(axis=1, how='any')
        
        if df.empty:
            print(f"Feature {feature} has no valid data for correlation.")
            continue
        
        # Compute the correlation matrix
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
        
        # Assuming you're pushing the correlation data as a POST request with JSON payload
        try:
            response = requests.post('http://your-api-endpoint', json=edges)
            if response.status_code == 200:
                print(f"Successfully pushed correlations for {feature}.")
            else:
                print(f"Failed to push correlations for {feature}. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error while pushing correlations for {feature}: {e}")
