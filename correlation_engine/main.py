from fastapi import FastAPI
from correlation_runner import generate_feature_correlations
from utils.symbol_manager import get_active_symbols_from_influx

app = FastAPI()

@app.post("/correlate")
def correlate():
    symbols = get_active_symbols_from_influx()
    generate_feature_correlations(symbols, duration="-1m")
    return {"status": "ok", "symbols_used": symbols}
