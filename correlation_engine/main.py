from fastapi import FastAPI
from correlation_runner import generate_feature_correlations
from ..utils.symbol_manager import get_all_active_symbols

app = FastAPI()

@app.post("/correlate")
def correlate():
    symbols = get_all_active_symbols()
    generate_feature_correlations(symbols, duration="-1m")
    return {"status": "ok", "symbols_used": symbols}
