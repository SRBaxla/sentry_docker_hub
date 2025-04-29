import os
import asyncio
from fastapi import FastAPI, BackgroundTasks, Query
from contextlib import asynccontextmanager

# Import existing modules
from influx_feature_loader import load_feature
from correlation_runner import generate_feature_correlations
from metadata_generator import (
    main as run_price_correlation_and_metadata,
    fetch_symbols,
    write_symbol_metadata
)

# Configuration via environment
FEATURE_DURATION     = os.getenv("FEATURE_DURATION", "-1m")
FEATURE_CORR_CUTOFF  = os.getenv("FEATURE_CORR_CUTOFF", "-1m")
PRICE_CORR_WINDOW    = os.getenv("PRICE_CORR_WINDOW", "60m")

# Interval settings for periodic tasks (in minutes)
FEATURE_INTERVAL = int(os.getenv("FEATURE_INTERVAL_MINUTES", "15"))
PRICE_INTERVAL   = int(os.getenv("PRICE_INTERVAL_MINUTES", "60"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    On startup, schedule periodic feature and price correlation tasks.
    """
    # Schedule periodic feature correlation
    asyncio.create_task(periodic_feature_loop(FEATURE_INTERVAL))
    # Schedule periodic price correlation + metadata
    asyncio.create_task(periodic_price_loop(PRICE_INTERVAL))
    yield
    # Optional cleanup can go here

app = FastAPI(lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/features/correlate")
async def feature_correlate(
    duration: str = Query(FEATURE_DURATION, description="Flux duration (e.g. -1m, -1h)"),
    background: BackgroundTasks = None
):
    """
    Generate feature-based correlations for all symbols within the given duration.
    """
    symbols = list(fetch_symbols())
    background.add_task(generate_feature_correlations, symbols, duration)
    return {"message": f"Feature correlation started for {len(symbols)} symbols, duration={duration}"}

@app.post("/prices/correlate")
async def price_correlate(background: BackgroundTasks = None):
    """
    Compute pairwise price correlations and generate keyword metadata.
    """
    background.add_task(run_price_correlation_and_metadata)
    symbols = list(fetch_symbols())
    background.add_task(write_symbol_metadata, symbols)
    return {"message": "Price correlation and keyword metadata generation started."}

async def periodic_feature_loop(interval_minutes: int):
    while True:
        symbols = list(fetch_symbols())
        await asyncio.to_thread(generate_feature_correlations, symbols, FEATURE_CORR_CUTOFF)
        await asyncio.sleep(interval_minutes * 60)

async def periodic_price_loop(interval_minutes: int):
    while True:
        await asyncio.to_thread(run_price_correlation_and_metadata)
        symbols = list(fetch_symbols())
        await asyncio.to_thread(write_symbol_metadata, symbols)
        await asyncio.sleep(interval_minutes * 60)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("aggregated_service:app", host="0.0.0.0", port=8001, reload=True)
