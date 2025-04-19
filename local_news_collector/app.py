from fastapi import FastAPI, Query, HTTPException
from datetime import datetime
from crawler import crawl_all
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

@app.get("/")
def home():
    return {"status":"ok","msg":"Sentry News Collector"}

@app.get("/crawl")
def crawl(
    query: str,
    limit: int = 10,
    from_date: str = Query(None, description="YYYY-MM-DD"),
    to_date:   str = Query(None, description="YYYY-MM-DD"),
):
    # parse dates
    try:
        fdt = datetime.strptime(from_date, "%Y-%m-%d") if from_date else None
        tdt = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else None
    except ValueError:
        raise HTTPException(400, "Invalid date format; use YYYY-MM-DD")

    results = crawl_all(query, limit, fdt, tdt)
    return {"query": query, "count": len(results), "results": results}
