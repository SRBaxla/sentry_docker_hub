import pytest
import httpx

# These should match your Docker Compose internal service URLs
services = [
    ("News",     "http://local_news_collector:8001/health"),
    ("Neo4j",    "http://neo4j_sync:8003/health"),
    ("Binance",  "http://binance_collector:8002/health"),
    ("Model",    "http://model_training:8005/health"),
    ("Infer",    "http://inference_service:5000/health"),
]

@pytest.mark.asyncio
@pytest.mark.parametrize("name, url", services)
async def test_service_health(name, url):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
            assert resp.status_code == 200, f"{name} returned {resp.status_code}"
            assert resp.json().get("status", "").lower() == "ok", f"{name} health not OK"
    except Exception as e:
        pytest.fail(f"{name} ❌ Service unreachable: {e}")
