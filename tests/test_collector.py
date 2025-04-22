import pytest
from httpx import AsyncClient
from binance_collector.collector import app  # adjust path as needed

@pytest.mark.asyncio
async def test_healthcheck():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
