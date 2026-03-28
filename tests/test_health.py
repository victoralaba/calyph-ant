# tests/test_health.py


async def test_health(http_client):
    response = await http_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["env"] == "development"


async def test_health_ready(http_client):
    response = await http_client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["checks"]["database"] == "ok"
    assert data["checks"]["redis"] == "ok"
