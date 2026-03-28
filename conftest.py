# conftest.py
import pytest
import httpx

BASE_URL = "http://localhost:8000"


@pytest.fixture()
def base_url():
    return BASE_URL


@pytest.fixture()
async def http_client():
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        yield client


@pytest.fixture()
async def auth_headers(http_client):
    """
    Log in as the seeded super admin and return auth headers.
    """
    response = await http_client.post("/auth/login", json={
        "email": "victorallentech@gmail.com",
        "password": "FirstTestUser100!"
    })
    assert response.status_code == 200, f"Login failed: {response.text}"
    token = response.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}
