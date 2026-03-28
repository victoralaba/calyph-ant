# tests/test_auth.py

ADMIN_EMAIL = "victorallentech@gmail.com"
ADMIN_PASSWORD = "FirstTestUser100!"


async def test_login_success(http_client):
    response = await http_client.post("/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    })
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"


async def test_login_wrong_password(http_client):
    response = await http_client.post("/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": "wrongpassword"
    })
    assert response.status_code == 401


async def test_login_unknown_email(http_client):
    response = await http_client.post("/auth/login", json={
        "email": "nobody@example.com",
        "password": "whatever"
    })
    assert response.status_code == 401


async def test_protected_route_without_token(http_client):
    response = await http_client.get("/api/v1/users/me")
    assert response.status_code == 401


async def test_protected_route_with_token(http_client, auth_headers):
    response = await http_client.get("/api/v1/users/me", headers=auth_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["email"] == ADMIN_EMAIL
