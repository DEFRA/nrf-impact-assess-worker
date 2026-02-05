from fastapi.testclient import TestClient

from app.common.http_client import create_async_client
from app.main import app

client = TestClient(app)


def test_root_success():
    response = client.get("/example/test")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_http_query_success(mocker):
    mock_client = mocker.AsyncMock()
    mock_client.get.return_value.status_code = 200

    app.dependency_overrides[create_async_client] = lambda: mock_client

    try:
        response = client.get("/example/http")

        assert response.status_code == 200
        assert response.json() == {"ok": 200}
    finally:
        app.dependency_overrides = {}
