import uuid
from datetime import datetime, timezone, timedelta

import pytest
from starlette import status

from project.config import Settings
from project.dependencies import get_settings
from tests.common.auth import BearerAuth, issue_client_access_token, issue_access_token
from tests.common.helpers import next_random_string, temporarily_change_dependency
from tests.common.rest import detail_of

endpoints = [
    ("PUT", "/local"),
    ("DELETE", "/local"),
    ("GET", f"/local/{uuid.uuid4()}"),  # UUID can be arbitrary for auth checks.
    ("GET", "/local/tags"),
    ("POST", "/local/tags"),
    ("GET", f"/local/tags/{uuid.uuid4()}"),
    ("PUT", "/local/upload"),
    ("PUT", "/intermediate"),
    ("GET", f"/intermediate/{uuid.uuid4()}"),
    ("PUT", "/final"),
    ("PUT", "/final/localdp"),
]


@pytest.mark.parametrize("method,path", endpoints)
def test_403_no_auth_header(test_client, method, path):
    r = test_client.request(method, path)

    assert r.status_code == status.HTTP_401_UNAUTHORIZED
    assert detail_of(r) == "Not authenticated"


@pytest.mark.parametrize("method,path", endpoints)
def test_403_jwt_expired(test_client, method, path):
    r = test_client.request(
        method,
        path,
        auth=BearerAuth(
            issue_client_access_token(
                issued_at=datetime.now(tz=timezone.utc) - timedelta(hours=1),
                expires_in=timedelta(seconds=1),
            )
        ),
    )

    assert r.status_code == status.HTTP_403_FORBIDDEN
    assert detail_of(r) == "JWT is malformed"


@pytest.mark.parametrize("method,path", endpoints)
def test_403_no_client_id_claim(test_client, method, path):
    r = test_client.request(method, path, auth=BearerAuth(issue_access_token()))

    assert r.status_code == status.HTTP_403_FORBIDDEN
    assert detail_of(r) == "JWT is malformed"


@pytest.mark.parametrize("method,path", endpoints)
def test_502_auth_provider_unavailable(monkeypatch, test_client, method, path):
    monkeypatch.setenv("OIDC__CERTS_URL", f"http://{next_random_string()}")
    reset_settings = temporarily_change_dependency(test_client, dependency=get_settings, callback=lambda: Settings())

    try:
        r = test_client.request(method, path, auth=BearerAuth(issue_client_access_token()))
    finally:
        reset_settings()

    assert r.status_code == status.HTTP_502_BAD_GATEWAY
    assert detail_of(r) == "Auth provider is unavailable", str(r.text)
