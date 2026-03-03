import uuid
from datetime import datetime, timezone, timedelta

import pytest
from starlette import status

from tests.common.auth import BearerAuth, issue_client_access_token, issue_access_token
from tests.common.rest import detail_of

endpoints = [
    ("GET", f"/intermediate/{uuid.uuid4()}", "auth.failure"),  # UUID can be arbitrary for auth checks
    ("PUT", "/intermediate", "auth.failure"),
    ("GET", f"/local/{uuid.uuid4()}", "auth.failure"),
    ("PUT", "/local", "auth.failure"),
    ("PUT", "/final", "auth.failure"),
    ("GET", "/local/tags", "auth.failure"),
    ("GET", f"/local/tags/{uuid.uuid4()}", "auth.failure"),
    ("PUT", "/final", "auth.failure"),
    ("PUT", "/final/localdp", "auth.failure"),
]


@pytest.mark.parametrize("method,path,expected_events", endpoints, indirect=["expected_events"])
def test_403_no_auth_header(test_client, method, path, expected_events):
    r = test_client.request(method, path)

    assert r.status_code == status.HTTP_401_UNAUTHORIZED
    assert detail_of(r) == "Not authenticated"


@pytest.mark.parametrize("method,path,expected_events", endpoints, indirect=["expected_events"])
def test_403_jwt_expired(test_client, method, path, expected_events):
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


@pytest.mark.parametrize("method,path,expected_events", endpoints, indirect=["expected_events"])
def test_403_no_client_id_claim(test_client, method, path, expected_events):
    r = test_client.request(method, path, auth=BearerAuth(issue_access_token()))

    assert r.status_code == status.HTTP_403_FORBIDDEN
    assert detail_of(r) == "JWT is malformed"
