import uuid

from flame_hub import HubAPIError
from starlette import status

from project.dependencies import get_storage_client
from project.server import load_pyproject
from tests.common.auth import BearerAuth, issue_client_access_token
from tests.common.rest import detail_of


def test_load_pyproject():
    # should parse correctly
    _ = load_pyproject()


def test_hub_api_exception_handler(monkeypatch, test_client, storage_client, analysis_id):
    def raise_error(_):
        raise HubAPIError(
            message="Test Error",
            request=None,
        )

    monkeypatch.setattr(storage_client, "get_bucket_file", raise_error)

    test_client.app.dependency_overrides[get_storage_client] = lambda: storage_client

    r = test_client.get(
        f"/intermediate/{uuid.uuid4()}",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
    )

    assert r.status_code == status.HTTP_502_BAD_GATEWAY
    assert detail_of(r) == "Unexpected response from Hub (status code unknown): 'Test Error'."

    test_client.app.dependency_overrides.pop(get_storage_client)
