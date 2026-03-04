import uuid

from flame_hub import HubAPIError
import peewee as pw
from starlette import status

from project.dependencies import get_storage_client, get_postgres_db
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

    try:
        assert r.status_code == status.HTTP_502_BAD_GATEWAY
        assert detail_of(r) == "Unexpected response from Hub (status code unknown): 'Test Error'."
    finally:
        test_client.app.dependency_overrides.pop(get_storage_client)


def test_database_exception_handler(monkeypatch, test_client, analysis_id):
    def override_postgres():
        return pw.PostgresqlDatabase("test")

    old_override_postgres = test_client.app.dependency_overrides.get(get_postgres_db, None)
    test_client.app.dependency_overrides[get_postgres_db] = override_postgres

    r = test_client.get(
        "/local/tags",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
    )

    try:
        assert r.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert "Unexpected database error." == detail_of(r)
    finally:
        if old_override_postgres is None:
            test_client.app.dependency_overrides.pop(get_postgres_db)
        else:
            test_client.app.dependency_overrides[get_postgres_db] = old_override_postgres
