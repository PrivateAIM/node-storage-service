import uuid
import os

from starlette import status
import pytest

from project import crud
from project.routers.local import (
    LocalUploadResponse,
)
from tests.common.auth import BearerAuth, issue_client_access_token
from tests.common.helpers import next_random_bytes, eventually, next_prefixed_name
from tests.common.rest import wrap_bytes_for_request, detail_of
from tests.common.env import hub_adapter_client_id


pytestmark = pytest.mark.live


def test_200_submit_receive_from_local(test_client, rng, core_client, project_id, analysis_id, minio, postgres):
    bucket = os.environ.get("MINIO__BUCKET")
    n_objects = len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/")))
    with crud.bind_to(postgres):
        n_results = len(crud.Result.select())
        n_tags = len(crud.Tag.select())
        n_tagged_results = len(crud.TaggedResult.select())

    blob = next_random_bytes(rng)
    r = test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
    )

    assert r.status_code == status.HTTP_200_OK

    # Check that there is exactly one new object inside the MinIO bucket, but no new database entries since the result
    # is untagged.
    assert len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/"))) == n_objects + 1
    with crud.bind_to(postgres):
        assert len(crud.Result.select()) == n_results
        assert len(crud.Tag.select()) == n_tags
        assert len(crud.TaggedResult.select()) == n_tagged_results

    model = LocalUploadResponse(**r.json())
    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token(analysis_id)),
    )

    assert r.status_code == status.HTTP_200_OK
    assert r.read() == blob


def test_404_unknown_oid(test_client, core_client, analysis_id):
    oid = uuid.uuid4()
    r = test_client.get(
        f"/local/{oid}",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Object with ID {oid} does not exist"


def test_200_result_from_another_analysis(test_client, core_client, analysis_id_factory, rng):
    first_analysis_id, second_analysis_id = analysis_id_factory(), analysis_id_factory()

    blob = next_random_bytes(rng)
    r = test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(first_analysis_id)),
        files=wrap_bytes_for_request(blob),
    )

    r = test_client.get(r.json()["url"], auth=BearerAuth(issue_client_access_token(second_analysis_id)))

    assert r.status_code == status.HTTP_200_OK
    assert r.read() == blob


def test_404_result_from_another_project(test_client, core_client, rng, project_id_factory, analysis_id_factory):
    first_analysis_id = analysis_id_factory(project_id_factory())
    second_analysis_id = analysis_id_factory(project_id_factory())

    blob = next_random_bytes(rng)
    r = test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(first_analysis_id)),
        files=wrap_bytes_for_request(blob),
    )

    object_id = r.json()["url"].split("/")[-1]
    r = test_client.get(r.json()["url"], auth=BearerAuth(issue_client_access_token(second_analysis_id)))

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Object with ID {object_id} does not exist"


def test_400_delete_results(test_client, project_id, minio, postgres):
    bucket = os.environ.get("MINIO__BUCKET")

    n_objects = len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/")))
    with crud.bind_to(postgres):
        n_results = len(crud.Result.select())
        n_tags = len(crud.Tag.select())
        n_tagged_results = len(crud.TaggedResult.select())

    r = test_client.delete(
        "/local",
        auth=BearerAuth(issue_client_access_token(hub_adapter_client_id())),
        params={"project_id": project_id},
    )

    assert r.status_code == status.HTTP_400_BAD_REQUEST
    assert detail_of(r) == f"Project '{project_id}' will not be deleted because it is still available on the Hub."

    # Test that nothing was deleted.
    assert len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/"))) == n_objects
    with crud.bind_to(postgres):
        assert len(crud.Result.select()) == n_results
        assert len(crud.Tag.select()) == n_tags
        assert len(crud.TaggedResult.select()) == n_tagged_results


def test_403_delete_results(test_client, project_id, minio, postgres):
    bucket = os.environ.get("MINIO__BUCKET")

    n_objects = len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/")))
    with crud.bind_to(postgres):
        n_results = len(crud.Result.select())
        n_tags = len(crud.Tag.select())
        n_tagged_results = len(crud.TaggedResult.select())

    client_id = str(uuid.uuid4())
    r = test_client.delete(
        "/local",
        auth=BearerAuth(issue_client_access_token(client_id)),
        params={"project_id": project_id},
    )

    assert r.status_code == status.HTTP_403_FORBIDDEN
    assert (
        detail_of(r) == f"Only the Hub Adapter client is allowed to delete local results, got client ID '{client_id}'."
    )

    # Test that nothing was deleted.
    assert len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/"))) == n_objects
    with crud.bind_to(postgres):
        assert len(crud.Result.select()) == n_results
        assert len(crud.Tag.select()) == n_tags
        assert len(crud.TaggedResult.select()) == n_tagged_results


def test_200_delete_results(test_client, core_client, rng, minio, postgres):
    project = core_client.create_project(name=next_prefixed_name())
    analysis = core_client.create_analysis(project_id=project.id, name=next_prefixed_name())

    def _project_and_analysis_exist():
        return core_client.get_project(project.id) is not None and core_client.get_analysis(analysis.id) is not None

    assert eventually(_project_and_analysis_exist)

    blob = next_random_bytes(rng)
    test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(analysis.id)),
        files=wrap_bytes_for_request(blob),
    )

    core_client.delete_analysis(analysis.id)
    core_client.delete_project(project.id)

    with crud.bind_to(postgres):
        n_results = len(crud.Result.select())
        n_tags = len(crud.Tag.select())
        n_tagged_results = len(crud.TaggedResult.select())

    r = test_client.delete(
        "/local",
        auth=BearerAuth(issue_client_access_token(hub_adapter_client_id())),
        params={"project_id": project.id},
    )

    assert r.status_code == status.HTTP_200_OK

    bucket = os.environ.get("MINIO__BUCKET")
    assert len(list(minio.list_objects(bucket, prefix=f"local/{project.id}/"))) == 0

    # Untagged results should not create any entries inside the postgres database.
    with crud.bind_to(postgres):
        assert len(crud.Result.select()) == n_results
        assert len(crud.Tag.select()) == n_tags
        assert len(crud.TaggedResult.select()) == n_tagged_results


def test_200_upload_local_file(test_client, core_client, rng, analysis_id):
    blob = next_random_bytes(rng)
    r = test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
    )

    assert r.status_code == status.HTTP_200_OK

    model = LocalUploadResponse(**r.json())
    object_id = str(model.url).split("/")[-1]

    r = test_client.put(
        "/local/upload",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        params={"object_id": object_id},
    )

    assert r.status_code == status.HTTP_200_OK

    bucket_files = core_client.find_analysis_bucket_files(filter={"analysis_id": analysis_id})

    assert len(bucket_files) == 1
    assert bucket_files[0].name == object_id

    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token(analysis_id)),
    )

    assert r.status_code == status.HTTP_200_OK
    assert r.read() == blob
