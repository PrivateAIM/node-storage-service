import string
import uuid
import os

import pytest
from starlette import status

from project import crud
from project.routers.local import (
    is_valid_tag,
    LocalUploadResponse,
    LocalTagListResponse,
    LocalTaggedResultListResponse,
)
from tests.common.auth import BearerAuth, issue_client_access_token
from tests.common.helpers import (
    next_random_bytes,
    eventually,
    next_prefixed_name,
    next_random_string,
    wait_for_analysis_bucket_file,
)
from tests.common.rest import wrap_bytes_for_request, detail_of
from tests.common.env import hub_adapter_client_id

pytestmark = pytest.mark.live

_tag_test_cases = [
    ("", False),
    (" ", False),
    ("-", False),
    ("--", False),
    ("-ab", False),
    ("ab-", False),
    ("-0", False),
    ("0-", False),
    (" -a", False),
    ("a- ", False),
    ("a", True),
    ("0", True),
    ("aa", True),
    ("00", True),
    ("a0", True),
    ("0a", True),
    ("result1", True),
    ("result-1", True),
    ("result--1", True),
    ("a" + "-" * 30 + "a", True),
    ("a" + "-" * 31 + "a", False),
    ("a" * 33, False),
]


@pytest.mark.parametrize("pattern,expected", _tag_test_cases)
def test_is_valid_tag(pattern, expected):
    assert is_valid_tag(pattern) == expected


def test_200_create_tagged_upload(test_client, rng, analysis_id, project_id, core_client, minio, postgres):
    # use global random here to generate different tags for each run
    tag = next_random_string(charset=string.ascii_lowercase)
    filename = str(uuid.uuid4())
    blob = next_random_bytes(rng)
    auth = BearerAuth(issue_client_access_token(analysis_id))

    bucket = os.environ.get("MINIO__BUCKET")
    n_objects = len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/")))
    with crud.bind_to(postgres):
        n_results = len(crud.Result.select())
        n_tags = len(crud.Tag.select())
        n_tagged_results = len(crud.TaggedResult.select())

    r = test_client.put(
        "/local",
        auth=auth,
        files=wrap_bytes_for_request(blob, file_name=filename),
        data={"tag": tag},
    )

    assert r.status_code == status.HTTP_200_OK

    model = LocalUploadResponse(**r.json())
    result_url = model.url

    # Check that there is exactly one new object inside the MinIO bucket and one new entry in each of the database
    # tables.
    assert len(list(minio.list_objects(bucket, prefix=f"local/{project_id}/"))) == n_objects + 1
    with crud.bind_to(postgres):
        assert len(crud.Result.select()) == n_results + 1
        assert len(crud.Tag.select()) == n_tags + 1
        assert len(crud.TaggedResult.select()) == n_tagged_results + 1

        new_results = crud.Result.select().where(crud.Result.client_id == analysis_id)
        assert len(new_results) == 1
        assert str(new_results[0].object_id) == str(result_url).split("/")[-1]
        assert new_results[0].filename == filename

        new_tags = crud.Tag.select().where(crud.Tag.project_id == project_id)
        assert len(new_tags) == 1
        assert new_tags[0].tag_name == tag

    r = test_client.get(
        "/local/tags",
        auth=auth,
    )

    assert r.status_code == status.HTTP_200_OK
    model = LocalTagListResponse(**r.json())
    assert any(tag_obj.name == tag for tag_obj in model.tags)

    r = test_client.get(
        f"/local/tags/{tag}",
        auth=auth,
    )

    assert r.status_code == status.HTTP_200_OK
    model = LocalTaggedResultListResponse(**r.json())

    tagged_result = model.results.pop()
    assert len(model.results) == 0  # check that it is empty after pop()

    assert tagged_result.url == result_url
    assert tagged_result.filename == filename


def test_404_submit_tagged(test_client, rng):
    rand_uuid = str(uuid.uuid4())
    blob = next_random_bytes(rng)

    r = test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(rand_uuid)),
        files=wrap_bytes_for_request(blob),
        data={"tag": "foobar"},
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Analysis with ID {rand_uuid} not found"


def test_404_get_tags(test_client):
    rand_uuid = str(uuid.uuid4())

    r = test_client.get(
        "/local/tags",
        auth=BearerAuth(issue_client_access_token(rand_uuid)),
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Analysis with ID {rand_uuid} not found"


def test_404_get_results_by_tag(test_client):
    rand_uuid = str(uuid.uuid4())

    r = test_client.get(
        # tag doesn't really matter here bc analysis check happens before everything else
        "/local/tags/foobar",
        auth=BearerAuth(issue_client_access_token(rand_uuid)),
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Analysis with ID {rand_uuid} not found"


def test_200_delete_tagged_results(test_client, core_client, rng, minio, postgres):
    project = core_client.create_project(name=next_prefixed_name())
    analysis = core_client.create_analysis(project_id=project.id, name=next_prefixed_name())

    def _project_and_analysis_exist():
        return core_client.get_project(project.id) is not None and core_client.get_analysis(analysis.id) is not None

    assert eventually(_project_and_analysis_exist)

    blob = next_random_bytes(rng)
    tag = next_random_string(charset=string.ascii_lowercase)
    test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(analysis.id)),
        files=wrap_bytes_for_request(blob),
        data={"tag": tag},
    )

    core_client.delete_analysis(analysis.id)
    core_client.delete_project(project.id)

    r = test_client.delete(
        "/local",
        auth=BearerAuth(issue_client_access_token(hub_adapter_client_id())),
        params={"project_id": project.id},
    )

    assert r.status_code == status.HTTP_200_OK

    bucket = os.environ.get("MINIO__BUCKET")
    assert len(list(minio.list_objects(bucket, prefix=f"local/{project.id}/"))) == 0

    with crud.bind_to(postgres):
        assert len(crud.Result.select().where(crud.Result.client_id == analysis.id)) == 0
        assert len(crud.Tag.select().where(crud.Tag.project_id == project.id)) == 0


def test_tag_existing_object(test_client, minio_object, project_id, analysis_id, postgres):
    object_id = minio_object.object_name.split("/")[-1]
    filename = next_random_string()
    tag_name = next_random_string(charset=string.ascii_lowercase)

    r = test_client.post(
        "/local/tags",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        params={"tag_name": tag_name, "object_id": object_id, "filename": filename},
    )

    assert r.status_code == status.HTTP_200_OK
    with crud.bind_to(postgres):
        results = crud.Result.select().where(
            (crud.Result.object_id == object_id) & (crud.Result.client_id == analysis_id)
        )
        assert len(results) == 1
        assert results[0].filename == filename
        tags = crud.Tag.select().where((crud.Tag.project_id == project_id) & (crud.Tag.tag_name == tag_name))
        assert len(tags) == 1

        # For the next test.
        n_results = crud.Result.select().count()
        n_tags = crud.Tag.select().count()
        n_tagged_results = crud.TaggedResult.select().count()

    # The same request should not produce new database entries.
    r = test_client.post(
        "/local/tags",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        params={"tag_name": tag_name, "object_id": object_id, "filename": filename},
    )

    assert r.status_code == status.HTTP_200_OK
    with crud.bind_to(postgres):
        assert n_results == crud.Result.select().count()
        assert n_tags == crud.Tag.select().count()
        assert n_tagged_results == crud.TaggedResult.select().count()

    # There is already an entry with object_id and analysis_id, but with a different filename. Since this produces a
    # database integrity error, a bad request should be returned.
    new_filename = next_random_string()
    r = test_client.post(
        "/local/tags",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        params={"tag_name": tag_name, "object_id": object_id, "filename": new_filename},
    )

    assert r.status_code == status.HTTP_400_BAD_REQUEST
    assert detail_of(r) == (
        f"The object ID {object_id} is already persisted for analysis {analysis_id}, but with a different filename "
        f"than {new_filename}."
    )


def test_200_upload_local_file(test_client, core_client, rng, analysis_id):
    blob = next_random_bytes(rng)
    tag_name = next_random_string(charset=string.ascii_lowercase)
    filename = next_random_string()
    r = test_client.put(
        "/local",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob, file_name=filename),
        data={"tag": tag_name},
    )

    assert r.status_code == status.HTTP_200_OK

    model = LocalUploadResponse(**r.json())

    r = test_client.put(
        "/local/upload",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        params={"object_id": model.object_id},
    )

    assert r.status_code == status.HTTP_200_OK
    assert wait_for_analysis_bucket_file(core_client, analysis_id), "Hub should return one result file."

    analysis_bucket_file = core_client.find_analysis_bucket_files(filter={"analysis_id": analysis_id}).pop()

    assert analysis_bucket_file.path == filename

    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token(analysis_id)),
    )

    assert r.status_code == status.HTTP_200_OK
    assert r.read() == blob
