import uuid

import pytest
from starlette import status

from tests.common.auth import issue_client_access_token, BearerAuth
from tests.common.helpers import next_random_bytes
from tests.common.rest import wrap_bytes_for_request, detail_of

pytestmark = pytest.mark.live


def test_200_submit_with_local_dp(test_client, rng, core_client, storage_client, analysis_id):
    # Send a valid numerical file
    raw_value = rng.random()
    blob = str(raw_value).encode("utf-8")
    filename = "test_result.txt"

    # Set parameters for DP
    form_data = {"epsilon": "1.0", "sensitivity": "1.0"}

    r = test_client.put(
        "/final/localdp",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files={"file": (filename, blob, "text/plain")},
        data=form_data,
    )

    assert r.status_code == status.HTTP_204_NO_CONTENT, f"Unexpected status code: {r.status_code}"

    # retrieve result and see if it returned a file with single number
    uploaded_files = core_client.find_analysis_bucket_files(
        filter={"analysis_id": analysis_id, "type": "RESULT"}, sort={"by": "created_at", "order": "descending"}
    )

    assert len(uploaded_files) > 0, "Hub should return at least one result file"
    stored_file = uploaded_files[0]  # Get the most recent file
    stored_content = next(storage_client.stream_bucket_file(stored_file.bucket_file_id))
    assert stored_content != b"", "Result file is empty"
    noisy_value = float(stored_content.decode("utf-8"))
    assert noisy_value != raw_value, "Noisy value should be different from raw value!"


def test_200_submit_to_upload(test_client, rng, core_client, storage_client, analysis_id):
    blob = next_random_bytes(rng)
    r = test_client.put(
        "/final",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
    )

    assert r.status_code == status.HTTP_204_NO_CONTENT

    analysis_bucket_result_files = core_client.find_analysis_bucket_files(
        filter={"analysis_id": analysis_id, "type": "RESULT"}, sort={"by": "created_at", "order": "descending"}
    )

    assert len(analysis_bucket_result_files) > 0, "Hub should return at least one result file"

    # get most recent
    analysis_bucket_result_file = analysis_bucket_result_files[0]
    # retrieve content
    result_file_content = next(storage_client.stream_bucket_file(analysis_bucket_result_file.bucket_file_id))

    # check file contents
    assert result_file_content == blob, "Result file has incorrect content"


def test_404_submit_invalid_id(test_client, rng):
    rand_uuid = str(uuid.uuid4())
    blob = next_random_bytes(rng)

    r = test_client.put(
        "/final",
        auth=BearerAuth(issue_client_access_token(rand_uuid)),
        files=wrap_bytes_for_request(blob),
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Result bucket for analysis with ID {rand_uuid} was not found"
