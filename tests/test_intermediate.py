import uuid

import pytest
from starlette import status

from project.dependencies import get_ecdh_private_key
from project.routers.intermediate import IntermediateUploadResponse
from tests.common.auth import (
    BearerAuth,
    issue_client_access_token,
)
from tests.common.helpers import (
    next_random_bytes,
    next_uuid,
    temporarily_change_dependency,
)
from tests.common.rest import wrap_bytes_for_request, detail_of

pytestmark = pytest.mark.live


@pytest.mark.parametrize(
    "expected_events",
    [("intermediate.put.success", "intermediate.object.get.success")],
    indirect=True,
)
def test_200_encrypt_and_decrypt(
    test_client, core_client, storage_client, rng, analysis_id, remote_node_and_private_key, this_node, expected_events
):
    remote_node, remote_private_key = remote_node_and_private_key
    blob = next_random_bytes(rng)
    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
        data={
            "remote_node_id": str(remote_node.id),
        },
    )

    assert r.status_code == status.HTTP_200_OK

    model = IntermediateUploadResponse(**r.json())

    # Temporarily change the private key to simulate another node to be able to decrypt data.
    reset_private_key = temporarily_change_dependency(test_client, get_ecdh_private_key, lambda: remote_private_key)

    try:
        r = test_client.get(
            f"{model.url.path}?{model.url.query}",
            auth=BearerAuth(issue_client_access_token(analysis_id)),
        )
    finally:
        reset_private_key()

    assert r.status_code == status.HTTP_200_OK, str(r.text)
    assert blob == r.read()
    assert storage_client.get_bucket_file(bucket_file_id=model.object_id) is None, (
        "File was not deleted from the Hub after its retrieval."
    )


@pytest.mark.parametrize("expected_events", ["intermediate.put.failure"], indirect=True)
def test_400_submit_encrypted_no_remote_public_key(
    test_client,
    rng,
    analysis_id,
    core_client,
    realm_id,
    expected_events,
):
    node = core_client.create_node(name=next_uuid(), realm_id=realm_id, node_type="default")

    try:
        r = test_client.put(
            "/intermediate",
            auth=BearerAuth(issue_client_access_token(analysis_id)),
            files=wrap_bytes_for_request(next_random_bytes(rng)),
            data={
                "remote_node_id": str(node.id),
            },
        )
    finally:
        core_client.delete_node(node.id)

    assert r.status_code == status.HTTP_400_BAD_REQUEST
    assert detail_of(r) == f"Remote node with ID {node.id} does not provide a public key"


@pytest.mark.parametrize("expected_events", ["intermediate.object.get.failure"], indirect=True)
def test_404_invalid_id(test_client, expected_events):
    rand_uuid = str(uuid.uuid4())
    r = test_client.get(
        f"/intermediate/{rand_uuid}",
        auth=BearerAuth(issue_client_access_token()),
        params={"remote_node_id": str(uuid.uuid4())},
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Object with ID {rand_uuid} does not exist"


@pytest.mark.parametrize("expected_events", ["intermediate.put.failure"], indirect=True)
def test_404_no_remote_node(test_client, analysis_id, core_client, rng, expected_events):
    rand_uuid = str(uuid.uuid4())
    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(next_random_bytes(rng)),
        data={
            "remote_node_id": rand_uuid,
        },
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Remote node with ID {rand_uuid} does not exist."


@pytest.mark.parametrize("expected_events", ["intermediate.put.failure"], indirect=True)
def test_404_submit_invalid_client_id(test_client, rng, expected_events):
    rand_uuid = str(uuid.uuid4())

    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(rand_uuid)),
        files=wrap_bytes_for_request(next_random_bytes(rng)),
        data={
            "remote_node_id": str(uuid.uuid4()),
        },
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Temp bucket for analysis with ID {rand_uuid} was not found"


@pytest.mark.parametrize(
    "expected_events",
    [("intermediate.put.success", "intermediate.object.get.failure")],
    indirect=True,
)
def test_400_decrypt_intermediate(
    test_client,
    core_client,
    analysis_id,
    this_node,
    remote_node_and_private_key,
    rng,
    realm_id,
    expected_events,
):
    blob = next_random_bytes(rng)
    remote_node, _ = remote_node_and_private_key
    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
        data={
            "remote_node_id": str(remote_node.id),
        },
    )

    assert r.status_code == status.HTTP_200_OK

    # Check that the response contains a path to a valid resource.
    model = IntermediateUploadResponse(**r.json())
    assert str(model.object_id) in str(model.url.path)

    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token()),
        params={"remote_node_id": this_node.id},
    )

    # The file is encrypted for a remote node and therefore cannot be decrypted by the node that encrypted the file
    # and of course all other nodes except that one remote node. Note that the local private key is not replaced.
    assert r.status_code == status.HTTP_400_BAD_REQUEST
    assert detail_of(r) == (
        f"File with ID {model.object_id} cannot be decrypted under the assumption that the file was encrypted by node "
        f"{this_node.id} for this node."
    )
