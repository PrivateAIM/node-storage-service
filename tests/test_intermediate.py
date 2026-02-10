import uuid

from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
import pytest
from starlette import status

from project.crypto import decrypt_default, load_ecdh_private_key
from project.routers.intermediate import IntermediateUploadResponse
from tests.common.auth import (
    BearerAuth,
    issue_client_access_token,
    get_test_ecdh_keypair,
)
from tests.common.helpers import (
    next_random_bytes,
    next_uuid,
    next_ecdh_keypair_bytes,
)
from tests.common.rest import wrap_bytes_for_request, detail_of

pytestmark = pytest.mark.live


@pytest.fixture()
def node(core_client, realm_id):
    node = core_client.create_node(name=next_uuid(), realm_id=realm_id, node_type="default")
    yield node
    core_client.delete_node(node.id)


@pytest.fixture()
def this_node(core_client, realm_id):
    node = core_client.create_node(name=next_uuid(), realm_id=realm_id, node_type="default")
    _, public_key = get_test_ecdh_keypair()
    # Also update node reference.
    node = core_client.update_node(
        node, public_key=public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).decode("ascii")
    )
    yield node
    core_client.delete_node(node.id)


@pytest.fixture()
def remote_node_and_private_key(core_client, realm_id):
    node = core_client.create_node(name=next_uuid(), realm_id=realm_id, node_type="default")
    private_key, public_key = next_ecdh_keypair_bytes()
    # Also update node reference.
    node = core_client.update_node(node, public_key=public_key.decode("ascii"))
    yield node, private_key
    core_client.delete_node(node.id)


def test_200_submit_receive_intermediate(test_client, rng, analysis_id, core_client):
    blob = next_random_bytes(rng)
    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
    )

    assert r.status_code == status.HTTP_200_OK

    # Check that the response contains a path to a valid resource.
    model = IntermediateUploadResponse(**r.json())
    assert str(model.object_id) in str(model.url.path)

    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token()),
    )

    assert r.status_code == status.HTTP_200_OK
    assert r.read() == blob


def test_200_submit_receive_intermediate_encrypted(
    test_client, core_client, rng, analysis_id, remote_node_and_private_key
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

    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token()),
    )

    _, public_key = get_test_ecdh_keypair()

    assert blob == decrypt_default(load_ecdh_private_key(remote_private_key), public_key, r.read())


def test_400_submit_encrypted_no_remote_public_key(test_client, rng, analysis_id, node, core_client):
    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(next_random_bytes(rng)),
        data={
            "remote_node_id": str(node.id),
        },
    )

    assert r.status_code == status.HTTP_400_BAD_REQUEST
    assert detail_of(r) == f"Remote node with ID {node.id} does not provide a public key"


def test_404_invalid_id(test_client):
    rand_uuid = str(uuid.uuid4())
    r = test_client.get(
        f"/intermediate/{rand_uuid}",
        auth=BearerAuth(issue_client_access_token()),
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Object with ID {rand_uuid} does not exist"


def test_404_no_remote_node(test_client, analysis_id, core_client, rng):
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


def test_404_submit_invalid_id(test_client, rng):
    rand_uuid = str(uuid.uuid4())

    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(rand_uuid)),
        files=wrap_bytes_for_request(next_random_bytes(rng)),
    )

    assert r.status_code == status.HTTP_404_NOT_FOUND
    assert detail_of(r) == f"Temp bucket for analysis with ID {rand_uuid} was not found"


def test_400_decrypt_intermediate(
    test_client,
    core_client,
    analysis_id,
    this_node,
    remote_node_and_private_key,
    node,
    rng,
    realm_id,
):
    blob = next_random_bytes(rng)
    r = test_client.put(
        "/intermediate",
        auth=BearerAuth(issue_client_access_token(analysis_id)),
        files=wrap_bytes_for_request(blob),
        data={
            "remote_node_id": str(remote_node_and_private_key[0].id),
        },
    )

    assert r.status_code == status.HTTP_200_OK

    # Check that the response contains a path to a valid resource.
    model = IntermediateUploadResponse(**r.json())
    assert str(model.object_id) in str(model.url.path)

    _, arbitrary_public_key = next_ecdh_keypair_bytes()
    core_client.update_node(node.id, public_key=arbitrary_public_key.decode("ascii"))

    r = test_client.get(
        model.url.path,
        auth=BearerAuth(issue_client_access_token()),
        params={"node_id": node.id},
    )

    assert r.status_code == status.HTTP_400_BAD_REQUEST
    assert detail_of(r) == (
        f"File with ID {model.object_id} cannot be decrypted under the assumption that the file was encrypted by node "
        f"{node.id} for this node."
    )
