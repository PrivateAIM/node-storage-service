import logging
import uuid
from typing import Annotated

import flame_hub
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi import APIRouter, UploadFile, Depends, HTTPException, File, Form
from pydantic import BaseModel, HttpUrl
from starlette import status
from starlette.requests import Request
from starlette.responses import StreamingResponse

from project import crypto
from project.config import Settings
from project.dependencies import (
    get_client_id,
    get_core_client,
    get_storage_client,
    get_ecdh_private_key,
    get_node_id,
    get_settings,
)

router = APIRouter()
logger = logging.getLogger(__name__)


class IntermediateUploadResponse(BaseModel):
    url: HttpUrl
    object_id: uuid.UUID


def get_remote_node_public_key(core_client: flame_hub.CoreClient, remote_node_id: str):
    # fetch remote node
    remote_node = core_client.get_node(remote_node_id)

    # Check if a node with this id exists.
    if remote_node is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Remote node with ID {remote_node_id} does not exist.",
        )

    # check if it has a public key assigned to it
    if remote_node.public_key is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Remote node with ID {remote_node_id} does not provide a public key",
        )

    # construct public key
    return crypto.load_ecdh_public_key_from_hex_string(remote_node.public_key)


@router.put(
    "",
    response_model=IntermediateUploadResponse,
    summary="Upload file as intermediate result to Hub",
    operation_id="putIntermediateResult",
    name="intermediate.put",
)
async def submit_intermediate_result_to_hub(
    client_id: Annotated[str, Depends(get_client_id)],
    file: Annotated[UploadFile, File()],
    request: Request,
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    storage_client: Annotated[flame_hub.StorageClient, Depends(get_storage_client)],
    private_key: Annotated[ec.EllipticCurvePrivateKey, Depends(get_ecdh_private_key)],
    node_id: Annotated[uuid.UUID, Depends(get_node_id)],
    remote_node_id: Annotated[str, Form()],
):
    """Upload a file as an intermediate result to the FLAME Hub.
    Returns a 200 on success.
    This endpoint uploads the file and returns a link with which it can be retrieved."""

    analysis_bucket_lst = core_client.find_analysis_buckets(filter={"analysis_id": client_id, "type": "TEMP"})

    if len(analysis_bucket_lst) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Temp bucket for analysis with ID {client_id} was not found",
        )

    analysis_bucket = analysis_bucket_lst.pop()

    # Get the public key of the remote node via the Hub.
    remote_public_key = get_remote_node_public_key(core_client, remote_node_id)

    bucket_file_lst = storage_client.upload_to_bucket(
        analysis_bucket.bucket_id,
        {
            "file_name": file.filename,
            "content": crypto.AESGCMEncryptingStream(
                file=file.file,
                private_key=private_key,
                remote_public_key=remote_public_key,
            ),
            "content_type": file.content_type or "application/octet-stream",
        },
    )

    if len(bucket_file_lst) != 1:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Expected single uploaded file to be returned by storage service, got {len(bucket_file_lst)}",
        )

    # Retrieve the uploaded bucket file.
    bucket_file = bucket_file_lst.pop()

    return IntermediateUploadResponse(
        object_id=bucket_file.id,
        url=str(
            request.url_for(
                "intermediate.object.get",
                object_id=bucket_file.id,
            ).include_query_params(
                remote_node_id=node_id,
            )
        ),
    )


@router.get(
    "/{object_id}",
    summary="Get intermediate result as file from Hub",
    operation_id="getIntermediateResult",
    # The client ID is not actually used here but required for authentication. Having this as a path dependency makes
    # PyCharm stop complaining about unused parameters.
    dependencies=[Depends(get_client_id)],
    name="intermediate.object.get",
)
async def retrieve_intermediate_result_from_hub(
    object_id: uuid.UUID,
    remote_node_id: str,
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    storage_client: Annotated[flame_hub.StorageClient, Depends(get_storage_client)],
    private_key: Annotated[ec.EllipticCurvePrivateKey, Depends(get_ecdh_private_key)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    """Get an intermediate result as file from the FLAME Hub."""
    if storage_client.get_bucket_file(object_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object with ID {object_id} does not exist",
        )

    # Test decryption first to raise a proper error.
    encrypted_chunk = next(storage_client.stream_bucket_file(object_id, chunk_size=settings.chunk_size))
    remote_node_public_key = get_remote_node_public_key(core_client, remote_node_id)
    try:
        crypto.decrypt_default(private_key, remote_node_public_key, encrypted_chunk)
    except InvalidTag:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to decrypt file with ID {object_id} which was encrypted by node {remote_node_id}.",
        )

    async def _stream_file():
        try:
            for chunk in storage_client.stream_bucket_file(object_id, chunk_size=settings.chunk_size):
                yield crypto.decrypt_default(private_key, remote_node_public_key, chunk)
        except InvalidTag:
            logger.exception(f"Failed to decrypt file with ID {object_id} while streaming.")
            raise
        finally:
            try:
                storage_client.delete_bucket_file(bucket_file_id=object_id)
            except flame_hub.HubAPIError:
                logger.exception(f"Failed to delete bucket file {object_id}")

    return StreamingResponse(_stream_file())
