import logging
from typing import Annotated

import flame_hub
from fastapi import APIRouter, Depends, UploadFile, HTTPException, Form
from opendp.domains import atom_domain
from opendp.measurements import make_laplace
from opendp.metrics import absolute_distance
from starlette import status

from project.dependencies import (
    get_client_id,
    get_core_client,
    get_storage_client,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.put(
    "/localdp",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Upload final result with Local DP to Hub",
    operation_id="putFinalResultWithLocalDP",
)
async def submit_final_single_value_with_local_dp_result_to_hub(
    client_id: Annotated[str, Depends(get_client_id)],
    file: UploadFile,
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    storage_client: Annotated[flame_hub.StorageClient, Depends(get_storage_client)],
    epsilon: Annotated[float, Form(...)],
    sensitivity: Annotated[float, Form(...)],
):
    """Upload a file as a final result with Local DP to the FLAME Hub.
    Ensures only the noisy value is stored. Returns 204 on success.
    """

    # Read and parse file as a single float value
    try:
        raw_value = float(await file.read())
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Uploaded file must contain a single numerical value.",
        )

    # Apply Laplace mechanism for Local DP
    scale = sensitivity / epsilon  # Laplace scale parameter
    laplace_mech = make_laplace(input_domain=atom_domain(T=float), input_metric=absolute_distance(T=float), scale=scale)
    noisy_value = laplace_mech(raw_value)

    noisy_file_content = str(noisy_value).encode("utf-8")

    analysis_bucket_lst = core_client.find_analysis_buckets(filter={"analysis_id": client_id, "type": "RESULT"})

    if len(analysis_bucket_lst) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Result bucket for analysis with ID {client_id} was not found",
        )

    analysis_bucket = analysis_bucket_lst.pop()

    bucket_file_lst = storage_client.upload_to_bucket(
        analysis_bucket.bucket_id,
        {
            "file_name": file.filename,
            "content": noisy_file_content.decode("utf-8"),
            "content_type": file.content_type or "application/octet-stream",
        },
    )

    if len(bucket_file_lst) != 1:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Expected single uploaded file to be returned by storage service, got {len(bucket_file_lst)}",
        )

    # fetch file s.t. it can be linked to result bucket
    bucket_file = bucket_file_lst.pop()

    # link file to analysis
    core_client.create_analysis_bucket_file(
        path=bucket_file.name,
        bucket_file_id=bucket_file.id,
        analysis_bucket_id=analysis_bucket.id,
        bucket_id=analysis_bucket.bucket_id,
    )


@router.put(
    "/",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Upload file as final result to Hub",
    operation_id="putFinalResult",
)
async def submit_final_result_to_hub(
    client_id: Annotated[str, Depends(get_client_id)],
    file: UploadFile,
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    storage_client: Annotated[flame_hub.StorageClient, Depends(get_storage_client)],
):
    """Upload a file as a final result to the FLAME Hub.
    Returns a 204 on success."""
    # fetch analysis bucket
    analysis_bucket_lst = core_client.find_analysis_buckets(filter={"analysis_id": client_id, "type": "RESULT"})

    if len(analysis_bucket_lst) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Result bucket for analysis with ID {client_id} was not found",
        )

    analysis_bucket = analysis_bucket_lst.pop()

    # upload to remote
    bucket_file_lst = storage_client.upload_to_bucket(
        analysis_bucket.bucket_id,
        {
            "file_name": file.filename,
            "content": file.file,
            "content_type": file.content_type or "application/octet-stream",
        },
    )

    if len(bucket_file_lst) != 1:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Expected single uploaded file to be returned by storage service, got {len(bucket_file_lst)}",
        )

    # fetch file s.t. it can be linked to result bucket
    bucket_file = bucket_file_lst.pop()

    # link file to analysis
    core_client.create_analysis_bucket_file(
        path=bucket_file.name,
        bucket_file_id=bucket_file.id,
        analysis_bucket_id=analysis_bucket.id,
        bucket_id=analysis_bucket.bucket_id,
    )
