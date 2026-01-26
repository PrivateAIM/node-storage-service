import io
import logging
import re
from urllib3.response import HTTPResponse
import uuid
from typing import Annotated

import flame_hub
import peewee as pw
from fastapi import Depends, UploadFile, APIRouter, HTTPException, File, Form
from cryptography.hazmat.primitives.asymmetric import ec
from minio import Minio, S3Error
from pydantic import BaseModel, HttpUrl, Field
from starlette import status
from starlette.requests import Request
from starlette.responses import StreamingResponse

from project import crud
from project.config import Settings
from project.dependencies import (
    get_client_id,
    get_settings,
    get_local_minio,
    get_postgres_db,
    get_core_client,
    get_storage_client,
    get_ecdh_private_key,
)
from project.routers.intermediate import IntermediateUploadResponse, submit_intermediate_result_to_hub

router = APIRouter()
logger = logging.getLogger(__name__)

_TAG_PATTERN = re.compile(r"[a-z0-9]{1,2}|[a-z0-9][a-z0-9-]{,30}[a-z0-9]")


def is_valid_tag(tag: str) -> bool:
    return _TAG_PATTERN.fullmatch(tag) is not None


def tag_object(
    tag: str,
    db: pw.PostgresqlDatabase,
    project_id: uuid.UUID | str,
    client_id: str,
    object_id: uuid.UUID | str,
    filename: str = None,
):
    if not is_valid_tag(tag):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid tag `{tag}`")

    with crud.bind_to(db):
        # TODO more elegant solution for filename being None?
        try:
            result, _ = crud.Result.get_or_create(
                client_id=client_id,
                object_id=object_id,
                filename=filename or "data.bin",
            )
        except pw.IntegrityError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The object ID {object_id} is already persisted for analysis {client_id}, but with a different "
                f"filename than {filename or 'data.bin'}.",
            )
        tag, _ = crud.Tag.get_or_create(tag_name=tag, project_id=project_id)
        crud.TaggedResult.get_or_create(tag=tag, result=result)


class LocalUploadResponse(BaseModel):
    url: HttpUrl
    object_id: uuid.UUID


class LocalTag(BaseModel):
    name: str
    url: HttpUrl


class LocalTagListResponse(BaseModel):
    tags: Annotated[list[LocalTag], Field(default_factory=list)]


class LocalTaggedResult(BaseModel):
    filename: str
    url: HttpUrl


class LocalTaggedResultListResponse(BaseModel):
    results: Annotated[list[LocalTaggedResult], Field(default_factory=list)]


def _get_project_id_for_analysis_or_raise(core_client: flame_hub.CoreClient, analysis_id: str):
    analysis = core_client.get_analysis(analysis_id)

    if analysis is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Analysis with ID {analysis_id} not found",
        )

    return str(analysis.project_id)


def _get_object_from_s3(
    minio: Minio, settings: Settings, project_id: str, object_id: uuid.UUID, client_id: str
) -> HTTPResponse:
    try:
        return minio.get_object(
            settings.minio.bucket,
            f"local/{project_id}/{object_id}",
        )
    except S3Error as e:
        logger.exception(
            f"Could not get object `{object_id}` for client `{client_id}` which is associated to project "
            f"`{project_id}`."
        )

        if e.code == "NoSuchKey":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Object with ID {object_id} does not exist",
            )

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Unexpected error from object store",
        )


@router.put(
    "/",
    response_model=LocalUploadResponse,
    summary="Upload file as intermediate result to local storage",
    operation_id="putLocalResult",
)
async def submit_intermediate_result_to_local(
    client_id: Annotated[str, Depends(get_client_id)],
    file: Annotated[UploadFile, File()],
    settings: Annotated[Settings, Depends(get_settings)],
    minio: Annotated[Minio, Depends(get_local_minio)],
    db: Annotated[pw.PostgresqlDatabase, Depends(get_postgres_db)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    request: Request,
    tag: Annotated[str | None, Form()] = None,
):
    """Upload a file as a local result.
    Returns a 200 on success.
    This endpoint uploads the file and returns a link with which it can be retrieved.
    An optional tag can be supplied to group the file with other files."""

    # retrieve project id from analysis
    project_id = _get_project_id_for_analysis_or_raise(core_client, client_id)

    object_id = uuid.uuid4()
    object_name = f"local/{project_id}/{object_id}"

    if tag is not None:
        tag_object(
            tag=tag, db=db, project_id=project_id, client_id=client_id, object_id=object_id, filename=file.filename
        )

    minio.put_object(
        settings.minio.bucket,
        object_name,
        data=file.file,
        length=file.size,
        content_type=file.content_type or "application/octet-stream",
    )

    return LocalUploadResponse(
        object_id=object_id,
        url=str(
            request.url_for(
                "retrieve_intermediate_result_from_local",
                object_id=object_id,
            )
        ),
    )


@router.delete(
    "/",
    summary="Delete all local results and database entries related to the specified project.",
    operation_id="deleteLocalResults",
)
async def delete_local_results(
    project_id: str,
    client_id: Annotated[str, Depends(get_client_id)],
    minio: Annotated[Minio, Depends(get_local_minio)],
    db: Annotated[pw.PostgresqlDatabase, Depends(get_postgres_db)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    """Delete all objects in MinIO and all Postgres database entries related to the specified project. Returns a 200 on
    success, a 400 if the project is still available on the Hub and a 403 if it is not the Hub Adapter client that sends
    the request. In both error cases nothing is deleted at all."""
    if client_id != settings.hub_adapter_client_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Only the Hub Adapter client is allowed to delete local results, got client ID '{client_id}'.",
        )

    if core_client.get_project(project_id) is not None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Project '{project_id}' will not be deleted because it is still available on the Hub.",
        )

    object_ids = []
    for object_ in minio.list_objects(settings.minio.bucket, prefix=f"local/{project_id}/"):
        minio.remove_object(settings.minio.bucket, object_.object_name)
        object_ids.append(object_.object_name.split("/")[-1])

    with crud.bind_to(db):
        crud.Result.delete().where(crud.Result.object_id.in_(object_ids)).execute()
        crud.Tag.delete().where(crud.Tag.project_id == project_id).execute()


@router.get(
    "/tags",
    summary="Get tags for a specific project",
    operation_id="getProjectTags",
    response_model=LocalTagListResponse,
)
async def get_project_tags(
    client_id: Annotated[str, Depends(get_client_id)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    db: Annotated[pw.PostgresqlDatabase, Depends(get_postgres_db)],
    request: Request,
):
    """Get a list of tags assigned to the project for an analysis.
    Returns a 200 on success."""
    project_id = _get_project_id_for_analysis_or_raise(core_client, client_id)

    with crud.bind_to(db):
        db_tags = crud.Tag.select().where(crud.Tag.project_id == project_id)

    return LocalTagListResponse(
        tags=[
            LocalTag(
                name=tag.tag_name,
                url=str(
                    request.url_for(
                        "get_results_by_project_tag",
                        tag_name=tag.tag_name,
                    )
                ),
            )
            for tag in db_tags
        ]
    )


@router.post(
    "/tags",
    summary="Tag an existing object",
    operation_id="tagObject",
    response_model=LocalTaggedResult,
)
async def create_object_tag(
    tag_name: str,
    object_id: uuid.UUID,
    client_id: Annotated[str, Depends(get_client_id)],
    settings: Annotated[Settings, Depends(get_settings)],
    db: Annotated[pw.PostgresqlDatabase, Depends(get_postgres_db)],
    minio: Annotated[Minio, Depends(get_local_minio)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    request: Request,
    filename: str | None = None,
):
    """Tag a specific object and return that file.
    Returns a 200 on success."""
    project_id = _get_project_id_for_analysis_or_raise(core_client, client_id)

    # Check if an object with that ID exists.
    _get_object_from_s3(minio, settings, project_id, object_id, client_id)

    tag_object(tag_name, db, project_id, client_id, object_id, filename)

    with crud.bind_to(db):
        result = (
            crud.Result.select()
            .where((crud.Result.object_id == object_id) & (crud.Result.client_id == client_id))
            .get()
        )

    return LocalTaggedResult(
        filename=result.filename,
        url=str(request.url_for("retrieve_intermediate_result_from_local", object_id=object_id)),
    )


@router.get(
    "/tags/{tag_name}",
    summary="Get results linked to a specific tag",
    operation_id="getTaggedResults",
    response_model=LocalTaggedResultListResponse,
)
async def get_results_by_project_tag(
    tag_name: str,
    client_id: Annotated[str, Depends(get_client_id)],
    db: Annotated[pw.PostgresqlDatabase, Depends(get_postgres_db)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    request: Request,
):
    """Get a list of files assigned to a tag.
    Returns a 200 on success."""
    project_id = _get_project_id_for_analysis_or_raise(core_client, client_id)

    with crud.bind_to(db):
        db_tagged_results = (
            crud.Result.select()
            .join(crud.TaggedResult)
            .join(crud.Tag)
            .where((crud.Tag.project_id == project_id) & (crud.Tag.tag_name == tag_name))
        )

    return LocalTaggedResultListResponse(
        results=[
            LocalTaggedResult(
                filename=result.filename,
                url=str(
                    request.url_for(
                        "retrieve_intermediate_result_from_local",
                        object_id=result.object_id,
                    )
                ),
            )
            for result in db_tagged_results
        ],
    )


@router.get(
    "/{object_id}",
    summary="Get intermediate result as file from local storage",
    operation_id="getLocalResult",
)
async def retrieve_intermediate_result_from_local(
    client_id: Annotated[str, Depends(get_client_id)],
    object_id: uuid.UUID,
    settings: Annotated[Settings, Depends(get_settings)],
    minio: Annotated[Minio, Depends(get_local_minio)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
):
    """Get a local result as file."""

    # retrieve project id from analysis
    project_id = _get_project_id_for_analysis_or_raise(core_client, client_id)

    response = _get_object_from_s3(minio, settings, project_id, object_id, client_id)

    return StreamingResponse(
        response,
        media_type=response.headers.get("Content-Type", "application/octet-stream"),
    )


@router.put(
    "/upload",
    summary="Upload a local file directly to the Hub",
    operation_id="uploadLocalFile",
    response_model=IntermediateUploadResponse,
)
async def upload_local_file(
    object_id: uuid.UUID,
    request: Request,
    client_id: Annotated[str, Depends(get_client_id)],
    minio: Annotated[Minio, Depends(get_local_minio)],
    settings: Annotated[Settings, Depends(get_settings)],
    core_client: Annotated[flame_hub.CoreClient, Depends(get_core_client)],
    storage_client: Annotated[flame_hub.StorageClient, Depends(get_storage_client)],
    db: Annotated[pw.PostgresqlDatabase, Depends(get_postgres_db)],
    private_key: Annotated[ec.EllipticCurvePrivateKey, Depends(get_ecdh_private_key)],
    remote_node_id: Annotated[str | None, Form()] = None,
):
    """Upload a local file directly to the FLAME Hub so that the requesting service does not have to load the file in
    its working memory to use the intermediate upload endpoint. Returns a 200 on success. This endpoint returns a link
    with which it can be retrieved."""

    # Retrieve project id from analysis.
    project_id = _get_project_id_for_analysis_or_raise(core_client, client_id)

    response = _get_object_from_s3(minio, settings, project_id, object_id, client_id)

    # Check for filename in database. If there is no filename, use object_id per default.
    filename = str(object_id)
    with crud.bind_to(db):
        result = crud.Result.select().where((crud.Result.object_id == object_id) & (crud.Result.client_id == client_id))
        if result.count() == 1:
            filename = result.get().filename

    file = UploadFile(
        file=io.BytesIO(response.read()),
        filename=filename,
        headers=response.headers,
    )

    return await submit_intermediate_result_to_hub(
        file=file,
        request=request,
        client_id=client_id,
        core_client=core_client,
        storage_client=storage_client,
        private_key=private_key,
        remote_node_id=remote_node_id,
    )
