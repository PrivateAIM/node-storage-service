import logging.config
from contextlib import asynccontextmanager
from pathlib import Path

import flame_hub
from fastapi import FastAPI, Request, HTTPException
import peewee as pw
from psycopg2 import DatabaseError
from pydantic import BaseModel
from starlette import status

from project.crud import postgres
from project.event_logging import event_logger
from project.routers import final, intermediate, local
from opendp.mod import enable_features

_app: FastAPI | None = None


class Author(BaseModel):
    name: str
    email: str


class Project(BaseModel):
    version: str
    description: str
    authors: list[Author]
    license: str


class PyProject(BaseModel):
    project: Project


def get_project_root():
    return Path(__file__).parent.parent


def load_pyproject():
    import tomli

    with open(get_project_root() / "pyproject.toml", mode="rb") as f:
        pyproject_data = tomli.load(f)
        return PyProject(**pyproject_data)


def load_readme():
    with open(get_project_root() / "README.md", mode="r") as f:
        return f.read()


@asynccontextmanager
async def lifespan(_: FastAPI):
    import os
    import json

    os.makedirs("logs", exist_ok=True)
    log_config_file_path = get_project_root() / "config" / "logging.json"

    with open(log_config_file_path) as f:
        log_config = json.load(f)

    logging.config.dictConfig(log_config)

    # Enable floating point features in OpenDP
    enable_features("floating-point")
    # Enable features in OpenDP
    enable_features("contrib")

    # Set up Postgres database to store results.
    postgres.setup()

    # Set up the logger for event logging.
    if event_logger.enabled:
        event_logger.setup()

    yield

    # Close all connections to the database. Note that it is not necessary to call event_logger.teardown.
    postgres.teardown()


def get_server_instance():
    global _app

    if _app is not None:
        return _app

    project_data = load_pyproject()
    project_readme = load_readme()

    logger = logging.getLogger(__name__)

    _app = FastAPI(
        title="FLAME Node Storage Service",
        summary=project_data.project.description,
        version=project_data.project.version,
        lifespan=lifespan,
        description=project_readme,
        license_info={
            "name": project_data.project.license,
            "identifier": project_data.project.license,
        },
        contact={
            "name": ", ".join([author.name for author in project_data.project.authors]),
            "url": "https://docs.privateaim.net/about/team.html",
        },
        servers=[
            {"url": "http://localhost:8000", "description": "Local"},
        ],
        openapi_tags=[
            {
                "name": "final",
                "description": "Upload final results to FLAME Hub",
            },
            {
                "name": "intermediate",
                "description": "Upload intermediate results to FLAME Hub",
            },
            {
                "name": "local",
                "description": "Upload intermediate results to local storage",
            },
            {
                "name": "healthz",
                "description": "Check whether the service is ready to process requests",
            },
        ],
    )

    @_app.get("/healthz", summary="Check service readiness", operation_id="getHealth", tags=["healthz"])
    async def do_healthcheck():
        """Check whether the service is ready to process requests. Responds with a 200 on success."""
        return {"status": "ok"}

    # re-raise as an http exception
    @_app.exception_handler(flame_hub.HubAPIError)
    async def handle_hub_api_error(_: Request, exc: flame_hub.HubAPIError):
        remote_status_code = "unknown"
        if exc.error_response is not None:
            remote_status_code = exc.error_response.status_code

        error_msg = f"Unexpected response from Hub (status code {remote_status_code}): '{exc}'."
        logger.exception(error_msg)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=error_msg,
        )

    async def handle_database_error(_: Request, exc: pw.PeeweeException | DatabaseError):
        error_msg = f"Unexpected database error: '{exc}'."
        logger.exception(error_msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=error_msg,
        )

    _app.add_exception_handler(pw.PeeweeException, handle_database_error)
    _app.add_exception_handler(DatabaseError, handle_database_error)

    _app.include_router(
        final.router,
        prefix="/final",
        tags=["final"],
    )

    _app.include_router(
        intermediate.router,
        prefix="/intermediate",
        tags=["intermediate"],
    )

    _app.include_router(
        local.router,
        prefix="/local",
        tags=["local"],
    )

    return _app
