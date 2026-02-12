import logging.config
from contextlib import asynccontextmanager
from pathlib import Path

import flame_hub
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
from starlette import status

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

    yield


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
        logger.exception("unexpected response from remote", exc_info=exc)

        remote_status_code = "unknown"

        if exc.error_response is not None:
            remote_status_code = exc.error_response.status_code

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Hub returned an unexpected response ({remote_status_code})",
        )

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
