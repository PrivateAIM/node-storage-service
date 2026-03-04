from enum import Enum
from functools import cached_property
import logging
import uuid

from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute, HTTPException, Request
from node_event_logging import AttributesModel, EventLog, EventModelMap, init_db
import peewee as pw
from psycopg2 import DatabaseError
from starlette import status
from starlette.datastructures import Address

from project.crud import Postgres
from project.dependencies import (
    get_settings,
    get_ssl_context,
    get_proxy_mounts,
    get_flame_hub_auth_flow,
    get_core_client,
)


SERVICE_NAME = "storage"
logger = logging.getLogger(__name__)


class EventTag(str, Enum):
    HUB = "Hub"
    HUB_ADAPTER = "Hub Adapter"
    PO = "Pod Orchestrator"
    STORAGE = "Storage"
    AUTH = "Authentication"

    INFO = "Info"
    WARNING = "Warning"
    ERROR = "Error"


class BaseRequestAttributes(AttributesModel):
    """Definition of additional attributes that are used for validation before persisting an event log."""

    method: str
    path: str
    url: str
    client: Address
    status_code: int
    tags: list[EventTag]


class AuthenticatedRequestAttributes(BaseRequestAttributes):
    """Requests for which authentication does not have failed, so client_id is always available. project_id is only
    available if client_id is an analysis_id and the analysis is still available on the Hub."""

    client_id: str
    project_id: uuid.UUID | None


AGNOSTIC_EVENTS = {
    "local.put": {
        "body_template": "Analysis {analysis_name} requested to upload a file to the local storage",
        "tags": [EventTag.STORAGE, EventTag.PO],
    },
    "local.delete": {
        "body_template": "Hub Adapter requested to delete all local files for project {project_id}",
        "tags": [EventTag.STORAGE, EventTag.HUB_ADAPTER],
    },
    "local.tags.get": {
        "body_template": "Analysis {analysis_name} requested a list of tags associated to its project {project_name}",
        "tags": [EventTag.STORAGE, EventTag.PO],
    },
    "local.tags.post": {
        "body_template": "Analysis {analysis_name} requested to tag object {object_id} with tag {tag_name}",
        "tags": [EventTag.STORAGE, EventTag.PO],
    },
    "local.tags.name.get": {
        "body_template": "Analysis {analysis_name} requested a list of all local files assigned to tag {tag_name}",
        "tags": [EventTag.STORAGE, EventTag.PO],
    },
    "local.object.get": {
        "body_template": "Analysis {analysis_name} requested to stream object {object_id} from local storage",
        "tags": [EventTag.STORAGE, EventTag.PO],
    },
    "local.upload.put": {
        "body_template": "Analysis {analysis_name} requested to directly upload the local object {object_id} as an "
        "intermediate result to the Hub",
        "tags": [EventTag.STORAGE, EventTag.PO, EventTag.HUB],
    },
    "intermediate.put": {
        "body_template": "Analysis {analysis_name} requested to upload a file as an intermediate result to the Hub",
        "tags": [EventTag.STORAGE, EventTag.PO, EventTag.HUB],
    },
    "intermediate.object.get": {
        "body_template": "Analysis {analysis_name} requested to download the intermediate result {object_id} from the "
        "Hub",
        "tags": [EventTag.STORAGE, EventTag.PO, EventTag.HUB],
    },
    "final.localdp.put": {
        "body_template": "Analysis {analysis_name} requested to upload a final result with local differential privacy "
        "to the Hub",
        "tags": [EventTag.STORAGE, EventTag.PO, EventTag.HUB],
    },
    "final.put": {
        "body_template": "Analysis {analysis_name} requested to upload a final result to the Hub",
        "tags": [EventTag.STORAGE, EventTag.PO, EventTag.HUB],
    },
    "auth": {
        "body_template": "Authentication against endpoint {endpoint_name} failed",
        "tags": [EventTag.STORAGE, EventTag.AUTH],
        "model": BaseRequestAttributes,
    },
    "unknown": {
        "body_template": "An unknown event has occurred",
        "tags": [EventTag.STORAGE],
        "model": BaseRequestAttributes,
    },
}


ANNOTATED_EVENTS = {}
for name, data in AGNOSTIC_EVENTS.items():
    ANNOTATED_EVENTS.update(
        {
            f"{name}.success": data,
            f"{name}.failure": data,
        }
    )


def annotate_event(event_name: str, status_code: int) -> tuple[str, EventTag]:
    """Append suffix to an event name indicating if a request was a success or failure and return an event tag based on
    the status code."""
    if status_code in (401, 403):
        status_tag = EventTag.WARNING
    elif status_code >= 400:
        status_tag = EventTag.ERROR
    else:
        status_tag = EventTag.INFO

    if event_name == "unknown":
        status_tag = EventTag.WARNING

    suffix = "failure" if status_code >= 400 else "success"

    return f"{event_name}.{suffix}", status_tag


class EventLogger(Postgres):
    """Event logging utility. This class is instantiated as event_logger in this module. event_logger is meant as a
    singleton. Do not instantiate this class again and use event_logger instead."""

    enabled: bool = get_settings().postgres.event_logging

    def __init__(self):
        super().__init__()
        logger.info(f"Event logging set to {'enabled' if self.enabled else 'disabled'}.")
        # Add the attributes model to the mapping to enable validation of attributes.
        EventModelMap.mapping = {
            event_name: event_data.get("model", AuthenticatedRequestAttributes)
            for event_name, event_data in ANNOTATED_EVENTS.items()
        }

    @cached_property
    def core_client(self):
        s = get_settings()
        ssl = get_ssl_context(s)
        proxy = get_proxy_mounts(s, ssl)
        auth = get_flame_hub_auth_flow(s, ssl, proxy)
        return get_core_client(s, auth, ssl, proxy)

    def setup(self):
        """Initializes the database and tests the connection. This is meant to be called during lifespan spin up."""
        init_db(self.db)
        self.test_connection()
        logger.info(f"Event logging enabled, connected to database at port {get_settings().postgres.port}.")

    def log_event(self, request: Request, status_code: int):
        """Log incoming FastAPI request."""
        # Set event name to unknown if route is not present.
        route = request.scope.get("route")
        if route is None:
            logger.warning("Route is None, set event name to unknown")
            event_name = "unknown"
        else:
            event_name = route.name

        template_kwargs = {"endpoint_name": event_name}

        # This only happens if authentication fails.
        try:
            client_id = request.state.client_id
        except AttributeError:
            client_id, event_name = None, "auth"

        event_name, status_tag = annotate_event(event_name, status_code)

        if event_name not in ANNOTATED_EVENTS:
            logger.warning(f"Unknown event name: {event_name}")
            event_name, _ = annotate_event("unknown", status_code)

        body_template = ANNOTATED_EVENTS[event_name]["body_template"]
        attributes = {
            "method": request.method,
            "path": request.scope.get("path"),
            "url": str(request.url),
            "client": request.client,
            "status_code": status_code,
            "tags": ANNOTATED_EVENTS[event_name]["tags"] + [status_tag],
        }

        template_kwargs |= dict(request.query_params) | request.path_params | {"client_id": client_id}

        if client_id is not None:
            # If client_id is an analysis_id, retrieve more information from the Hub about the analysis and project.
            analysis = self.core_client.get_analysis(analysis_id=client_id)
            if analysis is not None:
                template_kwargs.update(
                    {
                        "analysis_name": analysis.name,
                        "project_id": str(analysis.project_id),
                        "project_name": analysis.project.name,
                    }
                )
                attributes["project_id"] = str(analysis.project_id)
            else:
                template_kwargs.update(
                    {
                        "analysis_name": client_id,
                        "project_id": "(project not available)",
                        "project_name": "(project not available)",
                    }
                )

            attributes.update(
                {
                    "client_id": client_id,
                    "project_id": attributes.get("project_id", None),
                }
            )

        with self.db.atomic():
            try:
                EventLog.create(
                    event_name=event_name,
                    service_name=SERVICE_NAME,
                    body=body_template.format(**template_kwargs),
                    attributes=attributes,
                )
            except (pw.PeeweeException, DatabaseError) as e:
                logger.warning(str(e).strip())
                logger.warning("Failed to log event")


event_logger: EventLogger = EventLogger()


class EventLoggingRoute(APIRoute):
    """Route class to wrap route handler to log events when endpoints are called."""

    def get_route_handler(self):
        original_handler = super().get_route_handler()

        def safe_log(request: Request, status_code: int):
            try:
                event_logger.log_event(request, status_code)
            except Exception:
                logger.exception("Failed to log event.")

        # TODO: handle exceptions for streaming responses
        async def log_event(request: Request):
            try:
                response = await original_handler(request)
                safe_log(request, response.status_code)
                return response
            except HTTPException as e:
                safe_log(request, e.status_code)
                raise
            except RequestValidationError:
                safe_log(request, status.HTTP_422_UNPROCESSABLE_ENTITY)
                raise
            except Exception:
                safe_log(request, status.HTTP_500_INTERNAL_SERVER_ERROR)
                raise

        return log_event if event_logger.enabled else original_handler
