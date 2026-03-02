from enum import Enum
from functools import cached_property
import logging
import uuid

from fastapi.routing import APIRoute, Request
from node_event_logging import AttributesModel, EventLog, EventModelMap, init_db
import peewee as pw
from psycopg2 import DatabaseError
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

    INFO = "Info"
    WARNING = "Warning"
    ERROR = "Error"


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
    "unknown": {
        "body_template": "An unknown event has occurred",
        "tags": [EventTag.STORAGE],
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


class EventLogAttributes(AttributesModel):
    """Definition of additional attributes that is used for validation before persisting a log."""

    client_id: str
    project_id: uuid.UUID

    method: str
    path: str
    url: str
    client: Address
    status_code: int
    tags: list[EventTag]


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
        EventModelMap.mapping = {event_name: EventLogAttributes for event_name in ANNOTATED_EVENTS}

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

        event_name, status_tag = annotate_event(event_name, status_code)

        if event_name not in ANNOTATED_EVENTS:
            logger.warning(f"Unknown event name: {event_name}")
            event_name = "unknown"

        client_id = request.state.client_id

        template_kwargs = dict(request.query_params) | request.path_params | {"client_id": client_id}

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

        body = ANNOTATED_EVENTS[event_name]["body_template"].format(**template_kwargs)
        tags = ANNOTATED_EVENTS[event_name]["tags"] + [status_tag]
        attributes = {
            "client_id": client_id,
            "project_id": template_kwargs["project_id"],
            "method": request.method,
            "path": request.scope.get("path"),
            "url": str(request.url),
            "client": request.client,
            "status_code": status_code,
            "tags": tags,
        }

        with self.db.atomic():
            try:
                EventLog.create(
                    event_name=event_name,
                    service_name=SERVICE_NAME,
                    body=body,
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

        async def log_event(request: Request):
            response = await original_handler(request)
            event_logger.log_event(request, response.status_code)
            return response

        return log_event if event_logger.enabled else original_handler
