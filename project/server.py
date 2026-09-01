import logging.config
from contextlib import asynccontextmanager, ExitStack
import ssl
import uuid

from cryptography.hazmat.primitives.asymmetric import ec
from flame_hub import CoreClient, StorageClient, HubAPIError
from flame_hub.auth import ClientAuth
from fastapi import FastAPI, Request, HTTPException
import httpx2 as httpx
from minio import Minio
import peewee as pw
from psycopg2 import DatabaseError
from playhouse.pool import PooledPostgresqlDatabase
from starlette import status
import truststore
from opendp.mod import enable_features

from project import crypto
from project.config import CryptoProvider, Settings
from project.crud import proxy as db_proxy
from project.models import AppState
from project.routers import final, intermediate, local
from project.version import __version__
from project.utils import load_pyproject, load_readme


_app: FastAPI | None = None


def init_db(db: PooledPostgresqlDatabase | pw.PostgresqlDatabase) -> None:
    db_proxy.initialize(db)
    with db_proxy:  # This tests the database connection.
        pass


def _build_ssl_context(s: Settings) -> ssl.SSLContext:
    # See https://www.python-httpx.org/advanced/ssl/#configuring-client-instances.
    ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if s.extra_ca_certs is not None:
        ctx.load_verify_locations(cafile=s.extra_ca_certs)
    return ctx


def _build_proxy_mounts(s: Settings, ssl_context: ssl.SSLContext) -> dict[str, httpx.HTTPTransport] | None:
    proxy_mounts = {}
    http_proxy_set = s.proxy.http_url is not None
    https_proxy_set = s.proxy.https_url is not None

    if http_proxy_set and https_proxy_set:
        proxy_mounts["http://"] = httpx.HTTPTransport(proxy=str(s.proxy.http_url))
        proxy_mounts["https://"] = httpx.HTTPTransport(proxy=str(s.proxy.https_url), verify=ssl_context)
    elif http_proxy_set or https_proxy_set:
        proxy_url = str(s.proxy.http_url) if http_proxy_set else str(s.proxy.https_url)
        proxy_mounts["http://"] = httpx.HTTPTransport(proxy=proxy_url)
        proxy_mounts["https://"] = httpx.HTTPTransport(proxy=proxy_url, verify=ssl_context)

    return proxy_mounts or None


def _get_ecdh_private_key(s: Settings) -> ec.EllipticCurvePrivateKey:
    # Settings enforce that either path or bytes are set.
    if s.crypto.provider == CryptoProvider.raw:
        return crypto.load_ecdh_private_key(s.crypto.ecdh_private_key.get_secret_value())

    if s.crypto.provider == CryptoProvider.file:
        return crypto.load_ecdh_private_key_from_path(s.crypto.ecdh_private_key_path)

    raise NotImplementedError(f"Unknown crypto provider {s.crypto.provider}.")


def _get_node_id(s: Settings, core_client: CoreClient) -> uuid.UUID:
    client_id = s.hub.auth.id
    nodes = core_client.find_nodes(filter={"clientId": client_id})

    if len(nodes) != 1:
        raise RuntimeError(f"Found {len(nodes)} nodes with the client id {client_id}.")

    return nodes[0].id


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger = logging.getLogger(__name__)
    stack = ExitStack()

    try:
        s = Settings()
        logger.info("Successfully read and validated the configuration.")

        ssl_context = _build_ssl_context(s)
        # Generate new proxy mounts every time so that clients get fresh httpx2.HTTPTransport instances.
        auth_flow_client = httpx.Client(
            base_url=str(s.hub.auth_base_url),
            verify=ssl_context,
            mounts=_build_proxy_mounts(s, ssl_context),
        )
        stack.callback(auth_flow_client.close)

        auth = ClientAuth(s.hub.auth.id, s.hub.auth.secret.get_secret_value(), client=auth_flow_client)

        core_client = CoreClient(
            client=httpx.Client(
                base_url=str(s.hub.core_base_url),
                auth=auth,
                verify=ssl_context,
                mounts=_build_proxy_mounts(s, ssl_context),
                timeout=20,  # TODO: better solution for hardcoding the timeout here
            )
        )
        stack.callback(core_client.close)

        storage_client = StorageClient(
            client=httpx.Client(
                base_url=str(s.hub.storage_base_url),
                auth=auth,
                verify=ssl_context,
                mounts=_build_proxy_mounts(s, ssl_context),
                timeout=httpx.Timeout(
                    10, read=None, write=None
                ),  # TODO: better solution for hardcoding the timeout here
            )
        )
        stack.callback(storage_client.close)

        logger.info("Successfully instantiated the core and storage client to communicate with the Hub.")

        minio = Minio(
            s.s3.endpoint,
            access_key=s.s3.access_key,
            secret_key=s.s3.secret_key.get_secret_value(),
            region=s.s3.region,
            secure=s.s3.use_ssl,
        )
        if not minio.bucket_exists(bucket_name=s.s3.bucket):
            raise RuntimeError(f"Bucket '{s.s3.bucket}' does not exist.")
        logger.info(f"Connected to S3 object storage under {s.s3.endpoint}.")

        postgres = PooledPostgresqlDatabase(
            s.postgres.db,
            user=s.postgres.user,
            password=s.postgres.password.get_secret_value(),
            host=s.postgres.host,
            port=s.postgres.port,
            max_connections=s.postgres.max_connections,
            stale_timeout=s.postgres.stale_timeout,
            keepalives=1,
            keepalives_idle=s.postgres.keepalives_idle,
            keepalives_interval=s.postgres.keepalives_interval,
            keepalives_count=s.postgres.keepalives_count,
        )
        stack.callback(postgres.close_all)

        init_db(postgres)
        logger.info(f"Connected to database at port {s.postgres.port} to store tags and results.")

        app.state.app_state = AppState(
            settings=s,
            core_client=core_client,
            storage_client=storage_client,
            ecdh_private_key=_get_ecdh_private_key(s),
            node_id=_get_node_id(s, core_client),
            s3_client=minio,
            postgres=postgres,
        )
        logger.info("Building app state done.")

        # Enable OpenDP features.
        enable_features("floating-point")
        enable_features("contrib")
        logger.info("Enabled OpenDP's 'floating-point' and 'contrib' features.")

        yield
    finally:
        # Close all clients that were already registered.
        stack.close()
        logger.info("Successfully closed all database and client connections.")


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
            {
                "name": "info",
                "description": "General info about this service",
            },
        ],
    )

    @_app.get("/", operation_id="getInfo", tags=["info"], description="Get general info about this service")
    async def info():
        return {"version": __version__}

    @_app.get("/healthz", operation_id="getHealth", tags=["healthz"], description="Check service readiness")
    async def do_healthcheck():
        """Check whether the service is ready to process requests. Responds with a 200 on success."""
        return {"status": "ok"}

    # re-raise as an http exception
    @_app.exception_handler(HubAPIError)
    async def handle_hub_api_error(_: Request, exc: HubAPIError):
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
        logger.exception(f"Unexpected database error: '{exc}'.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected database error.",
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
