from io import BytesIO
import os
import random
import ssl
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer

from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, load_pem_private_key
import flame_hub
import httpx2 as httpx
import peewee as pw
import pytest
import truststore
from jwcrypto import jwk
from starlette.testclient import TestClient
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.community.postgres import PostgresContainer
from minio import Minio
from opendp.mod import enable_features

from project.config import Settings
from project.dependencies import (
    get_postgres_db,
    get_local_s3,
    get_ecdh_private_key,
    get_node_id,
    get_core_client,
    get_storage_client,
    get_settings,
)
from project.migrations.scripts.router import init_router
from project.server import get_server_instance, init_db
from tests.common import env
from tests.common.auth import get_oid_test_jwk, get_test_ecdh_keypair
from tests.common.helpers import (
    next_prefixed_name,
    eventually,
    next_random_bytes,
    next_uuid,
    next_ecdh_keypair_bytes,
    temporarily_change_dependency,
)


@pytest.fixture(scope="package")
def use_testcontainers():
    return os.environ.get("PYTEST__USE_TESTCONTAINERS", "0") == "1"


@pytest.fixture(scope="package")
def response_timeout():
    return int(os.getenv("PYTEST__RESPONSE_TIMEOUT", "5"))


@pytest.fixture(scope="package")
def postgres(use_testcontainers):
    host = os.environ.get("POSTGRES__HOST")
    port = os.environ.get("POSTGRES__PORT", 5432)
    dbname = os.environ.get("POSTGRES__DB")
    user = os.environ.get("POSTGRES__USER")
    password = os.environ.get("POSTGRES__PASSWORD")

    if use_testcontainers:
        postgres_container = PostgresContainer(
            "postgres:17.2",
            username=user,
            password=password,
            dbname=dbname,
            driver=None,
            ports=[5432],
        ).waiting_for(LogMessageWaitStrategy("database system is ready to accept connections"))
        postgres_container.start()

        host = postgres_container.get_container_host_ip()
        port = postgres_container.get_exposed_port(5432)

        # Update env vars to the host and port of the testcontainer.
        os.environ["POSTGRES__HOST"] = host
        os.environ["POSTGRES__PORT"] = str(port)

    postgres = pw.PostgresqlDatabase(dbname, user=user, password=password, host=host, port=port)

    # Execute database migrations.
    with init_router() as router:
        router.run()

    # Initialize database here since the lifespan method is not executed.
    init_db(postgres)

    yield postgres

    postgres.close()

    if use_testcontainers:
        postgres_container.stop()


@pytest.fixture(scope="package")
def override_postgres(use_testcontainers, postgres):
    if not use_testcontainers:
        yield None
    else:

        def _override_get_postgres_db():
            return postgres

        yield _override_get_postgres_db


@pytest.fixture(scope="package")
def s3(use_testcontainers):
    access_key = os.environ.get("S3__ACCESS_KEY", "admin")
    secret_key = os.environ.get("S3__SECRET_KEY", "s3cr3t_p4ssw0rd")
    bucket = os.environ.get("S3__BUCKET", "flame")
    endpoint = os.environ.get("S3__ENDPOINT", "localhost:8333")
    region = os.environ.get("S3__REGION")
    secure = bool(int(os.environ.get("S3__USE_SSL", 0)))

    if use_testcontainers:
        seaweedfs = DockerContainer(
            image="chrislusf/seaweedfs:4.39",
            env={
                "AWS_ACCESS_KEY_ID": access_key,
                "AWS_SECRET_ACCESS_KEY": secret_key,
                "S3_BUCKET": bucket,
            },
            ports=[8333],
        ).waiting_for(LogMessageWaitStrategy("All enabled components are running and ready to use"))
        seaweedfs.start()

        endpoint = f"{seaweedfs.get_container_host_ip()}:{seaweedfs.get_exposed_port(8333)}"
        secure = False

        # Update env var to the endpoint of the testcontainer.
        os.environ["S3__ENDPOINT"] = endpoint

    s3 = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure, region=region)

    assert s3.bucket_exists(bucket)

    yield s3

    if use_testcontainers:
        seaweedfs.stop()


@pytest.fixture(scope="package")
def ssl_context():
    return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)


@pytest.fixture(scope="package")
def client_auth_client(ssl_context):
    client = httpx.Client(base_url=env.hub_auth_base_url(), verify=ssl_context)
    try:
        yield flame_hub.auth.ClientAuth(
            env.hub_client_auth_id(),
            env.hub_client_auth_secret(),
            client=client,
        )
    finally:
        client.close()


@pytest.fixture(scope="package")
def password_auth_client(ssl_context):
    client = httpx.Client(base_url=env.hub_auth_base_url(), verify=ssl_context)
    try:
        yield flame_hub.auth.PasswordAuth(
            username=os.getenv("PYTEST__HUB_USER", "admin"),
            password=os.getenv("PYTEST__HUB_USER_PASSWORD", "start123"),
            client=client,
        )
    finally:
        client.close()


@pytest.fixture(scope="package")
def auth_client(client_auth_client, ssl_context, response_timeout):
    client = httpx.Client(
        auth=client_auth_client,
        base_url=env.hub_auth_base_url(),
        verify=ssl_context,
        timeout=response_timeout,
    )
    try:
        yield flame_hub.AuthClient(client=client)
    finally:
        client.close()


@pytest.fixture(scope="package")
def core_client(client_auth_client, ssl_context, response_timeout):
    client = httpx.Client(
        auth=client_auth_client,
        base_url=env.hub_core_base_url(),
        verify=ssl_context,
        timeout=response_timeout,
    )
    try:
        yield flame_hub.CoreClient(client=client)
    finally:
        client.close()


@pytest.fixture(scope="package")
def core_client_pwd_auth(password_auth_client, ssl_context, response_timeout):
    client = httpx.Client(
        auth=password_auth_client,
        base_url=env.hub_core_base_url(),
        verify=ssl_context,
        timeout=response_timeout,
    )
    try:
        yield flame_hub.CoreClient(client=client)
    finally:
        client.close()


@pytest.fixture(scope="package")
def storage_client(client_auth_client, ssl_context, response_timeout):
    client = httpx.Client(
        auth=client_auth_client,
        base_url=env.hub_storage_base_url(),
        verify=ssl_context,
        timeout=response_timeout,
    )
    try:
        yield flame_hub.StorageClient(client=client)
    finally:
        client.close()


@pytest.fixture(scope="package")
def override_settings():
    # At this point, settings also picks up the hosts and ports of possible testcontainers.
    def _get_settings():
        return Settings()

    yield _get_settings


@pytest.fixture(scope="package")
def override_core_client(core_client):
    def _get_core_client():
        return core_client

    yield _get_core_client


@pytest.fixture(scope="package")
def override_storage_client(storage_client):
    def _get_storage_client():
        return storage_client

    yield _get_storage_client


@pytest.fixture(scope="package")
def override_s3(use_testcontainers, s3):
    if not use_testcontainers:
        yield None
    else:

        def _override_get_local_s3():
            return s3

        yield _override_get_local_s3


@pytest.fixture(scope="package")
def override_ecdh_private_key():
    private_key, _ = get_test_ecdh_keypair()

    def _get_ecdh_private_key():
        return private_key

    yield _get_ecdh_private_key


@pytest.fixture(scope="package")
def test_app(
    override_settings,
    override_s3,
    override_postgres,
    override_ecdh_private_key,
    override_core_client,
    override_storage_client,
):
    app = get_server_instance()

    if callable(override_postgres):
        app.dependency_overrides[get_postgres_db] = override_postgres

    if callable(override_s3):
        app.dependency_overrides[get_local_s3] = override_s3

    app.dependency_overrides[get_settings] = override_settings
    app.dependency_overrides[get_ecdh_private_key] = override_ecdh_private_key
    app.dependency_overrides[get_core_client] = override_core_client
    app.dependency_overrides[get_storage_client] = override_storage_client

    return app


@pytest.fixture(scope="package")
def test_client(test_app):
    enable_features("floating-point")
    enable_features("contrib")
    # see https://fastapi.tiangolo.com/advanced/testing-events/
    return TestClient(test_app)


@pytest.fixture(scope="package", autouse=True)
def setup_jwks_endpoint():
    jwks = jwk.JWKSet()
    jwks["keys"].add(get_oid_test_jwk())
    jwks_str = jwks.export(private_keys=False)

    class JWKSHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(jwks_str.encode("utf-8"))

    httpd_url = urllib.parse.urlparse(env.oidc_certs_url())
    httpd = HTTPServer((httpd_url.hostname, httpd_url.port), JWKSHandler)

    t = threading.Thread(target=httpd.serve_forever)
    t.start()

    yield

    httpd.shutdown()


@pytest.fixture(scope="package")
def rng():
    return random.Random(727)


@pytest.fixture(scope="package")
def master_image(core_client):
    preferred_base_image_name = os.environ.get("PYTEST__PREFERRED_BASE_MASTER_IMAGE", "python/base")
    filter_ = {"virtualPath": preferred_base_image_name}

    if len(core_client.find_master_images(filter=filter_)) == 0:
        core_client.sync_master_images()

    def _wait_for_master_image():
        return len(core_client.find_master_images(filter=filter_)) == 1

    assert eventually(_wait_for_master_image)

    return core_client.find_master_images(filter=filter_)[0]


@pytest.fixture
def project_id_factory(core_client_pwd_auth, master_image):
    core_client = core_client_pwd_auth
    project_ids = []

    def _factory():
        project_name = next_prefixed_name()
        project = core_client.create_project(
            name=project_name,
            master_image_id=master_image,
            display_name=next_prefixed_name(),
        )

        def _project_exists():
            return core_client.get_project(project.id) is not None

        assert eventually(_project_exists)

        # Get freshly created project from the Hub.
        project = core_client.get_project(project.id)

        # Check the project name.
        assert project.name == project_name

        # Check that project appears in list.
        assert len(core_client.find_projects(filter={"id": project.id})) == 1

        project_ids.append(project.id)

        return project.id

    yield _factory

    for project_id in project_ids:
        core_client.delete_project(project_id)

        # Check that project is no longer found.
        assert core_client.get_project(project_id) is None


@pytest.fixture
def project_id(project_id_factory):
    return project_id_factory()


@pytest.fixture
def analysis_id_factory(core_client_pwd_auth, storage_client, project_id):
    core_client = core_client_pwd_auth
    analysis_ids, bucket_ids, analysis_bucket_ids = [], [], []

    def _factory(_project_id=project_id):
        analysis_name = next_prefixed_name()
        analysis = core_client.create_analysis(_project_id, analysis_name)

        def _analysis_exists():
            return core_client.get_analysis(analysis.id) is not None

        assert eventually(_analysis_exists)

        # Check name and project ID of the analysis.
        assert analysis.name == analysis_name
        assert analysis.project_id == _project_id

        # Check if analysis exists.
        assert core_client.get_analysis(analysis.id) is not None

        for bucket_type in flame_hub.types.AnalysisBucketType:
            bucket_name = next_prefixed_name()
            bucket = storage_client.create_bucket(name=bucket_name)

            # Check if bucket exists.
            assert storage_client.get_bucket(bucket.id) is not None

            analysis_bucket = core_client.create_analysis_bucket(
                bucket_type=bucket_type,
                bucket_id=bucket.id,
                analysis_id=analysis.id,
            )

            # Check if analysis bucket exists.
            assert core_client.get_analysis_bucket(analysis_bucket.id) is not None

            bucket_ids.append(bucket.id)
            analysis_bucket_ids.append(analysis_bucket.id)

        analysis_ids.append(analysis.id)

        return analysis.id

    yield _factory

    for analysis_bucket_id in analysis_bucket_ids:
        core_client.delete_analysis_bucket(analysis_bucket_id)
        assert core_client.get_analysis_bucket(analysis_bucket_id) is None

    for analysis_id in analysis_ids:
        core_client.delete_analysis(analysis_id)
        assert core_client.get_analysis(analysis_id) is None

    for bucket_id in bucket_ids:
        # Delete all bucket files before deleting the bucket itself.
        for bucket_file in storage_client.find_bucket_files(filter={"bucketId": bucket_id}):
            storage_client.delete_bucket_file(bucket_file.id)
        storage_client.delete_bucket(bucket_id)
        assert storage_client.get_bucket(bucket_id) is None


@pytest.fixture
def analysis_id(analysis_id_factory):
    return analysis_id_factory()


@pytest.fixture(scope="package")
def realm_id(auth_client):
    preferred_realm_name = os.environ.get("PYTEST__PREFERRED_REALM_NAME", "master")
    realm_list = auth_client.find_realms(filter={"name": preferred_realm_name})

    assert len(realm_list) == 1

    yield realm_list.pop()


@pytest.fixture(scope="package")
def this_node(test_client, core_client, realm_id):
    node = core_client.create_node(name=next_uuid(), realm_id=realm_id, node_type="default")
    _, public_key = get_test_ecdh_keypair()
    # Also update node reference.
    node = core_client.update_node(
        node, public_key=public_key.public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo).hex()
    )

    def override_get_node_id():
        return node.id

    # Change dependency here since live infra is mandatory at this point.
    temporarily_change_dependency(test_client, get_node_id, override_get_node_id)
    yield node
    core_client.delete_node(node.id)


@pytest.fixture()
def remote_node_and_private_key(core_client, realm_id):
    node = core_client.create_node(name=next_uuid(), realm_id=realm_id, node_type="default")
    private_key, public_key = next_ecdh_keypair_bytes()
    # Also update node reference.
    node = core_client.update_node(node, public_key=public_key.hex())
    yield node, load_pem_private_key(private_key, password=None)
    core_client.delete_node(node.id)


@pytest.fixture
def s3_object(s3, rng, project_id):
    blob = next_random_bytes(rng)
    object_name = next_uuid()
    bucket = os.environ.get("S3__BUCKET", "flame")
    obj = s3.put_object(
        bucket_name=bucket, object_name=f"local/{project_id}/{object_name}", data=BytesIO(blob), length=len(blob)
    )
    yield obj
    s3.remove_object(bucket_name=bucket, object_name=obj.object_name)
