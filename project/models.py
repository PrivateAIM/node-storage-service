import uuid

from cryptography.hazmat.primitives.asymmetric import ec
from flame_hub import CoreClient, StorageClient
from minio import Minio
from playhouse.pool import PooledPostgresqlDatabase
from pydantic import BaseModel, ConfigDict, Field

from project.config import Settings
from project.jwks import JWKSCache


class AppState(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )

    settings: Settings
    core_client: CoreClient
    storage_client: StorageClient
    ecdh_private_key: ec.EllipticCurvePrivateKey
    node_id: uuid.UUID
    s3_client: Minio
    postgres: PooledPostgresqlDatabase
    start_time: float
    jwks_cache: JWKSCache = Field(default_factory=lambda data: JWKSCache(jwks_url=str(data["settings"].oidc.certs_url)))


class Author(BaseModel):
    name: str
    email: str | None = None


class Project(BaseModel):
    version: str
    description: str
    authors: list[Author]
    license: str


class PyProject(BaseModel):
    project: Project
