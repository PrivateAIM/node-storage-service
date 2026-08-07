from enum import Enum
from pathlib import Path
from typing import Literal, Annotated, Union

from pydantic import BaseModel, HttpUrl, ConfigDict, Field, AnyHttpUrl, SecretStr, SecretBytes
from pydantic_settings import BaseSettings, SettingsConfigDict


class FrozenBaseModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class S3Connection(FrozenBaseModel):
    endpoint: str
    access_key: str
    secret_key: SecretStr
    region: str = "us-east-1"
    use_ssl: bool = True


class S3BucketConfig(S3Connection):
    bucket: str


class OIDCConfig(FrozenBaseModel):
    certs_url: HttpUrl
    client_id_claim_name: str = "client_id"
    skip_jwt_validation: bool = False


class ClientAuthConfig(FrozenBaseModel):
    id: str
    secret: SecretStr


class HubConfig(FrozenBaseModel):
    core_base_url: HttpUrl = "https://core.privateaim.net"
    auth_base_url: HttpUrl = "https://auth.privateaim.net"
    storage_base_url: HttpUrl = "https://storage.privateaim.net"

    auth: ClientAuthConfig


class PostgresConfig(FrozenBaseModel):
    host: str
    password: SecretStr
    user: str
    db: str
    port: int = 5432
    max_connections: int = 20
    stale_timeout: int = 300
    keepalives_idle: int = 60
    keepalives_interval: int = 30
    keepalives_count: int = 3
    migrations_tablename: str = "storage_service_migration_history"


class CryptoProvider(str, Enum):
    raw = "raw"
    file = "file"


class RawCryptoConfig(FrozenBaseModel):
    provider: Literal[CryptoProvider.raw]
    ecdh_private_key: SecretBytes


class FileCryptoConfig(FrozenBaseModel):
    provider: Literal[CryptoProvider.file]
    ecdh_private_key_path: Path


class ProxyConfig(FrozenBaseModel):
    http_url: AnyHttpUrl | None = None
    https_url: AnyHttpUrl | None = None


class Settings(BaseSettings):
    hub: HubConfig
    s3: S3BucketConfig
    oidc: OIDCConfig
    postgres: PostgresConfig
    crypto: Annotated[Union[RawCryptoConfig, FileCryptoConfig], Field(discriminator="provider")]
    proxy: Annotated[ProxyConfig, Field(default_factory=ProxyConfig)]
    extra_ca_certs: Path | None = None
    hub_adapter_client_id: str = "hub-adapter"

    model_config = SettingsConfigDict(
        frozen=True,
        env_file=".env",
        env_nested_delimiter="__",
        extra="ignore",
    )
