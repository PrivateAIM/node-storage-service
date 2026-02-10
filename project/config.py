from enum import Enum
from pathlib import Path
from typing import Literal, Annotated, Union

from pydantic import BaseModel, HttpUrl, ConfigDict, Field, AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class FrozenBaseModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class MinioConnection(FrozenBaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    use_ssl: bool = True


class MinioBucketConfig(MinioConnection):
    bucket: str


class OIDCConfig(FrozenBaseModel):
    certs_url: HttpUrl
    client_id_claim_name: str = "client_id"
    skip_jwt_validation: bool = False


class AuthFlow(str, Enum):
    password = "password"
    client = "client"


class PasswordAuthConfig(FrozenBaseModel):
    flow: Literal[AuthFlow.password]
    username: str
    password: str


class ClientAuthConfig(FrozenBaseModel):
    flow: Literal[AuthFlow.client]
    id: str
    secret: str


class HubConfig(FrozenBaseModel):
    core_base_url: HttpUrl = "https://core.privateaim.net"
    auth_base_url: HttpUrl = "https://auth.privateaim.net"
    storage_base_url: HttpUrl = "https://storage.privateaim.net"

    auth: Annotated[Union[ClientAuthConfig, PasswordAuthConfig], Field(discriminator="flow")]


class PostgresConfig(FrozenBaseModel):
    host: str
    password: str
    user: str
    db: str
    port: int = 5432


class CryptoProvider(str, Enum):
    raw = "raw"
    file = "file"


class RawCryptoConfig(FrozenBaseModel):
    provider: Literal[CryptoProvider.raw]
    ecdh_private_key: bytes


class FileCryptoConfig(FrozenBaseModel):
    provider: Literal[CryptoProvider.file]
    ecdh_private_key_path: Path


class ProxyConfig(FrozenBaseModel):
    http_url: AnyHttpUrl | None = None
    https_url: AnyHttpUrl | None = None


class Settings(BaseSettings):
    hub: HubConfig
    minio: MinioBucketConfig
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
    )
