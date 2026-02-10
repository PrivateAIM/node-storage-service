import json
import logging
import ssl
from functools import lru_cache
from typing import Annotated

import flame_hub.auth
import httpx
import peewee as pw
import truststore
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from httpx import HTTPError
from jwcrypto import jwk, jwt, common
from minio import Minio
from starlette import status

from project import crypto
from project.config import (
    Settings,
    MinioBucketConfig,
    AuthFlow,
    CryptoProvider,
    FileCryptoConfig,
    RawCryptoConfig,
)

security = HTTPBearer()
logger = logging.getLogger(__name__)


@lru_cache
def get_settings():
    return Settings()


def get_auth_jwks(settings: Annotated[Settings, Depends(get_settings)]):
    if settings.oidc.skip_jwt_validation:
        logger.warning("Since JWT validation is skipped, an empty JWKS is returned")
        return jwk.JWKSet()

    jwks_url = str(settings.oidc.certs_url)

    try:
        r = httpx.get(jwks_url)
        r.raise_for_status()
    except HTTPError:
        logger.exception("Failed to read OIDC config")

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Auth provider is unavailable",
        )

    jwks_payload = r.text

    return jwk.JWKSet.from_json(jwks_payload)


def __create_minio_from_config(minio: MinioBucketConfig):
    return Minio(
        minio.endpoint,
        access_key=minio.access_key,
        secret_key=minio.secret_key,
        region=minio.region,
        secure=minio.use_ssl,
    )


def get_local_minio(
    settings: Annotated[Settings, Depends(get_settings)],
):
    return __create_minio_from_config(settings.minio)


def get_client_id(
    settings: Annotated[Settings, Depends(get_settings)],
    jwks: Annotated[jwk.JWKSet, Depends(get_auth_jwks)],
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
):
    # TODO here be dragons!
    if settings.oidc.skip_jwt_validation:
        logger.warning("JWT validation is skipped, so JWT could be signed by an untrusted party or be expired")

        token = jwt.JWT(
            jwt=credentials.credentials,
            check_claims={
                settings.oidc.client_id_claim_name: None,
            },
        )

        # this hurts to write but there's no other way. token.token is an instance of JWS, and accessing
        # the payload property expects that it is validated. but it isn't since we're skipping validation.
        # so we have to access the undocumented property objects and read the payload from there.
        return json.loads(token.token.objects["payload"])[settings.oidc.client_id_claim_name]

    try:
        token = jwt.JWT(
            jwt=credentials.credentials,
            key=jwks,
            expected_type="JWS",
            algs=["RS256"],
            check_claims={
                "iat": None,
                "exp": None,
                settings.oidc.client_id_claim_name: None,
            },
        )

        jwt_data = json.loads(token.claims)
        return jwt_data[settings.oidc.client_id_claim_name]
    except (common.JWException, ValueError):
        logger.exception("Failed to deserialize JWT")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="JWT is malformed")


@lru_cache
def get_ssl_context(
    settings: Annotated[Settings, Depends(get_settings)],
):
    # see https://www.python-httpx.org/advanced/ssl/#configuring-client-instances
    ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if settings.extra_ca_certs is not None:
        ctx.load_verify_locations(cafile=settings.extra_ca_certs)
    return ctx


ProxyMount = dict[str, httpx.HTTPTransport] | None


def get_proxy_mounts(
    settings: Annotated[Settings, Depends(get_settings)],
    ssl_context: Annotated[ssl.SSLContext, Depends(get_ssl_context)],
):
    proxy = settings.proxy
    proxy_mounts = {}

    http_proxy_set = proxy.http_url is not None
    https_proxy_set = proxy.https_url is not None

    if http_proxy_set and https_proxy_set:
        # if two urls are provided, set them for each mode of transport individually
        proxy_mounts["http://"] = httpx.HTTPTransport(proxy=str(proxy.http_url))
        proxy_mounts["https://"] = httpx.HTTPTransport(proxy=str(proxy.https_url), verify=ssl_context)
    elif not http_proxy_set and not https_proxy_set:
        # if no urls are provided, do nothing
        pass
    else:
        # if one url is provided, use it for both modes of transport
        proxy_url = str(proxy.http_url) if http_proxy_set else str(proxy.https_url)

        proxy_mounts["http://"] = httpx.HTTPTransport(proxy=proxy_url)
        proxy_mounts["https://"] = httpx.HTTPTransport(proxy=proxy_url, verify=ssl_context)

    if len(proxy_mounts) == 0:
        return None

    return proxy_mounts


def get_flame_hub_auth_flow(
    settings: Annotated[Settings, Depends(get_settings)],
    ssl_context: Annotated[ssl.SSLContext, Depends(get_ssl_context)],
    proxy_mounts: Annotated[ProxyMount, Depends(get_proxy_mounts)],
):
    if settings.hub.auth.flow == AuthFlow.password:
        return flame_hub.auth.PasswordAuth(
            settings.hub.auth.username,
            settings.hub.auth.password,
            client=httpx.Client(base_url=str(settings.hub.auth_base_url), verify=ssl_context, mounts=proxy_mounts),
        )

    if settings.hub.auth.flow == AuthFlow.client:
        return flame_hub.auth.ClientAuth(
            settings.hub.auth.id,
            settings.hub.auth.secret,
            client=httpx.Client(base_url=str(settings.hub.auth_base_url), verify=ssl_context, mounts=proxy_mounts),
        )

    raise NotImplementedError(f"unknown auth flow {settings.hub.auth.flow}")


def get_core_client(
    settings: Annotated[Settings, Depends(get_settings)],
    auth_flow: Annotated[
        flame_hub.auth.ClientAuth | flame_hub.auth.PasswordAuth,
        Depends(get_flame_hub_auth_flow),
    ],
    ssl_context: Annotated[ssl.SSLContext, Depends(get_ssl_context)],
    proxy_mounts: Annotated[ProxyMount, Depends(get_proxy_mounts)],
):
    return flame_hub.CoreClient(
        client=httpx.Client(
            base_url=str(settings.hub.core_base_url), auth=auth_flow, verify=ssl_context, mounts=proxy_mounts
        )
    )


def get_storage_client(
    settings: Annotated[Settings, Depends(get_settings)],
    auth_flow: Annotated[
        flame_hub.auth.ClientAuth | flame_hub.auth.PasswordAuth,
        Depends(get_flame_hub_auth_flow),
    ],
    ssl_context: Annotated[ssl.SSLContext, Depends(get_ssl_context)],
    proxy_mounts: Annotated[ProxyMount, Depends(get_proxy_mounts)],
):
    return flame_hub.StorageClient(
        client=httpx.Client(
            base_url=str(settings.hub.storage_base_url), auth=auth_flow, verify=ssl_context, mounts=proxy_mounts
        )
    )


def get_postgres_db(
    settings: Annotated[Settings, Depends(get_settings)],
):
    pg = settings.postgres

    return pw.PostgresqlDatabase(
        pg.db,
        user=pg.user,
        password=pg.password,
        host=pg.host,
        port=pg.port,
    )


def get_ecdh_private_key_from_path(crypto_config: FileCryptoConfig):
    return crypto.load_ecdh_private_key_from_path(crypto_config.ecdh_private_key_path)


def get_ecdh_private_key_from_bytes(crypto_config: RawCryptoConfig):
    return crypto.load_ecdh_private_key(
        # replace literal newlines with real newlines (e.g. if provided via env variable)
        crypto_config.ecdh_private_key.replace(b"\\n", b"\n")
    )


def get_ecdh_private_key(settings: Annotated[Settings, Depends(get_settings)]):
    # settings enforce that either path or bytes are set
    if settings.crypto.provider == CryptoProvider.raw:
        return get_ecdh_private_key_from_bytes(settings.crypto)

    if settings.crypto.provider == CryptoProvider.file:
        return get_ecdh_private_key_from_path(settings.crypto)

    raise NotImplementedError(f"unknown crypto provider {settings.crypto.provider}")
