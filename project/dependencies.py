import json
import logging
from typing import Annotated
import uuid

from cryptography.hazmat.primitives.asymmetric import ec
from flame_hub import CoreClient, StorageClient
import httpx2 as httpx
from playhouse.pool import PooledPostgresqlDatabase
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from httpx2 import HTTPError
from jwcrypto import jwk, jwt, common
from minio import Minio
from starlette import status

from project.config import Settings
from project.models import AppState


security = HTTPBearer()
logger = logging.getLogger(__name__)


def get_app_state(request: Request) -> AppState:
    return request.app.state.app_state


def get_settings(state: Annotated[AppState, Depends(get_app_state)]) -> Settings:
    return state.settings


def get_local_s3(state: Annotated[AppState, Depends(get_app_state)]) -> Minio:
    return state.s3_client


def get_core_client(state: Annotated[AppState, Depends(get_app_state)]) -> CoreClient:
    return state.core_client


def get_storage_client(state: Annotated[AppState, Depends(get_app_state)]) -> StorageClient:
    return state.storage_client


def get_node_id(state: Annotated[AppState, Depends(get_app_state)]) -> uuid.UUID:
    return state.node_id


def get_postgres_db(state: Annotated[AppState, Depends(get_app_state)]) -> PooledPostgresqlDatabase:
    return state.postgres


def get_ecdh_private_key(state: Annotated[AppState, Depends(get_app_state)]) -> ec.EllipticCurvePrivateKey:
    return state.ecdh_private_key


# TODO: It is not necessary to fetch JWKS on every request.
async def get_auth_jwks(settings: Annotated[Settings, Depends(get_settings)]) -> jwk.JWKSet:
    if settings.oidc.skip_jwt_validation:
        logger.warning("Since JWT validation is skipped, an empty JWKS is returned")
        return jwk.JWKSet()

    jwks_url = str(settings.oidc.certs_url)

    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(jwks_url)
            r.raise_for_status()
        except HTTPError:
            logger.exception("Failed to read OIDC config")

            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Auth provider is unavailable",
            )

    return jwk.JWKSet.from_json(r.text)


def get_client_id(
    settings: Annotated[Settings, Depends(get_settings)],
    jwks: Annotated[jwk.JWKSet, Depends(get_auth_jwks)],
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> str:
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
        client_id = jwt_data[settings.oidc.client_id_claim_name]
        return client_id
    except (common.JWException, ValueError):
        logger.exception("Failed to deserialize JWT")
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="JWT is malformed")


def get_analysis_id(
    client_id: Annotated[str, Depends(get_client_id)],
    core_client: Annotated[CoreClient, Depends(get_core_client)],
) -> str:
    """Resolve the analysis an incoming token belongs to.

    Authup client-credentials tokens identify the Hub-managed *client* (``client_id`` = the
    Authup client UUID), not the analysis. The Hub keeps that client id on the analysis, so the
    analysis is looked up with a single query filtered by client id. This requires a Hub that
    allows ``filter[clientId]`` on analyses (PrivateAIM/hub#1930).
    """
    try:
        client_uuid = uuid.UUID(client_id)
    except ValueError:
        # Authup client ids are UUIDs, so no analysis can match.
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Token is not associated with an analysis")

    try:
        analyses = core_client.find_analyses(filter={"clientId": str(client_uuid)}, page={"limit": 1})
    except HTTPError:
        logger.exception(f"Failed to resolve analysis for client ID {client_id}")
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Hub is unavailable")

    if len(analyses) == 0:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Token is not associated with an analysis")

    analysis_id = str(analyses[0].id)
    logger.debug(f"Resolved client ID {client_id} to analysis ID {analysis_id}")
    return analysis_id
