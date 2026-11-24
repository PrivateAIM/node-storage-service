import asyncio
import logging
import time

from fastapi import status, HTTPException
import httpx2 as httpx
from jwcrypto import jwk, jws


logger = logging.getLogger(__name__)


class JWKSCache:
    """Holds the parsed JWKS and refetches it only when a kid is unknown (rate-limited).

    The cache never hits the JWKS endpoint more often than min_refresh_interval (in seconds), no matter if a kid is
    present in the cache or not. max_age (also in seconds) serves as a safety net, so that revoked or rotated-out keys
    don't live in the cache forever.
    """

    def __init__(self, jwks_url: str, min_refresh_interval: float = 30.0, max_age: float = 3_600.0) -> None:
        self.jwks_url = jwks_url
        self.min_refresh_interval = min_refresh_interval
        self.max_age = max_age
        self.jwks = jwk.JWKSet()
        self.last_success = 0.0  # monotonic
        self.last_attempt = 0.0  # monotonic, but includes failed attempts

        self._lock = asyncio.Lock()

    def is_usable(self, kid: str | None) -> bool:
        if time.monotonic() - self.last_success > self.max_age:
            return False
        return kid is None or self.jwks.get_key(kid) is not None

    async def download(self) -> jwk.JWKSet:
        async with httpx.AsyncClient() as client:
            try:
                r = await client.get(self.jwks_url)
                r.raise_for_status()
            except httpx.HTTPError as e:
                logger.exception("Failed to read JWKS")
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail="Auth provider is unavailable",
                ) from e

        try:
            return jwk.JWKSet.from_json(r.text)
        except (ValueError, jwk.InvalidJWKValue) as e:
            logger.exception("Failed to parse JWK set")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to parse JWK set",
            ) from e

    async def get(self, kid: str | None) -> jwk.JWKSet:
        if self.is_usable(kid):  # The common case once the cache is warm.
            return self.jwks

        async with self._lock:
            if self.is_usable(kid):  # Another request may have refreshed while we waited for the lock.
                return self.jwks

            now = time.monotonic()
            if now - self.last_attempt < self.min_refresh_interval:
                # Unknown kid or expired cache, but we refreshed very recently so we serve what we have. The validation
                # will reject the unknown kid.
                return self.jwks

            self.last_attempt = now
            self.jwks = await self.download()
            self.last_success = now

            return self.jwks


def peek_kid(token: str) -> str | None:
    """Reads the kid from the JWT header. Returns None if its absent or malformed."""
    try:
        kid = jws.JWS()
        kid.deserialize(token)
        header = kid.jose_header
    except jws.InvalidJWSObject:
        return None

    if not isinstance(header, dict):
        return None

    return header.get("kid")
