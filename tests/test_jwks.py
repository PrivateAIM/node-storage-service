import asyncio
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import threading
import time
from unittest.mock import AsyncMock

from fastapi import HTTPException, status
import pytest
from jwcrypto import jwk, jwt

from project.jwks import peek_kid, JWKSCache
from tests.common.helpers import next_random_string
from tests.common.auth import get_oid_test_jwk


@pytest.fixture
def dummy_jwks_endpoint():
    return "http://localhost:64982"


@pytest.fixture
def serve_dummy_jwks_endpoint_factory(dummy_jwks_endpoint):
    @contextmanager
    def _serve(jwk_: str | None = None):
        if jwk_ is None:
            jwks = jwk.JWKSet()
            jwks["keys"].add(get_oid_test_jwk())
            jwks_str = jwks.export(private_keys=False)
        else:
            jwks_str = jwk_

        class JWKSHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(jwks_str.encode("utf-8"))

        httpd_url = urllib.parse.urlparse(dummy_jwks_endpoint)
        httpd = HTTPServer((httpd_url.hostname, httpd_url.port), JWKSHandler)

        t = threading.Thread(target=httpd.serve_forever)
        t.start()

        try:
            yield
        finally:
            httpd.shutdown()
            httpd.server_close()
            t.join()

    return _serve


@pytest.fixture
def jwks_cache(dummy_jwks_endpoint) -> JWKSCache:
    return JWKSCache(jwks_url=dummy_jwks_endpoint)


@pytest.mark.parametrize("now", [0.0, 10.0, 100.0, 4_000.0])
@pytest.mark.parametrize("kid", [None, "unknown"])
def test_cold_cache_downloads_regardless_of_uptime(monkeypatch, jwks_cache, now, kid):
    monkeypatch.setattr("project.jwks.time.monotonic", lambda: now)
    keys = jwk.JWKSet()
    keys.add(get_oid_test_jwk())
    download = AsyncMock(return_value=keys)
    monkeypatch.setattr(jwks_cache, "download", download)

    assert asyncio.run(jwks_cache.get(kid)) is keys
    assert asyncio.run(jwks_cache.get(kid)) is keys
    download.assert_awaited_once_with()


def test_peek_id():
    kid = next_random_string()
    key = jwk.JWK.generate(kty="EC", crv="P-256", kid=kid, use="sig", alg="ES256")
    token = jwt.JWT(
        header={"alg": "ES256", "kid": kid, "typ": "JWT"},
        claims={"sub": "user-123", "iat": int(time.time()), "exp": int(time.time()) + 300},
    )
    token.make_signed_token(key)
    encoded = token.serialize()

    assert peek_kid(encoded) == kid


def test_peek_kid_none():
    assert peek_kid(next_random_string()) is None


def test_jwk_parse_error(serve_dummy_jwks_endpoint_factory, jwks_cache):
    with serve_dummy_jwks_endpoint_factory(next_random_string()):
        with pytest.raises(HTTPException) as e:
            asyncio.run(jwks_cache.get(next_random_string()))

        assert e.value.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
        assert e.value.detail == "Failed to parse JWK set"


def test_no_refetch_in_refresh_interval(serve_dummy_jwks_endpoint_factory, jwks_cache):
    with serve_dummy_jwks_endpoint_factory():
        asyncio.run(jwks_cache.get(next_random_string()))  # Trigger download.

        assert jwks_cache.jwks.get_key(get_oid_test_jwk()["kid"]) is not None

        jwks_cache.jwks = jwk.JWKSet()
        asyncio.run(jwks_cache.get(next_random_string()))  # Should not download anything.

        assert jwks_cache.jwks.get_key(get_oid_test_jwk()["kid"]) is None
