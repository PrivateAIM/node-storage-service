import random
import string
import time
import uuid
from typing import Callable

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import httpx

from tests.common import env


def eventually(predicate: Callable[[], bool]) -> bool:
    """Return True if the predicate passed into this function returns True after a set amount of attempts.
    Between each attempt there is a delay of one second. The amount of retries can be configured with the
    PYTEST__ASYNC_MAX_RETRIES environment variable."""
    max_retries = int(env.async_max_retries())
    delay_secs = int(env.async_retry_delay_seconds())

    for _ in range(max_retries):
        if not predicate():
            time.sleep(delay_secs)
            continue

        return True

    return False


def next_uuid():
    """Get random UUID as string."""
    return str(uuid.uuid4())


def next_prefixed_name():
    """Get random UUID prefixed with 'node-it-'."""
    return f"node-it-{next_uuid()}"


def is_valid_uuid(val: str):
    """Return True if the value provided can be parsed into a valid UUID."""
    try:
        uuid.UUID(val)
    except ValueError:
        return False

    return True


def next_random_bytes(rng: random.Random, n: int = 16):
    """Return a bytes object with random content. (default length: 16)"""
    return rng.randbytes(n)


def next_ecdh_keypair():
    private_key = ec.generate_private_key(curve=ec.SECP384R1())
    public_key = private_key.public_key()

    return private_key, public_key


def next_ecdh_keypair_bytes():
    """Return a new ECDH keypair."""
    private_key, public_key = next_ecdh_keypair()

    return (
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ),
    )


def next_random_string(charset=string.ascii_letters, length: int = 20):
    assert length > 0
    assert len(charset) > 0

    return "".join([random.choice(charset) for _ in range(length)])


def wait_for_analysis_bucket_file(core_client: httpx.Client, analysis_id: str | uuid.UUID):
    """After a file is uploaded, it takes some time until the Hub creates the corresponding analysis bucket file. This
    function tries multiple times to retrieve the resource and returns True or False depending on if the resource is
    available on the Hub."""

    def wrapper():
        analysis_bucket_files = core_client.find_analysis_bucket_files(filter={"analysis_id": analysis_id})
        return len(analysis_bucket_files) == 1

    return eventually(wrapper)
