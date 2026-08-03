import random

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa, ed448, dsa, x448
import pytest

from project.crypto import encrypt_default, decrypt_default, load_ecdh_public_key, load_ecdh_private_key
from tests.common.helpers import next_ecdh_keypair


NON_ECDH_PRIVATE_KEYS = (
    rsa.generate_private_key(public_exponent=65_537, key_size=2_048),
    ed448.Ed448PrivateKey.generate(),
    dsa.generate_private_key(key_size=2_048),
    x448.X448PrivateKey.generate(),
)


def _get_public_key_in_pem_format(private_key):
    public_key = private_key.public_key()
    return public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _get_private_key_in_pem_format(private_key):
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def test_encrypt_decrypt():
    alice_private, alice_public = next_ecdh_keypair()
    bob_private, bob_public = next_ecdh_keypair()

    t = random.randbytes(1_024)  # generate arbitrary bytes
    ct = encrypt_default(alice_private, bob_public, t)
    t2 = decrypt_default(bob_private, alice_public, ct)

    assert t == t2
    assert ct != t


def test_load_ecdh_private_key():
    private_key = ec.generate_private_key(ec.SECP384R1())
    private_numbers = private_key.private_numbers()

    assert private_numbers == load_ecdh_private_key(_get_private_key_in_pem_format(private_key)).private_numbers()


@pytest.mark.parametrize("private_key", NON_ECDH_PRIVATE_KEYS)
def test_load_ecdh_private_key_type_error(private_key):
    with pytest.raises(TypeError) as e:
        load_ecdh_private_key(_get_private_key_in_pem_format(private_key))

    assert str(e.value) == "Expected an EC private key."


def test_load_ecdh_public_key():
    private_key = ec.generate_private_key(ec.SECP384R1())

    assert private_key.public_key() == load_ecdh_public_key(_get_public_key_in_pem_format(private_key))


@pytest.mark.parametrize("private_key", NON_ECDH_PRIVATE_KEYS)
def test_load_ecdh_public_key_type_error(private_key):
    with pytest.raises(TypeError) as e:
        load_ecdh_public_key(_get_public_key_in_pem_format(private_key))

    assert str(e.value) == "Expected an EC public key."
