import os


def __get_env(env_name: str, val_def: str | None = None) -> str:
    val = os.getenv(env_name, val_def)

    if val is None:
        raise ValueError(f"environment variable `{env_name}` is not set")

    return val


def hub_core_base_url():
    return __get_env("HUB__CORE_BASE_URL", "https://core.privateaim.dev")


def hub_storage_base_url():
    return __get_env("HUB__STORAGE_BASE_URL", "https://storage.privateaim.dev")


def hub_auth_base_url():
    return __get_env("HUB__AUTH_BASE_URL", "https://auth.privateaim.dev")


def hub_password_auth_username():
    return __get_env("HUB__AUTH__USERNAME")


def hub_password_auth_password():
    return __get_env("HUB__AUTH__PASSWORD")


def hub_client_auth_id():
    return __get_env("HUB__AUTH__ID")


def hub_client_auth_secret():
    return __get_env("HUB__AUTH__SECRET")


def oidc_certs_url():
    return __get_env("OIDC__CERTS_URL", "http://localhost:8001/.well-known/jwks.json")


def oidc_client_id_claim_name():
    return __get_env("OIDC__CLIENT_ID_CLAIM_NAME", "client_id")


def async_max_retries():
    return __get_env("ASYNC_MAX_RETRIES", "10")


def async_retry_delay_seconds():
    return __get_env("ASYNC_RETRY_DELAY_SECONDS", "1")


def hub_adapter_client_id():
    return __get_env("HUB_ADAPTER_CLIENT_ID", "hub-adapter")
