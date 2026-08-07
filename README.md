![GitHub Release](https://img.shields.io/github/v/release/PrivateAIM/node-storage-service?label=Release)
![Code Coverage](https://img.shields.io/badge/Coverage-87%25-yellowgreen.svg)
![License](https://img.shields.io/github/license/PrivateAIM/node-storage-service?label=License)
[![Conventional Commits](https://img.shields.io/badge/Conventional%20Commits-1.0.0-%23FE5196?logo=conventionalcommits&logoColor=white)](https://conventionalcommits.org)

# FLAME Node Storage Service

The FLAME Node Storage Service is responsible for handling result files for federated analyses within FLAME.
It uses a local object storage to store intermediate files, as well as to enqueue files for upload to the FLAME Hub.

# Setup

You will need access to a S3 instance and an identification provider that offers a JWKS endpoint for the access
tokens it issues and a Postgres instance.

For manual installation, you will need Python 3.10 or higher and [Poetry](https://python-poetry.org/) installed.
Clone the repository and run `poetry install` in the root directory.
Create a copy of `.env.example`, name it `.env` and configure to your needs.
Finally, use the command `flame-storage` to start the service.

```
$ git clone https://github.com/PrivateAIM/node-storage-service.git
$ cd node-storage-service
$ poetry install
$ cp .env.example .env
$ poetry run flame-storage
```

To run an ephemeral version of the Node Storage Service with all services it needs pre-configured,
simply run `docker compose up -d`.
You can best explore the API by checking the documentation out at http://localhost:8080/docs.
To acquire a JWT for use with the API, [use the corresponding script](./docker/keycloak/issue-jwt.sh).
Be aware that, unless you test against your own Hub instance, the actual responses of this service will not be
very helpful.

# Configuration

The following table shows all available configuration options.

| **Environment variable**       | **Description**                                                                                 | **Default**                       |  **Required**  |
|--------------------------------|-------------------------------------------------------------------------------------------------|-----------------------------------|:--------------:|
| HUB__CORE_BASE_URL             | Base URL for the FLAME Core API                                                                 | https://core.privateaim.net       |                |
| HUB__STORAGE_BASE_URL          | Base URL for the FLAME Storage API                                                              | https://storage.privateaim.net    |                |
| HUB__AUTH_BASE_URL             | Base URL for the FLAME Auth API                                                                 | https://auth.privateaim.net       |                |
| HUB__AUTH__ID                  | Client ID to use for obtaining access tokens using client credentials auth scheme               |                                   |       x        |
| HUB__AUTH__SECRET              | Client secret to use for obtaining access tokens using client credentials auth scheme           |                                   |       x        |
| S3__ENDPOINT                   | S3 API endpoint (without scheme)                                                                |                                   |       x        |
| S3__ACCESS_KEY                 | Access key for interacting with S3 API                                                          |                                   |       x        |
| S3__SECRET_KEY                 | Secret key for interacting with S3 API                                                          |                                   |       x        |
| S3__BUCKET                     | Name of S3 bucket to store result files in                                                      |                                   |       x        |
| S3__REGION                     | Region of S3 bucket to store result files in                                                    | us-east-1                         |                |
| S3__USE_SSL                    | Flag for en-/disabling encrypted traffic to S3 API                                              | 0                                 |                |
| OIDC__CERTS_URL                | URL to OIDC-complaint JWKS endpoint for validating JWTs                                         |                                   |       x        |
| OIDC__CLIENT_ID_CLAIM_NAME     | JWT claim to identify authenticated requests with                                               | client_id                         |                |
| POSTGRES__HOST                 | Hostname of Postgres instance for storing tags and result meta data                             |                                   |       x        |
| POSTGRES__PORT                 | Port of Postgres instance for storing tags and result meta data                                 | 5432                              |                |
| POSTGRES__USER                 | Username for access to Postgres instance for storing tags and result meta data                  |                                   |       x        |
| POSTGRES__PASSWORD             | Password for access to Postgres instance for storing tags and result meta data                  |                                   |       x        |
| POSTGRES__DB                   | Database of Postgres instance for storing tags and result meta data                             |                                   |       x        |
| POSTGRES__MAX_CONNECTIONS      | Maximum number of connections for pooled Postgres instance per worker                           | 20                                |                |
| POSTGRES__STALE_TIMEOUT        | Number of seconds to allow connections to be used                                               | 300                               |                |
| POSTGRES__KEEPALIVES_IDLE      | How long a connection needs to be idle before the first TCP keepalive probe is sent             | 60                                |                |
| POSTGRES__KEEPALIVES_INTERVAL  | Time between successive TCP probes after the first one                                          | 30                                |                |
| POSTGRES__KEEPALIVES_COUNT     | Number of failed TCP probes before declaring a connection dead                                  | 3                                 |                |
| POSTGRES__MIGRATIONS_TABLENAME | Name of the table where peewee stores which migrations have been executed.                      | storage_service_migration_history |                |
| CRYPTO__PROVIDER               | Provider for ECDH private key (`raw` or `file`)                                                 |                                   |       x        |
| CRYPTO__ECDH_PRIVATE_KEY       | Contents of ECDH private key file                                                               |                                   | x<sup>1)</sup> |
| CRYPTO__ECDH_PRIVATE_KEY_PATH  | Path to ECDH private key file                                                                   |                                   | x<sup>2)</sup> |
| PROXY__HTTP_URL                | URL of HTTP proxy<sup>3)</sup>                                                                  |                                   |                |
| PROXY__HTTPS_URL               | URL of HTTPS proxy<sup>3)</sup>                                                                 |                                   |                |
| EXTRA_CA_CERTS                 | Path to a certificate bundle containing additional certificates to be added to the SSL context. |                                   |                |
| HUB_ADAPTER_CLIENT_ID          | Keycloak client ID for the Hub Adapter client.                                                  | hub-adapter                       |                |

<sup>1)</sup> Only if `CRYPTO__PROVIDER` is set to `raw`  
<sup>2)</sup> Only if `CRYPTO__PROVIDER` is set to `file`  
<sup>3)</sup> If only one of the two URLs is set, it will be used for both HTTP and HTTPS transport

## Note on running tests

Set up tests by copying `.env.example` into a new file called `.env.test`.

```
$ cp .env.example .env.test
```

[testcontainers](https://testcontainers.com/) will automatically start up and tear down containers used only for testing.
Disable them by setting `PYTEST__USE_TESTCONTAINERS=0`.

You can then execute tests by running `pytest`.
Pre-existing environment variables take precedence and will not be overwritten by the contents of `.env.test`.

Since analyses can only be created by user accounts and analyses need to be created during tests, the environment
variables `PYTEST__HUB_USER` and `PYTEST__HUB_USER_PASSWORD` need to be set.

OIDC does not need to be configured, since an OIDC-compatible endpoint will be spawned alongside the tests that are
being run.
A [pre-generated keypair](tests/assets/keypair.pem) is used for this purpose.
This allows all tests to generate valid JWTs as well as the service to validate them.
The keypair is for development purposes only and should not be used in a productive setting.

Some tests need a running FLAME Hub.
To exclude these tests, append `-m "not live"` to the command above.
Similarly, appending `-m live` will only run tests that need a Hub.

For testing against a forward proxy, [check the README in the `proxy` directory](./proxy/README.md).

For testing without using testcontainers, this repository provides a Docker compose file that spins up all necessary
services and executes database migrations. Simply run the following command.

```
$ docker compose -f tests/docker-compose.yml up -d --build
```

# License

The FLAME Node Storage Service is released under the Apache 2.0 license.
