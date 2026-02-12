#!/bin/bash
set -e

# This script is used by a pre-commit to lint the OpenAPI specification of this service with Spectral.
openapi-spec spectral/openapi.json
docker run --rm -v "$(pwd)":/tmp stoplight/spectral lint --ruleset "/tmp/.spectral.yaml" "/tmp/spectral/openapi.json" "$@"
