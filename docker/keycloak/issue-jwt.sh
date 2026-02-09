#!/bin/bash
set -euo pipefail

_print_error() {
    printf "\033[31;1m[!]\033[0m %s\n" "$1"
}

_exit_with_code() {
    _print_error "$2"
    exit "$1"
}

cmds=(
  "docker"
  "jq"
)

for cmd in "${cmds[@]}"
do
  if ! type "$cmd" > /dev/null;
  then
    _exit_with_code 1 "$cmd must be installed on the system"
  fi
done

kc_ip_addr="$(docker network inspect node-storage-service_default | jq -r '.[0].Containers[] | select(.Name | test("keycloak")) | .IPv4Address' | cut -d'/' -f1)"
kc_ip_port="8080"
kc_client_id="node-storage-service"
kc_client_secret="$(jq -r  '.clients[] | select(.clientId == "node-storage-service") | .secret' import/realm-export.json)"

curl -s -X POST -d "client_id=$kc_client_id" \
  -d "client_secret=$kc_client_secret" \
  -d "grant_type=client_credentials" \
  "http://$kc_ip_addr:$kc_ip_port/realms/flame/protocol/openid-connect/token" | \
  jq -r ".access_token"
