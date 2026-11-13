#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)

# TLS integration fixtures generate short-lived certificates with the OpenSSL
# CLI. Ubuntu 26 GitHub runners no longer guarantee that binary is preinstalled.
if [[ "${GITHUB_ACTIONS:-false}" == "true" && "${RUNNER_OS:-}" == "Linux" ]] && ! command -v openssl >/dev/null 2>&1; then
    sudo apt-get update
    sudo apt-get install --yes openssl
fi

exec "$project_root/.infra/bootstrap.sh" .infra/bin/prepare-local-path-dependencies.sh "$@"
