#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$project_root/.infra/tools/cleanup-build-artifacts.sh"
"$project_root/.infra/tools/prepare-local-path-dependencies.sh"
"$project_root/.infra/tools/infra-tool.sh" rs-infra-coverage --project "$project_root" collect "$@"
"$project_root/.infra/tools/coverage-report.sh"
