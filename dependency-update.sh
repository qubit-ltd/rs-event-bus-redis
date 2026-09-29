#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
bash "$project_root/ci/prepare-event-bus.sh"
exec "$project_root/.infra/tools/dependency-update.sh" "$@"
