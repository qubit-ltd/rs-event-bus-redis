#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
source "$project_root/ci/prepare-event-bus.sh"

diagnostic=$(check_local_redis_patch "$project_root/ci/fixtures/redis-patch-stale.toml" "$project_root" 2>&1) && {
    echo "expected stale redis patch to be rejected" >&2
    exit 1
}
grep -Fq "[patch.crates-io] key 'redis'" <<<"$diagnostic" || {
    echo "stale patch diagnostic did not identify the incorrect key" >&2
    exit 1
}
grep -Fq "qubit-redis" <<<"$diagnostic" || {
    echo "stale patch diagnostic did not identify the package name" >&2
    exit 1
}

check_local_redis_patch "$project_root/ci/fixtures/redis-patch-correct.toml" "$project_root"
echo "prepare-event-bus preflight fixtures passed"
