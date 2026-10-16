#!/usr/bin/env bash
set -euo pipefail

check_local_redis_patch() {
    local config_file=$1
    local project_root=$2
    [ -f "$config_file" ] || return 0

    if awk '
        /^[[:space:]]*\[patch\.crates-io\][[:space:]]*([#].*)?$/ { in_patch=1; next }
        /^[[:space:]]*\[/ { in_patch=0 }
        in_patch && /^[[:space:]]*redis[[:space:]]*=/ && /path[[:space:]]*=[[:space:]]*"third_party\/redis-rs"/ { found=1 }
        END { exit !found }
    ' "$config_file"; then
        echo "error: $config_file has [patch.crates-io] key 'redis' pointing to the removed local qubit-redis fork at third_party/redis-rs; remove or update this stale patch" >&2
        return 1
    fi
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
main() {
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
check_local_redis_patch "$project_root/.cargo/config.toml" "$project_root"
"$project_root/.infra/bin/prepare-local-path-dependencies.sh"
# A local developer checkout is never reset. Only an explicitly enabled CI
# checkout may change HEAD, and dirty work is protected even in that mode.
dependencies=(rs-event-bus rs-task)
revision_files=(event-bus-revision.txt task-revision.txt)
revisions=()
current_revisions=()
for index in "${!dependencies[@]}"; do
    dependency=${dependencies[$index]}
    dependency_root="$project_root/../$dependency"
    revision=$(cat "$project_root/ci/${revision_files[$index]}")
    [[ "$revision" =~ ^[0-9a-f]{40}$ ]] || {
        echo "error: invalid $dependency revision" >&2
        exit 1
    }
    current_revision=$(git -C "$dependency_root" rev-parse HEAD)
    dependency_status=$(git -C "$dependency_root" status --porcelain --untracked-files=all)
    revisions+=("$revision")
    current_revisions+=("$current_revision")
    if [ "${RS_EVENT_BUS_PIN:-0}" != "1" ]; then
        if [ -n "$dependency_status" ] || [ "$current_revision" != "$revision" ]; then
            echo "warning: local $dependency differs from pinned revision $revision; checkout preserved" >&2
        fi
        continue
    fi
    [ "${GITHUB_ACTIONS:-false}" = "true" ] || {
        echo "error: RS_EVENT_BUS_PIN=1 requires an ephemeral GitHub Actions checkout" >&2
        exit 1
    }
    [ -d "$dependency_root/.git" ] || {
        echo "error: refusing to pin a shared $dependency worktree" >&2
        exit 1
    }
    [ -z "$dependency_status" ] || {
        echo "error: refusing to pin dirty $dependency checkout" >&2
        exit 1
    }
done
[ "${RS_EVENT_BUS_PIN:-0}" = "1" ] || exit 0

# Validate both checkouts before changing either dependency's HEAD.
for index in "${!dependencies[@]}"; do
    dependency=${dependencies[$index]}
    dependency_root="$project_root/../$dependency"
    revision=${revisions[$index]}
    if [ "${current_revisions[$index]}" != "$revision" ]; then
        git -C "$dependency_root" fetch --no-tags origin "$revision"
        git -C "$dependency_root" checkout --detach "$revision"
    fi
    [ "$(git -C "$dependency_root" rev-parse HEAD)" = "$revision" ] || {
        echo "error: $dependency revision did not match pin" >&2
        exit 1
    }
done
}

main "$@"
fi
