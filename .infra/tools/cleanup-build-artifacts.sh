#!/usr/bin/env bash
set -euo pipefail

# Remove transient Cargo outputs while preserving coverage reports and source files.
cleanup_build_artifacts() {
    local status=$?
    if [ "${RS_INFRA_ARTIFACT_CLEANUP:-1}" = "1" ]; then
        for directory in \
            "$project_root/target/debug" \
            "$project_root/target/release" \
            "$project_root/target/llvm-cov-target" \
            "$project_root/fuzz/target"; do
            if [ -d "$directory" ]; then
                echo "Cleaning transient build artifacts: $directory"
                command rm -rf -- "$directory"
            fi
        done
    fi
    if [ "$status" -eq 0 ]; then
        echo "Build artifact cleanup completed"
    else
        echo "Build artifact cleanup completed after failure (exit code $status)" >&2
    fi
    return "$status"
}

trap cleanup_build_artifacts EXIT
