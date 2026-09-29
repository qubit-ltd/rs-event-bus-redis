#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$project_root/.infra/tools/cleanup-build-artifacts.sh"
"$project_root/.infra/tools/prepare-local-path-dependencies.sh"
(
    eval "$(cargo llvm-cov show-env --sh)"
    cargo build --examples --all-features --locked --target-dir "$project_root/target/llvm-cov-target"
)
coverage_report="$project_root/target/infra/coverage/raw.json"
mkdir -p "$(dirname "$coverage_report")"
cargo llvm-cov --locked --no-clean --package qubit-event-bus-redis --all-features --json --output-path "$coverage_report" "$@" -- --test-threads=1
"$project_root/.infra/tools/infra-tool.sh" rs-infra-coverage --project "$project_root" check --input "$coverage_report"
"$project_root/.infra/tools/coverage-report.sh"
