#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$project_root/.infra/tools/cleanup-build-artifacts.sh"
export RS_INFRA_STYLE_TOOLCHAIN="${RS_INFRA_STYLE_TOOLCHAIN:-nightly-2026-06-05}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"
if [ -f "$project_root/.infra/style/rustfmt.toml" ]; then
    export RS_INFRA_STYLE_RUSTFMT_CONFIG="$project_root/.infra/style/rustfmt.toml"
elif [ -f "$project_root/rustfmt.toml" ]; then
    export RS_INFRA_STYLE_RUSTFMT_CONFIG="$project_root/rustfmt.toml"
fi
bash "$project_root/ci/prepare-event-bus.sh"
"$project_root/.infra/tools/infra-tool.sh" rs-infra-ci --project "$project_root" "$@" check
