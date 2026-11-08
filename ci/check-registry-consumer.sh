#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "usage: $0 [--mode source-candidate|published] --provider-version X.Y.Z --core-version X.Y.Z --spi-version X.Y.Z" >&2
}

mode=source-candidate
provider_version=
core_version=
spi_version=
while (($#)); do
    case "$1" in
        --mode)
            if (($# < 2)); then usage; exit 2; fi
            mode=$2
            shift 2
            ;;
        --provider-version|--core-version|--spi-version)
            option=$1
            if (($# < 2)); then usage; exit 2; fi
            case "$option" in
                --provider-version) provider_version=$2 ;;
                --core-version) core_version=$2 ;;
                --spi-version) spi_version=$2 ;;
            esac
            shift 2
            ;;
        *) usage; echo "error: unknown argument: $1" >&2; exit 2 ;;
    esac
done
case "$mode" in
    source-candidate|published) ;;
    *) usage; echo "error: unknown registry consumer mode: $mode" >&2; exit 2 ;;
esac
if [[ -z "$provider_version" || -z "$core_version" || -z "$spi_version" ]]; then
    usage
    echo "error: all three candidate versions are required" >&2
    exit 2
fi

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
fixture="$project_root/tests/fixtures/registry_consumer"
consumer_dir=$(mktemp -d "${TMPDIR:-/tmp}/redis-bus-registry-consumer.XXXXXX")
stage=argument-validation
on_exit() {
    status=$?
    if ((status != 0)); then
        echo "registry consumer mode=$mode failed during $stage (exit $status)" >&2
    fi
    rm -rf "$consumer_dir"
}
trap on_exit EXIT
echo "registry consumer mode=$mode: provider=$provider_version core=$core_version spi=$spi_version"

stage=prepare
python3 "$project_root/ci/registry_consumer_target.py" prepare \
    --source-manifest "$project_root/Cargo.toml" --fixture "$fixture" --output "$consumer_dir" \
    --mode "$mode" \
    --provider-version "$provider_version" --core-version "$core_version" --spi-version "$spi_version"
stage=manifest-check
python3 "$project_root/ci/check_registry_consumer_manifest.py" "$consumer_dir/Cargo.toml"
cd "$consumer_dir"
stage=generate-lockfile
cargo +1.94.0 generate-lockfile
for features in 'sync,discovery' 'async,discovery'; do
    stage="check-$features"
    cargo +1.94.0 check --locked --no-default-features --features "$features"
done
stage=check-default-features
cargo +1.94.0 check --locked
stage=metadata
metadata=$(cargo +1.94.0 metadata --format-version 1 --locked)
stage=metadata-verify
printf '%s' "$metadata" | python3 "$project_root/ci/registry_consumer_target.py" verify \
    --provider-version "$provider_version" --core-version "$core_version" --spi-version "$spi_version"
for features in 'sync,discovery' 'async,discovery'; do
    stage="run-$features"
    cargo +1.94.0 run --locked --no-default-features --features "$features"
done
stage=run-default-features
cargo +1.94.0 run --locked
echo "registry consumer checks passed for mode=$mode provider=$provider_version core=$core_version spi=$spi_version"
stage=complete
