#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
fixture="$project_root/tests/fixtures/registry_consumer"
consumer_dir=$(mktemp -d "${TMPDIR:-/tmp}/redis-bus-registry-consumer.XXXXXX")
trap 'rm -rf "$consumer_dir"' EXIT

python3 "$project_root/ci/check_registry_consumer_manifest.py" "$fixture/Cargo.toml"

cp -R "$fixture/." "$consumer_dir/"
cd "$consumer_dir"

cargo +1.94.0 generate-lockfile
for features in 'sync,discovery' 'async,discovery'; do
    cargo +1.94.0 check --locked --no-default-features --features "$features"
done
cargo +1.94.0 check --locked

metadata=$(cargo +1.94.0 metadata --format-version 1 --locked)
python3 -c '
import json, sys
metadata = json.load(sys.stdin)
required = {"qubit-event-bus", "qubit-event-bus-redis", "qubit-spi"}
packages = {package["name"]: package for package in metadata["packages"] if package["name"] in required}
missing = required - packages.keys()
if missing:
    raise SystemExit(f"missing registry packages: {sorted(missing)}")
for name, package in packages.items():
    source = package["source"]
    if name == "registry-consumer" and source is None:
        continue
    if source != "registry+https://github.com/rust-lang/crates.io-index":
        raise SystemExit(f"{name} did not resolve from crates.io: {source}")
' <<<"$metadata"

echo "published registry consumer checks passed"
