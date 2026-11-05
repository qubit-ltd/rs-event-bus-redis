#!/usr/bin/env bash
set -Eeuo pipefail

repo_root=$(git rev-parse --show-toplevel)
log_root=${PACKAGE_CANDIDATE_LOG_DIR:-"$repo_root/target/package-candidate-logs"}
mkdir -p "$log_root"
run_id=$(date -u +%Y%m%dT%H%M%SZ)-$$
log_dir="$log_root/$run_id"
mkdir -p "$log_dir"
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/redis-bus-package-candidate.XXXXXX")
phase=initialization
finish() {
  status=$?
  if (( status == 0 )); then
    printf 'GREEN phase=%s logs=%s\n' "$phase" "$log_dir"
  else
    printf 'BLOCKED phase=%s exit=%s logs=%s\n' "$phase" "$status" "$log_dir" >&2
  fi
  rm -rf "$tmp_dir"
}
trap finish EXIT
run() {
  local label=$1
  shift
  phase=$label
  printf 'FIRST_CHECK phase=%s\n' "$phase"
  "$@" >"$log_dir/$label.log" 2>&1 || { cat "$log_dir/$label.log" >&2; return 1; }
}

phase=archive
mkdir -p "$tmp_dir/archive"
export CARGO_TARGET_DIR="$tmp_dir/cargo-target"
export CARGO_HOME="$tmp_dir/cargo-home"
mkdir -p "$CARGO_HOME"
cd "$tmp_dir"
run package cargo +1.94.0 package --no-verify --allow-dirty --manifest-path "$repo_root/Cargo.toml"
crate_file=$(find "$CARGO_TARGET_DIR/package" -maxdepth 1 -type f -name '*.crate' -print -quit)
[[ -n "$crate_file" ]] || { echo 'cargo package produced no .crate archive' >&2; exit 1; }
printf 'archive=%s\n' "$(basename "$crate_file")" | tee "$log_dir/archive-identity.txt"

phase=extract
python3 - "$crate_file" "$tmp_dir/unpacked" <<'PY'
import sys, tarfile
from pathlib import PurePosixPath
archive, destination = sys.argv[1:]
with tarfile.open(archive, "r:gz") as crate:
    members = crate.getmembers()
    for member in members:
        path = PurePosixPath(member.name)
        if path.is_absolute() or ".." in path.parts or not (member.isdir() or member.isfile()):
            raise SystemExit(f"unsafe crate archive member: {member.name}")
    crate.extractall(destination, filter="data")
PY
crate_root=$(find "$tmp_dir/unpacked" -mindepth 1 -maxdepth 1 -type d -print -quit)
[[ -n "$crate_root" && -f "$crate_root/Cargo.toml" ]] || { echo 'normalized package manifest missing' >&2; exit 1; }
crate_name=$(python3 - "$crate_root/Cargo.toml" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as manifest_file:
    print(tomllib.load(manifest_file)["package"]["name"])
PY
)

phase=manifest
run manifest python3 "$repo_root/ci/package_candidate.py" check-manifest "$crate_root/Cargo.toml"

# The crate is the sole local source. Do not inherit workspace .cargo config or source replacement.
phase=cargo-config
if find "$repo_root/.cargo" -maxdepth 1 -type f -name 'config*' -print -quit 2>/dev/null | grep -q .; then
  echo 'repository .cargo configuration exists; refusing possible source replacement' >&2
  exit 1
fi
cd "$crate_root"
run check-no-default cargo +1.94.0 check --lib --no-default-features
run check-sync-discovery cargo +1.94.0 check --lib --no-default-features --features sync,discovery --locked
run check-async-discovery cargo +1.94.0 check --lib --no-default-features --features async,discovery --locked
run check-default cargo +1.94.0 check --lib --locked
phase=metadata
printf 'FIRST_CHECK phase=%s\n' "$phase"
cargo +1.94.0 metadata --format-version 1 --locked >"$log_dir/metadata.log" 2>"$log_dir/metadata.stderr" || {
  cat "$log_dir/metadata.stderr" >&2
  exit 1
}
python3 "$repo_root/ci/package_candidate.py" check-metadata "$crate_name" < "$log_dir/metadata.log" > "$log_dir/metadata-validation.log" 2>&1 || { cat "$log_dir/metadata-validation.log" >&2; exit 1; }
