#!/usr/bin/env python3
"""Prepare and verify an isolated consumer for an explicit registry candidate."""

import argparse
import json
import re
import shutil
import sys
import tomllib
from pathlib import Path

REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
PACKAGES = {
    "provider": "qubit-event-bus-redis",
    "core": "qubit-event-bus",
    "spi": "qubit-spi",
}


class TargetError(Exception):
    pass


def load_manifest(path: Path) -> dict:
    # Cargo accepts multiline inline tables although TOML 1.0's tomllib does not.
    text = path.read_text()
    text = re.sub(r"\{([^{}]*)\}", lambda match: "{" + " ".join(match.group(1).split()) + "}", text, flags=re.S)
    return tomllib.loads(text)


def parse_version(value: str, label: str) -> str:
    if not re.fullmatch(r"[0-9]+[.][0-9]+[.][0-9]+", value):
        raise TargetError(f"{label} version must be an exact stable X.Y.Z version")
    return value


def arguments_versions(args) -> dict[str, str]:
    return {key: parse_version(getattr(args, f"{key}_version"), key) for key in PACKAGES}


def core_minor(manifest: dict) -> tuple[int, int]:
    dependencies = manifest.get("dependencies", {})
    value = dependencies.get("qubit-event-bus")
    requirement = value.get("version") if isinstance(value, dict) else value
    if not isinstance(requirement, str) or not re.fullmatch(r"\^?0[.]\d+(?:[.]\d+)?", requirement):
        raise TargetError("dependencies.qubit-event-bus.version: unsupported requirement; expected a single 0.x minor")
    parts = requirement.removeprefix("^").split(".")
    return int(parts[0]), int(parts[1])


def check_source_versions(source_manifest: Path, versions: dict[str, str]) -> None:
    try:
        source = load_manifest(source_manifest)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise TargetError(f"cannot read source manifest: {error}") from None
    package_version = source.get("package", {}).get("version")
    if versions["provider"] != package_version:
        raise TargetError(f"provider candidate {versions['provider']} does not equal source package version {package_version}")
    required = core_minor(source)
    candidate = tuple(int(part) for part in versions["core"].split(".")[:2])
    if candidate != required:
        raise TargetError(f"core candidate {versions['core']} does not match source core requirement {required[0]}.{required[1]}")


def prepare(args) -> None:
    versions = arguments_versions(args)
    mode = getattr(args, "mode", "source-candidate")
    if mode not in {"source-candidate", "published"}:
        raise TargetError(f"unknown registry consumer mode: {mode}")
    if mode == "source-candidate":
        check_source_versions(Path(args.source_manifest), versions)
    fixture = Path(args.fixture)
    output = Path(args.output)
    if not (fixture / "Cargo.toml").is_file():
        raise TargetError("fixture must contain Cargo.toml")
    output.mkdir(parents=True, exist_ok=True)
    shutil.copytree(fixture, output, dirs_exist_ok=True)
    manifest_path = output / "Cargo.toml"
    manifest = load_manifest(manifest_path)
    dependencies = manifest.get("dependencies", {})
    for role, package in PACKAGES.items():
        if package not in dependencies:
            raise TargetError(f"fixture dependencies.{package}: missing direct dependency")
        dependency = dependencies[package]
        if isinstance(dependency, dict):
            dependency.pop("path", None)
            dependency.pop("git", None)
            dependency["version"] = f"={versions[role]}"
        elif not isinstance(dependency, str):
            raise TargetError(f"fixture dependencies.{package}: expected a dependency string or table")
    # Only these version fields are rewritten; preserve the fixture's features and source.
    lines = manifest_path.read_text().splitlines()
    replacements = {package: f'version = "={versions[role]}"' for role, package in PACKAGES.items()}
    in_dependencies = False
    rewritten = []
    for line in lines:
        if line.startswith("["):
            in_dependencies = line.strip() == "[dependencies]"
        if in_dependencies:
            key_match = re.match(r"\s*([\w-]+)\s*=", line)
            if key_match and key_match.group(1) in replacements:
                package = key_match.group(1)
                if "{" in line:
                    line = re.sub(r'version\s*=\s*"[^"]+"', replacements[package], line, count=1)
                    line = re.sub(r',\s*path\s*=\s*"[^"]+"', "", line)
                    line = re.sub(r'path\s*=\s*"[^"]+"\s*,\s*', "", line)
                else:
                    line = f'{package} = "={versions[next(role for role, name in PACKAGES.items() if name == package)]}"'
        rewritten.append(line)
    manifest_path.write_text("\n".join(rewritten) + "\n")


def verify(args) -> None:
    versions = arguments_versions(args)
    metadata = json.load(sys.stdin)
    packages = metadata.get("packages", [])
    by_id = {package.get("id"): package for package in packages}
    root_id = metadata.get("resolve", {}).get("root")
    root = by_id.get(root_id)
    if root is None:
        raise TargetError("metadata root package is missing")
    resolve_nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}
    root_node = resolve_nodes.get(root_id, {})
    direct_ids = {dependency.get("name"): dependency.get("pkg") for dependency in root_node.get("deps", [])}
    selected: dict[str, dict] = {}
    for role, name in PACKAGES.items():
        package_id = direct_ids.get(name)
        package = by_id.get(package_id)
        if package is None:
            raise TargetError(f"{name}: direct dependency did not resolve to a package")
        if package.get("version") != versions[role]:
            raise TargetError(f"{name}: resolved version {package.get('version')}; expected {versions[role]}")
        if package.get("source") != REGISTRY:
            category = "path" if package.get("source") is None else "non-registry"
            raise TargetError(f"{name} {package.get('version')}: expected registry source, got {category}")
        selected[name] = package

    core_id = selected[PACKAGES["core"]]["id"]
    provider_id = selected[PACKAGES["provider"]]["id"]
    provider_node = resolve_nodes.get(provider_id, {})
    provider_core_ids = {dependency.get("pkg") for dependency in provider_node.get("deps", [])
                         if dependency.get("name") == PACKAGES["core"]}
    if provider_core_ids != {core_id}:
        versions_found = sorted(by_id[package_id].get("version", "?") for package_id in provider_core_ids if package_id in by_id)
        raise TargetError(f"{PACKAGES['provider']} {versions['provider']}: core dependency edge resolves to {versions_found}; expected {versions['core']}")
    core_ids = {package.get("id") for package in packages if package.get("name") == PACKAGES["core"]}
    if core_ids != {core_id}:
        versions_found = sorted(package.get("version", "?") for package in packages if package.get("name") == PACKAGES["core"])
        raise TargetError(f"multiple core package identities in metadata: {versions_found}")
    print("verified registry combination: " + ", ".join(f"{role}={versions[role]}" for role in PACKAGES))


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("--source-manifest", required=True)
    prep.add_argument("--fixture", required=True)
    prep.add_argument("--output", required=True)
    prep.add_argument("--mode", choices=("source-candidate", "published"), default="source-candidate")
    verify_parser = commands.add_parser("verify")
    for command in (prep, verify_parser):
        for role in PACKAGES:
            command.add_argument(f"--{role}-version", required=True)
    args = parser.parse_args()
    try:
        prepare(args) if args.command == "prepare" else verify(args)
    except (TargetError, OSError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
