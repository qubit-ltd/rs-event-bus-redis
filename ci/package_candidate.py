#!/usr/bin/env python3
"""Validate the normalized manifest and resolution of a packaged crate."""
import json
import sys
import tomllib
from pathlib import Path

REGISTRY_PREFIX = "registry+"


class CandidateError(Exception):
    pass


def load_manifest(path: Path) -> dict:
    with path.open("rb") as stream:
        return tomllib.load(stream)


def manifest_issues(manifest: dict) -> list[str]:
    issues = []
    if "patch" in manifest:
        issues.append("[patch.*] tables are not allowed")
    def check_table(value, where):
        if not isinstance(value, dict):
            return
        if "path" in value:
            issues.append(f"{where} uses a path dependency")
        if "git" in value:
            issues.append(f"{where} uses a git dependency")

    for section in ("dependencies", "build-dependencies"):
        for name, value in manifest.get(section, {}).items():
            check_table(value, f"{section}.{name}")
    for target, tables in manifest.get("target", {}).items():
        for section in ("dependencies", "build-dependencies"):
            for name, value in tables.get(section, {}).items():
                check_table(value, f"target.{target}.{section}.{name}")
    # Workspace members and development dependencies do not contribute to package --lib.
    return issues


def validate_metadata(metadata: dict, root_name: str) -> None:
    packages = metadata.get("packages", [])
    roots = [p for p in packages if p.get("name") == root_name and p.get("source") is None]
    if len(roots) != 1:
        raise CandidateError(f"expected one local candidate root {root_name}; found {len(roots)}")
    root = roots[0]
    nodes = {node.get("id"): node for node in metadata.get("resolve", {}).get("nodes", [])}
    root_node = nodes.get(root.get("id"))
    if root_node is None:
        raise CandidateError("candidate root is missing from Cargo resolve graph")
    by_id = {p.get("id"): p for p in packages}
    reachable = set()
    def production_edge(edge):
        kinds = edge.get("dep_kinds", [])
        return not kinds or any(kind.get("kind") in (None, "normal", "build") for kind in kinds)

    pending = [dep.get("pkg") for dep in root_node.get("deps", []) if production_edge(dep)]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        package = by_id.get(package_id)
        if package is None:
            raise CandidateError(f"resolved dependency {package_id} is missing from metadata")
        source = package.get("source")
        if not isinstance(source, str) or not source.startswith(REGISTRY_PREFIX):
            raise CandidateError(f"production dependency {package.get('name')} {package.get('version')} has non-registry source {source}")
        node = nodes.get(package_id, {})
        pending.extend(dep.get("pkg") for dep in node.get("deps", []) if production_edge(dep))
    core_ids = {p.get("id") for p in packages if p.get("name") == "qubit-event-bus" and p.get("id") in reachable}
    if len(core_ids) != 1:
        raise CandidateError(f"expected one reachable qubit-event-bus identity; found {len(core_ids)}")


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "check-manifest":
        try:
            issues = manifest_issues(load_manifest(Path(sys.argv[2])))
        except (OSError, tomllib.TOMLDecodeError) as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        for issue in issues:
            print(f"error: {issue}", file=sys.stderr)
        return int(bool(issues))
    if len(sys.argv) == 3 and sys.argv[1] == "check-metadata":
        try:
            validate_metadata(json.load(sys.stdin), sys.argv[2])
        except (CandidateError, json.JSONDecodeError) as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        print(f"verified production dependencies for {sys.argv[2]}")
        return 0
    print(f"usage: {Path(sys.argv[0]).name} check-manifest CARGO_TOML | check-metadata ROOT_NAME", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
