#!/usr/bin/env python3
"""Reject local Cargo dependencies and patches in a registry consumer manifest."""

import sys
import tomllib
from pathlib import Path


def manifest_issues(manifest: dict) -> list[str]:
    issues = []
    if "patch" in manifest:
        issues.append("[patch.*] tables are not allowed")

    def check_dependency_tables(value, location):
        if not isinstance(value, dict):
            return
        if "path" in value:
            issues.append(f"{location} uses a path dependency")
        for name, nested in value.items():
            if isinstance(nested, dict):
                check_dependency_tables(nested, f"{location}.{name}")

    for section in ("dependencies", "dev-dependencies", "build-dependencies", "workspace"):
        if section in manifest:
            check_dependency_tables(manifest[section], section)
    if "target" in manifest:
        check_dependency_tables(manifest["target"], "target")
    return issues


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {Path(sys.argv[0]).name} CARGO_TOML", file=sys.stderr)
        return 2

    manifest_path = Path(sys.argv[1])
    with manifest_path.open("rb") as source:
        manifest = tomllib.load(source)
    issues = manifest_issues(manifest)
    if issues:
        for issue in issues:
            print(f"error: {manifest_path}: {issue}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
