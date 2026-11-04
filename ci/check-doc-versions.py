#!/usr/bin/env python3
"""Check marked bilingual dependency examples against the current Cargo manifest."""

import re
import sys
import tomllib
from pathlib import Path

MARKER = "<!-- current-source-dependencies -->"
DOCUMENT_PAIRS = (
    ("README.md", "README.zh_CN.md"),
    ("doc/user_guide.md", "doc/user_guide.zh_CN.md"),
)
DEPENDENCIES = ("qubit-event-bus-redis", "qubit-event-bus", "qubit-spi")


class DocumentationError(Exception):
    pass


def extract_dependencies(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    if text.count(MARKER) != 1:
        raise DocumentationError(f"{path}: expected exactly one {MARKER} marker")
    marked = text.split(MARKER, 1)[1]
    match = re.search(r"\s*```toml\s*\n(.*?)\n```", marked, re.DOTALL)
    if match is None:
        raise DocumentationError(f"{path}: marker must precede a TOML code block")
    try:
        block = tomllib.loads(match.group(1))
    except tomllib.TOMLDecodeError as error:
        raise DocumentationError(f"{path}: invalid marked TOML: {error}") from error
    dependencies = block.get("dependencies")
    if not isinstance(dependencies, dict):
        raise DocumentationError(f"{path}: marked TOML must contain [dependencies]")
    result = {}
    for name in DEPENDENCIES:
        entry = dependencies.get(name)
        version = entry.get("version") if isinstance(entry, dict) else entry
        if not isinstance(version, str):
            raise DocumentationError(f"{path}: missing version for {name}")
        result[name] = version
    return result


def manifest_dependencies(manifest_path: Path) -> dict[str, str]:
    text = manifest_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    normalized = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if re.match(r"^\s*[A-Za-z0-9_-]+\s*=\s*\{\s*$", line):
            block = [line.strip()]
            index += 1
            while index < len(lines) and not re.match(r"^\s*\}\s*(?:#.*)?$", lines[index]):
                block.append(lines[index].strip())
                index += 1
            if index == len(lines):
                raise DocumentationError(f"{manifest_path}: unterminated multiline inline table")
            if block[-1].endswith(","):
                block[-1] = block[-1][:-1]
            block.append("}")
            normalized.append(" ".join(block))
        else:
            normalized.append(line)
        index += 1
    manifest = tomllib.loads("\n".join(normalized))
    result = {"qubit-event-bus-redis": manifest.get("package", {}).get("version")}
    dependencies = manifest.get("dependencies", {})
    for name in DEPENDENCIES[1:]:
        entry = dependencies.get(name)
        result[name] = entry.get("version") if isinstance(entry, dict) else entry
    missing = [name for name, version in result.items() if not isinstance(version, str)]
    if missing:
        raise DocumentationError(f"Cargo.toml: missing versions for {', '.join(missing)}")
    return result


def check(root: Path) -> list[str]:
    expected = manifest_dependencies(root / "Cargo.toml")
    issues = []
    for english_name, chinese_name in DOCUMENT_PAIRS:
        english_path = root / english_name
        chinese_path = root / chinese_name
        try:
            english = extract_dependencies(english_path)
            chinese = extract_dependencies(chinese_path)
        except (OSError, DocumentationError) as error:
            issues.append(str(error))
            continue
        if english != chinese:
            issues.append(f"{english_name} and {chinese_name}: marked dependency versions differ")
        for path, dependencies in ((english_path, english), (chinese_path, chinese)):
            for name, version in expected.items():
                if dependencies.get(name) != version:
                    issues.append(
                        f"{path}: {name} version {dependencies.get(name)!r} does not match Cargo.toml {version!r}"
                    )
    return issues


def main() -> int:
    root = Path(sys.argv[1]).resolve() if len(sys.argv) == 2 else Path(__file__).resolve().parents[1]
    if len(sys.argv) > 2:
        print(f"usage: {Path(sys.argv[0]).name} [PROJECT_ROOT]", file=sys.stderr)
        return 2
    try:
        issues = check(root)
    except (OSError, tomllib.TOMLDecodeError, DocumentationError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    for issue in issues:
        print(f"error: {issue}", file=sys.stderr)
    if issues:
        return 1
    print("verified current-source dependency versions in bilingual documentation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
