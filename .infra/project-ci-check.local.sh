#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
python3 "$project_root/ci/check-doc-versions.py" "$project_root"
python3 "$project_root/ci/tests/test_doc_versions.py"
