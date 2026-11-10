#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
python3 "$project_root/ci/check-doc-versions.py" "$project_root"
python3 "$project_root/ci/tests/test_doc_versions.py"
python3 "$project_root/ci/tests/test_package_candidate.py"
python3 "$project_root/ci/tests/test_registry_consumer_manifest.py"
python3 "$project_root/ci/tests/test_registry_consumer_target.py"
python3 "$project_root/ci/tests/test_registry_consumer_execution.py"
bash "$project_root/ci/tests/prepare-event-bus-preflight.sh"
