#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/release_pypi.sh [options]

Build and optionally upload dandelion-orchestrator to PyPI.

Options:
  --pypi        Upload to production PyPI after building.
  --testpypi    Upload to TestPyPI after building.
  --skip-check  Skip `make check`.
  --skip-git    Skip git clean-state check, push, and tag push.
  --no-clean    Do not remove dist/ before building.
  --gh-release  Create a GitHub Release from CHANGELOG.md after tagging.
  -h, --help    Show this help.

Examples:
  scripts/release_pypi.sh
  scripts/release_pypi.sh --testpypi
  scripts/release_pypi.sh --pypi --gh-release

Notes:
  - Install release and check tools first:
      python3 -m pip install --upgrade build twine
      python3 -m pip install -e ".[dev]"
  - For upload, Twine expects credentials from ~/.pypirc, keyring, or prompt.
    PyPI token upload username is __token__.
  - --gh-release requires the GitHub CLI (gh).
EOF
}

require_python_module() {
  local module="$1"
  local install_name="$2"
  local install_hint="$3"

  if ! python3 -c "import ${module}" >/dev/null 2>&1; then
    echo "Missing Python package: ${install_name}" >&2
    echo "Install it with:" >&2
    echo "  ${install_hint}" >&2
    exit 1
  fi
}

upload_target=""
run_check=1
run_git=1
clean_dist=1
gh_release=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pypi)
      upload_target="pypi"
      ;;
    --testpypi)
      upload_target="testpypi"
      ;;
    --skip-check)
      run_check=0
      ;;
    --skip-git)
      run_git=0
      ;;
    --no-clean)
      clean_dist=0
      ;;
    --gh-release)
      gh_release=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

cd "$(dirname "$0")/.."

package_name="$(python3 - <<'PY'
import tomllib
from pathlib import Path

data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
print(data["project"]["name"])
PY
)"

version="$(python3 - <<'PY'
import tomllib
from pathlib import Path

data = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
print(data["project"]["version"])
PY
)"

tag="v${version}"

echo "Package: ${package_name}"
echo "Version: ${version}"
echo "Tag: ${tag}"

if [[ "${run_check}" -eq 1 ]]; then
  require_python_module "ruff" "ruff" 'python3 -m pip install -e ".[dev]"'
  require_python_module "pyright" "pyright" 'python3 -m pip install -e ".[dev]"'
  echo "Running checks..."
  make check
fi

if [[ "${run_git}" -eq 1 ]]; then
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "Git working tree is not clean. Commit or stash changes before release." >&2
    git status --short >&2
    exit 1
  fi

  echo "Pushing main..."
  git push origin main

  if git rev-parse "${tag}" >/dev/null 2>&1; then
    echo "Tag ${tag} already exists locally."
  else
    echo "Creating tag ${tag}..."
    git tag "${tag}"
  fi

  echo "Pushing tag ${tag}..."
  git push origin "${tag}"
fi

require_python_module "build.__main__" "build" "python3 -m pip install --upgrade build twine"
require_python_module "twine" "twine" "python3 -m pip install --upgrade build twine"

if [[ "${clean_dist}" -eq 1 ]]; then
  echo "Cleaning build artifacts..."
  rm -rf build dist
fi

echo "Building package..."
python3 -m build

echo "Checking distribution..."
python3 -m twine check dist/*

case "${upload_target}" in
  "")
    echo "Build complete. Upload skipped."
    echo "To upload to TestPyPI: scripts/release_pypi.sh --testpypi"
    echo "To upload to PyPI:     scripts/release_pypi.sh --pypi"
    ;;
  testpypi)
    echo "Uploading to TestPyPI..."
    python3 -m twine upload --repository testpypi dist/*
    ;;
  pypi)
    echo "Uploading to PyPI..."
    python3 -m twine upload dist/*
    ;;
esac

if [[ "${gh_release}" -eq 1 ]]; then
  if ! command -v gh >/dev/null 2>&1; then
    echo "GitHub CLI (gh) not found. Skipping GitHub Release." >&2
  else
    notes_file="$(mktemp)"
    trap 'rm -f "${notes_file}"' EXIT
    if [[ -f CHANGELOG.md ]]; then
      python3 - "${version}" "${notes_file}" <<'PY'
import re, sys
version = sys.argv[1]
out_path = sys.argv[2]
text = open("CHANGELOG.md", encoding="utf-8").read()
pattern = rf"## \[{re.escape(version)}\].*?\n(.*?)(?=\n## \[|\Z)"
match = re.search(pattern, text, re.DOTALL)
with open(out_path, "w", encoding="utf-8") as f:
    f.write(match.group(1).strip() if match else "")
PY
    fi
    echo "Creating GitHub Release ${tag}..."
    if [[ -s "${notes_file}" ]]; then
      gh release create "${tag}" dist/* --title "${tag}" --notes-file "${notes_file}"
    else
      gh release create "${tag}" dist/* --title "${tag}" --generate-notes
    fi
  fi
fi
