#!/usr/bin/env bash
# Image-build check driver (ADR-007 follow-up 18, D24; W3-13). Builds the repo's Python app image for each
# package (default: every packages/tributary-*), then runs image_env_check.py INSIDE it with the registries
# mounted read-only. A negative control then puts a second installation of an installed distribution
# first on PYTHONPATH and requires the same check to fail. Only images tagged tributary-imgcheck/* are
# created, and those are removed on exit; nothing else in Docker is touched.
#
# TB_BUILD_CA_BUNDLE=/path/ca.crt passes an extra CA to the build (re-terminating egress proxies only).
#
#   .github/scripts/image_check.sh [package ...]
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
packages=("$@")
if [ ${#packages[@]} -eq 0 ]; then
  for d in packages/tributary-*/; do packages+=("$(basename "$d")"); done
fi

fixture="$(mktemp -d)"
built=()
trap 'rm -rf "$fixture"; for i in "${built[@]}"; do docker rmi "$i" >/dev/null 2>&1 || true; done' EXIT
# A second installation of the package under test itself (it is always in its own image).
make_fixture() { # package
  rm -rf "${fixture:?}"/*
  local folder="${1//-/_}-0.0.dist-info"
  mkdir -p "$fixture/$folder"
  printf 'Metadata-Version: 2.1\nName: %s\nVersion: 0.0\n' "$1" > "$fixture/$folder/METADATA"
  chmod -R a+rX "$fixture"
}

run_check() { # image [extra docker args...]
  local image="$1"; shift
  docker run --rm --read-only --network none --cap-drop ALL --security-opt no-new-privileges -v "$root/contracts:/c:ro" -v "$root/.github/scripts:/s:ro" "$@" \
    --entrypoint python "$image" /s/image_env_check.py --contracts /c
}

status=0
for pkg in "${packages[@]}"; do
  image="tributary-imgcheck/$pkg"
  echo "== $pkg: build"
  built+=("$image")
  docker build -q ${TB_BUILD_CA_BUNDLE:+--secret "id=ca,src=$TB_BUILD_CA_BUNDLE"} -f deploy/compose/images/python-app.Dockerfile --build-arg "TB_PACKAGE=$pkg" -t "$image" . >/dev/null
  echo "== $pkg: check"
  run_check "$image" || { echo "FAIL: $pkg image environment"; status=1; }
  echo "== $pkg: negative control (must fail)"
  make_fixture "$pkg"
  if out="$(run_check "$image" -v "$fixture:/dup:ro" -e PYTHONPATH=/dup 2>&1)"; then
    echo "FAIL: negative control passed for $pkg, the check is not detecting duplicates"; status=1
  elif ! grep -q "FAIL: duplicate distribution '$pkg' installed 2 times" <<<"$out"; then
    echo "FAIL: negative control failed for the wrong reason in $pkg:"; echo "$out"; status=1
  else
    echo "negative control failed as required: duplicate distribution '$pkg'"
  fi
done
exit "$status"
