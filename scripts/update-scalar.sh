#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT
#
# Vendors the standalone bundle of @scalar/api-reference into
# internal/api/apiref/ (embedded in the server binary, served at
# /api/reference). Usage: scripts/update-scalar.sh <exact-version>
# Then bump scalarBundleName in internal/api/openapi.go and rerun the tests.
set -euo pipefail

version="${1:?usage: update-scalar.sh <exact-version>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

(cd "$tmp" && npm pack "@scalar/api-reference@${version}" --silent >/dev/null && tar xzf ./*.tgz)

out="$root/internal/api/apiref"
rm -f "$out"/scalar-*.js.gz
# The source map is not shipped: drop the pointer so browsers do not request it.
grep -v '^//# sourceMappingURL' "$tmp/package/dist/browser/standalone.js" | gzip -9n >"$out/scalar-${version}.js.gz"
echo "vendored $out/scalar-${version}.js.gz"
