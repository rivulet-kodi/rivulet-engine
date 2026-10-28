#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT

# Dead-code gate: fails when golang.org/x/tools/cmd/deadcode (pinned in
# tools/go.mod) finds a function that no main package reaches.
#
# It runs without -test: with -test every test function is a root, so a
# production function that only tests call would count as live. Without it such
# functions are reported, which is the point: test-only helpers belong in
# _test.go files.
#
# Exclusions (the expected result is zero findings after them):
#   - cmd/librivulet-engine: the cgo shared library. Its //export entry points
#     (ServerStart, ServerStop, ServerVersion and the helpers behind them) are
#     called from C, never from a Go main, so deadcode reports the whole
#     package.
#   - scripts/deadcode-allow.txt: "<file> <symbol> # reason" lines for code that
#     only the library mode reaches. An entry that matches no finding is itself
#     an error, so the list cannot go stale.
#
# Usage: scripts/deadcode-check.sh

set -euo pipefail

cd "$(dirname "$0")/.."

if [ "$#" -ne 0 ]; then
	echo "usage: $0" >&2
	exit 2
fi

export LC_ALL=C

allow=scripts/deadcode-allow.txt
tmp=$(mktemp -d "${TMPDIR:-/tmp}/deadcode.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

go tool -modfile=tools/go.mod deadcode ./... >"$tmp/raw"

# Reduce each report to "<file> <symbol>", dropping the cgo library package.
awk '
	/^[[:space:]]*$/ { next }
	match($0, /^[^:]+:[0-9]+:[0-9]+: unreachable func: /) {
		loc = substr($0, 1, RLENGTH)
		sym = substr($0, RLENGTH + 1)
		sub(/:[0-9]+:[0-9]+: unreachable func: $/, "", loc)
		if (loc ~ /^cmd\/librivulet-engine\//) next
		print loc " " sym
		next
	}
	{ print "deadcode-check: unrecognised deadcode output line: " $0 > "/dev/stderr"; bad = 1 }
	END { exit bad }
' "$tmp/raw" | sort -u >"$tmp/found"

# Allowlist entries without reason text or comments.
sed -E -e '/^[[:space:]]*(#|$)/d' -e 's/[[:space:]]*#.*$//' "$allow" | sort -u >"$tmp/allowed"

comm -23 "$tmp/found" "$tmp/allowed" >"$tmp/findings"
comm -13 "$tmp/found" "$tmp/allowed" >"$tmp/stale"

status=0
if [ -s "$tmp/findings" ]; then
	cat <<EOF
deadcode-check: unreachable code (no main package reaches it):

$(sed 's/^/  /' "$tmp/findings")

Delete it, or move test-only code into a _test.go file. Code only the cgo
library reaches goes in $allow with a reason.
EOF
	status=1
fi
if [ -s "$tmp/stale" ]; then
	cat <<EOF
deadcode-check: $allow entries that match no finding (delete them):

$(sed 's/^/  /' "$tmp/stale")
EOF
	status=1
fi
[ "$status" -eq 0 ] || exit 1

echo "deadcode-check: ok"
