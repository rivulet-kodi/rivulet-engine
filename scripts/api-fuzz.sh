#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT
#
# Fuzz the HTTP API with Schemathesis against internal/api/openapi.json.
#
# By default the script builds the container image of the working tree, runs
# the engine on an --internal container network (no egress: every route that
# fetches a URL fails fast and the same way on every machine) with trackers,
# metadata lookups and IMDb disabled, and runs the pinned Schemathesis image on
# that network against http://rivulet:11470. Teardown (containers + network) is
# a trap, so an interrupted run leaves nothing behind.
#
# Usage:
#   scripts/api-fuzz.sh [pr|nightly]     # default: pr
#
# One pass with the built-in checks not_a_server_error,
# response_schema_conformance and status_code_conformance, plus the project
# check foreign_origin_rejected (tests/apifuzz/hooks.py): a request carrying a
# foreign Origin must be refused with 403 on every operation.
#
# Operations that must not be fuzzed are disabled in tests/apifuzz/schemathesis.toml
# (each with its reason); findings deferred from triage are listed in the same
# file under "Known issues".
#
# Budgets: pr = small, fixed seed (deterministic); nightly = bigger, random seed
# printed (reproduce with RIVULET_FUZZ_SEED=<seed>).
#
# Environment:
#   RIVULET_FUZZ_SEED         override the seed
#   RIVULET_FUZZ_EXAMPLES     override --max-examples per operation
#   RIVULET_FUZZ_REPORT_DIR   reports/cassettes/engine log (default: ./schemathesis-report)
#   RIVULET_FUZZ_DOCKER       container CLI (default: docker, else podman)
#   RIVULET_FUZZ_IMAGE        engine image to run instead of building one from
#                             the working tree (default: build rivulet-engine:fuzz)
#   RIVULET_FUZZ_URL          fuzz an already running server instead of starting
#                             one (no container, no network). It must run with
#                             the default Origin allowlist.
#   SCHEMATHESIS              command line of the Schemathesis CLI. Default: the
#                             pinned container image. For a quick local run
#                             against RIVULET_FUZZ_URL (required in this mode):
#                             SCHEMATHESIS='uvx schemathesis@4.32.5'
set -euo pipefail

mode="${1:-pr}"
root="$(cd "$(dirname "$0")/.." && pwd)"
report_dir="$(realpath -m "${RIVULET_FUZZ_REPORT_DIR:-schemathesis-report}")"

# Keep the tag and digest in sync: the digest is the multi-arch index of the tag.
IMAGE='docker.io/schemathesis/schemathesis:4.32.5@sha256:e414b3466a95d03fc6d9de1b7842e1d4738775493750166035b943c0bdcce248'

net=rivulet-fuzz
engine=rivulet-fuzz-engine
engine_port=11470

case "$mode" in
pr)
	examples=20
	seed="${RIVULET_FUZZ_SEED:-20261028}"
	;;
nightly)
	examples=100
	seed="${RIVULET_FUZZ_SEED:-$(od -An -N4 -tu4 /dev/urandom | tr -d ' ')}"
	;;
*)
	echo "usage: $0 [pr|nightly]" >&2
	exit 2
	;;
esac
examples="${RIVULET_FUZZ_EXAMPLES:-$examples}"

if [ -n "${SCHEMATHESIS:-}" ] && [ -z "${RIVULET_FUZZ_URL:-}" ]; then
	echo "api-fuzz: SCHEMATHESIS (local CLI) needs RIVULET_FUZZ_URL: the engine container's network is not reachable from the host" >&2
	exit 2
fi

if [ -n "${RIVULET_FUZZ_DOCKER:-}" ]; then
	docker_cli="$RIVULET_FUZZ_DOCKER"
elif command -v docker >/dev/null 2>&1; then
	docker_cli=docker
else
	docker_cli=podman
fi

mkdir -p "$report_dir"

# Rootless podman maps the caller to a different in-container uid unless told
# to keep it, and the report dir is mounted writable for the caller's uid.
userns=()
if "$docker_cli" --version 2>/dev/null | grep -qi podman; then
	userns=(--userns=keep-id)
fi

cleanup() {
	if [ -z "${RIVULET_FUZZ_URL:-}" ]; then
		"$docker_cli" logs "$engine" >"$report_dir/engine.log" 2>&1 || true
		"$docker_cli" rm -f "$engine" >/dev/null 2>&1 || true
		"$docker_cli" network rm "$net" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

if [ -n "${RIVULET_FUZZ_URL:-}" ]; then
	url="$RIVULET_FUZZ_URL"
	net_args=(--network host)
else
	url="http://rivulet:$engine_port"
	net_args=(--network "$net")
	engine_image="${RIVULET_FUZZ_IMAGE:-}"
	if [ -z "$engine_image" ]; then
		engine_image=rivulet-engine:fuzz
		"$docker_cli" build -t "$engine_image" "$root"
	fi
	# A leftover from an interrupted run must not break this one.
	"$docker_cli" rm -f "$engine" >/dev/null 2>&1 || true
	"$docker_cli" network rm "$net" >/dev/null 2>&1 || true
	"$docker_cli" network create --internal "$net" >/dev/null
	# RIVULET_CREATE_METADATA_TIMEOUT: the fuzzer asks for torrents that have no
	# peers; the default 90s metadata wait would stall every such request.
	# RIVULET_ALLOWED_HOSTS: the fuzzer reaches the engine by the name "rivulet",
	# which the DNS-rebinding guard would refuse on non-media routes.
	"$docker_cli" run -d --name "$engine" --network "$net" --network-alias rivulet \
		-e RIVULET_ALLOWED_HOSTS=rivulet \
		-e RIVULET_DISABLE_TRACKERS=1 \
		-e RIVULET_TRACKERS_URL=off \
		-e RIVULET_METADATA_URL=off \
		-e RIVULET_LOCAL_IMDB=0 \
		-e RIVULET_EXTRACTORS_URL=off \
		-e RIVULET_CREATE_METADATA_TIMEOUT=2s \
		"$engine_image" >/dev/null
	for _ in $(seq 1 60); do
		if "$docker_cli" exec "$engine" wget -q -O /dev/null "http://127.0.0.1:$engine_port/heartbeat" 2>/dev/null; then
			break
		fi
		sleep 1
	done
	if ! "$docker_cli" exec "$engine" wget -q -O /dev/null "http://127.0.0.1:$engine_port/heartbeat" 2>/dev/null; then
		echo "api-fuzz: the engine did not come up" >&2
		"$docker_cli" logs "$engine" >&2 || true
		exit 1
	fi
fi

echo "api-fuzz: mode=$mode seed=$seed max-examples=$examples url=$url"

# schemathesis ARGS...: run the CLI, in the pinned container unless SCHEMATHESIS
# points at a local one. $spec/$fuzz/$out are the spec, config dir and report
# dir as that CLI sees them.
schemathesis() {
	if [ -n "${SCHEMATHESIS:-}" ]; then
		# The CLI writes a .schemathesis cache next to its cwd: keep it out of the tree.
		# shellcheck disable=SC2086
		(cd "$report_dir" && SCHEMATHESIS_HOOKS="$fuzz/hooks.py" $SCHEMATHESIS "$@")
	else
		# Run as the caller so the mounted report dir is writable (the image user is not).
		"$docker_cli" run --rm "${net_args[@]}" ${userns[@]+"${userns[@]}"} --user "$(id -u):$(id -g)" -w /report \
			-e HOME=/report -e PYTHONDONTWRITEBYTECODE=1 \
			-e SCHEMATHESIS_HOOKS=/fuzz/hooks.py \
			-v "$root/internal/api/openapi.json:/spec/openapi.json:ro" \
			-v "$root/tests/apifuzz:/fuzz:ro" \
			-v "$report_dir:/report" "$IMAGE" "$@"
	fi
}

if [ -n "${SCHEMATHESIS:-}" ]; then
	spec="$root/internal/api/openapi.json" fuzz="$root/tests/apifuzz" out="$report_dir"
else
	spec=/spec/openapi.json fuzz=/fuzz out=/report
fi

status=0

# run NAME CHECKS [extra schemathesis args...]; keeps going after a failing pass
# so one run reports every problem.
run() {
	local name="$1" checks="$2"
	shift 2
	echo "api-fuzz: === pass $name ==="
	# Every request carries an allowed browser Origin (as stremio-web would):
	# POST /settings refuses an Origin-less write from a non-loopback peer, and
	# the fuzzer reaches the engine over the container network.
	schemathesis --config-file "$fuzz/schemathesis.toml" run "$spec" --url "$url" \
		--checks "$checks" --max-examples "$examples" --seed "$seed" \
		--phases examples,coverage,fuzzing --workers 1 --request-timeout 20 \
		-H "Origin: https://web.stremio.com" \
		--report junit,vcr --report-dir "$out/$name" --no-color "$@" || status=1
}

run main not_a_server_error,response_schema_conformance,status_code_conformance,foreign_origin_rejected

exit "$status"
