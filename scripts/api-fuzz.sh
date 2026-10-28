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
# Two passes, each with the built-in checks not_a_server_error,
# response_schema_conformance and status_code_conformance, plus the project
# check foreign_origin_rejected (tests/apifuzz/hooks.py): a request carrying a
# foreign Origin must be refused with 403 on every operation.
#
#   main      a default engine. Archive, NZB, DVR, HLS and local-files routes
#             only see their "nothing there" paths. Config: schemathesis.toml.
#   stateful  a second engine with those features on, next to a fixture server
#             (tests/apifuzz/fixtures.py: archives in every supported format,
#             an NZB with a fake NNTP server, a video, a subtitle file). A setup
#             step creates the sessions and recordings under known keys, and
#             the Schemathesis config feeds those keys back into the requests, so
#             the success paths of /{zip,tar,tgz,7zip,rar}/*, /nzb/*, /record*,
#             /api/recordings*, /hlsv2/*, /local-addon/* and /probe are reached.
#             Config: stateful.toml. Skipped when RIVULET_FUZZ_URL is set (the
#             fixtures need the container network).
#
# Operations that must not be fuzzed are disabled in tests/apifuzz/schemathesis.toml
# (each with its reason); findings deferred from triage are listed in the same
# file under "Known issues". /casting is only reachable on its 404 path: SSDP
# discovery is multicast and there is no renderer on the network.
#
# Budgets: pr = small, fixed seed (deterministic); nightly = bigger, random seed
# printed (reproduce with RIVULET_FUZZ_SEED=<seed>).
#
# Environment:
#   RIVULET_FUZZ_SEED         override the seed
#   RIVULET_FUZZ_EXAMPLES     override --max-examples per operation
#   RIVULET_FUZZ_REQUEST_TIMEOUT  per-request timeout in seconds (default: 40)
#   RIVULET_FUZZ_PASS         main | stateful | all (default: all)
#   RIVULET_FUZZ_REPORT_DIR   reports/cassettes/engine logs/fixtures (default: ./schemathesis-report)
#   RIVULET_FUZZ_DOCKER       container CLI (default: docker, else podman)
#   RIVULET_FUZZ_IMAGE        engine image to run instead of building one from
#                             the working tree (default: build rivulet-engine:fuzz)
#   RIVULET_FUZZ_URL          fuzz an already running server instead of starting
#                             one (no container, no network; main pass only). It
#                             must run with the default Origin allowlist.
#   SCHEMATHESIS              command line of the Schemathesis CLI. Default: the
#                             pinned container image. For a quick local run
#                             against RIVULET_FUZZ_URL (required in this mode):
#                             SCHEMATHESIS='uvx schemathesis@4.32.5'
set -euo pipefail

mode="${1:-pr}"
passes="${RIVULET_FUZZ_PASS:-all}"
root="$(cd "$(dirname "$0")/.." && pwd)"
report_dir="$(realpath -m "${RIVULET_FUZZ_REPORT_DIR:-schemathesis-report}")"

# Keep the tag and digest in sync: the digest is the multi-arch index of the tag.
IMAGE='docker.io/schemathesis/schemathesis:4.32.5@sha256:e414b3466a95d03fc6d9de1b7842e1d4738775493750166035b943c0bdcce248'

net=rivulet-fuzz
engine=rivulet-fuzz-engine
stateful_engine=rivulet-fuzz-stateful
fixtures=rivulet-fuzz-fixtures
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

case "$passes" in
all | main | stateful) ;;
*)
	echo "api-fuzz: RIVULET_FUZZ_PASS must be main, stateful or all" >&2
	exit 2
	;;
esac
run_main=0
run_stateful=0
[ "$passes" != stateful ] && run_main=1
[ "$passes" != main ] && run_stateful=1

if [ -n "${SCHEMATHESIS:-}" ] && [ -z "${RIVULET_FUZZ_URL:-}" ]; then
	echo "api-fuzz: SCHEMATHESIS (local CLI) needs RIVULET_FUZZ_URL: the engine container's network is not reachable from the host" >&2
	exit 2
fi
if [ -n "${RIVULET_FUZZ_URL:-}" ] && [ "$run_stateful" = 1 ]; then
	if [ "$passes" = stateful ]; then
		echo "api-fuzz: the stateful pass needs the container network; it cannot run against RIVULET_FUZZ_URL" >&2
		exit 2
	fi
	echo "api-fuzz: RIVULET_FUZZ_URL is set: running the main pass only" >&2
	run_stateful=0
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
		for c in "$engine" "$stateful_engine" "$fixtures"; do
			"$docker_cli" logs "$c" >"$report_dir/$c.log" 2>&1 || true
			"$docker_cli" rm -f "$c" >/dev/null 2>&1 || true
		done
		"$docker_cli" network rm "$net" >/dev/null 2>&1 || true
	fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# wait_up CONTAINER: block until the engine in CONTAINER answers /heartbeat.
wait_up() {
	local c="$1"
	for _ in $(seq 1 60); do
		if "$docker_cli" exec "$c" wget -q -O /dev/null "http://127.0.0.1:$engine_port/heartbeat" 2>/dev/null; then
			return 0
		fi
		sleep 1
	done
	echo "api-fuzz: the engine $c did not come up" >&2
	"$docker_cli" logs "$c" >&2 || true
	exit 1
}

# Settings every fuzz engine shares.
# RIVULET_CREATE_METADATA_TIMEOUT: the fuzzer asks for torrents that have no
# peers; the default 90s metadata wait would stall every such request.
# RIVULET_ALLOWED_HOSTS (per engine): the fuzzer reaches the engine by an
# alias, which the DNS-rebinding guard would refuse on non-media routes.
engine_env=(
	-e RIVULET_DISABLE_TRACKERS=1
	-e RIVULET_TRACKERS_URL=off
	-e RIVULET_METADATA_URL=off
	-e RIVULET_LOCAL_IMDB=0
	-e RIVULET_EXTRACTORS_URL=off
	-e RIVULET_CREATE_METADATA_TIMEOUT=2s
)

url="${RIVULET_FUZZ_URL:-}"
stateful_url="http://rivulet-stateful:$engine_port"
if [ -n "${RIVULET_FUZZ_URL:-}" ]; then
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
	for c in "$engine" "$stateful_engine" "$fixtures"; do
		"$docker_cli" rm -f "$c" >/dev/null 2>&1 || true
	done
	"$docker_cli" network rm "$net" >/dev/null 2>&1 || true
	"$docker_cli" network create --internal "$net" >/dev/null
	if [ "$run_main" = 1 ]; then
		"$docker_cli" run -d --name "$engine" --network "$net" --network-alias rivulet \
			-e RIVULET_ALLOWED_HOSTS=rivulet "${engine_env[@]}" \
			"$engine_image" >/dev/null
		wait_up "$engine"
	fi
fi

echo "api-fuzz: mode=$mode seed=$seed max-examples=$examples passes=$passes url=$url"

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

# Above the server's own longest bound (yt-dlp and ffprobe: 30 s), so a slow
# no-egress failure surfaces as the server's 502/500 answer, not a client
# timeout. On CI runners DNS on the internal network times out instead of
# failing fast, which makes /yt take the full 30 s.
request_timeout="${RIVULET_FUZZ_REQUEST_TIMEOUT:-40}"

# run NAME CONFIG URL CHECKS [extra schemathesis args...]; keeps going after a
# failing pass so one run reports every problem.
run() {
	local name="$1" config="$2" target="$3" checks="$4"
	shift 4
	echo "api-fuzz: === pass $name ==="
	# Every request carries an allowed browser Origin (as stremio-web would):
	# POST /settings refuses an Origin-less write from a non-loopback peer, and
	# the fuzzer reaches the engine over the container network.
	schemathesis --config-file "$fuzz/$config" run "$spec" --url "$target" \
		--checks "$checks" --max-examples "$examples" --seed "$seed" \
		--phases examples,coverage,fuzzing --workers 1 --request-timeout "$request_timeout" \
		-H "Origin: https://web.stremio.com" \
		--report junit,vcr --report-dir "$out/$name" --no-color "$@" || status=1
}

# fixtures_python ARGS...: run tests/apifuzz/fixtures.py in the Schemathesis
# image (it is the only Python on hand), as the caller, on the fuzz network.
fixtures_python() {
	"$docker_cli" run --rm "${net_args[@]}" ${userns[@]+"${userns[@]}"} --user "$(id -u):$(id -g)" \
		-e PYTHONDONTWRITEBYTECODE=1 \
		-v "$root/tests/apifuzz:/fuzz:ro" -v "$report_dir:/report" \
		--entrypoint python "$IMAGE" /fuzz/fixtures.py "$@"
}

# start_stateful: build the fixtures, start the fixture server and an engine with
# the file/NZB/DVR/HLS features on, and create the sessions the pass addresses.
start_stateful() {
	rm -rf "$report_dir/fixtures" "$report_dir/stateful"
	mkdir -p "$report_dir/fixtures"
	# The clip comes from the engine image's own ffmpeg (nothing to download).
	# Video plus audio, mp4 for archives/NZB/HLS and mpegts for the recorder.
	"$docker_cli" run --rm ${userns[@]+"${userns[@]}"} --user "$(id -u):$(id -g)" \
		-v "$report_dir/fixtures:/out" --entrypoint sh "$engine_image" -c '
		ffmpeg -hide_banner -loglevel error -y \
			-f lavfi -i testsrc=size=96x64:rate=10:duration=2 -f lavfi -i sine=frequency=440:duration=2 \
			-c:v mpeg4 -q:v 5 -pix_fmt yuv420p -c:a aac -b:a 32k -movflags +faststart /out/clip.mp4 &&
		ffmpeg -hide_banner -loglevel error -y -i /out/clip.mp4 -c copy -f mpegts /out/clip.ts'
	fixtures_python build /report/fixtures

	"$docker_cli" run -d --name "$fixtures" --network "$net" --network-alias fixtures \
		${userns[@]+"${userns[@]}"} --user "$(id -u):$(id -g)" \
		-e PYTHONDONTWRITEBYTECODE=1 -e PYTHONUNBUFFERED=1 \
		-v "$root/tests/apifuzz:/fuzz:ro" -v "$report_dir:/report" \
		--entrypoint python "$IMAGE" /fuzz/fixtures.py serve /report/fixtures >/dev/null
	for _ in $(seq 1 30); do
		if "$docker_cli" exec "$fixtures" python -c \
			"import urllib.request as u; u.urlopen('http://127.0.0.1:8080/clip.nzb')" 2>/dev/null; then
			break
		fi
		sleep 1
	done

	# The engine is the main one plus: the fixtures as the only local archive
	# root and the only private hosts it may fetch from (the proxy and HLS
	# allowlists are per host; archive/NZB fetches have only an on/off switch,
	# and the NNTP connection of an NZB goes through the same switch), DVR on,
	# and a local-files library whose files are served by hash.
	"$docker_cli" run -d --name "$stateful_engine" --network "$net" --network-alias rivulet-stateful \
		-v "$report_dir/fixtures:/fixtures:ro" \
		-e RIVULET_ALLOWED_HOSTS=rivulet-stateful "${engine_env[@]}" \
		-e RIVULET_ARCHIVE_LOCAL_ROOT=/fixtures \
		-e RIVULET_ARCHIVE_ALLOW_PRIVATE=1 \
		-e RIVULET_PROXY_PRIVATE_ALLOW=fixtures \
		-e RIVULET_HLS_PRIVATE_ALLOW=fixtures \
		-e RIVULET_DVR_ENABLED=1 \
		-e RIVULET_DVR_MAX_ACTIVE=16 \
		-e LOCAL_FILES_DIR=/fixtures/library \
		-e RIVULET_LOCAL_FILES_PUBLIC_URL="$stateful_url" \
		"$engine_image" >/dev/null
	wait_up "$stateful_engine"
	fixtures_python setup "$stateful_url" /report
}

if [ "$run_main" = 1 ]; then
	run main schemathesis.toml "$url" \
		not_a_server_error,response_schema_conformance,status_code_conformance,foreign_origin_rejected
fi

if [ "$run_stateful" = 1 ]; then
	start_stateful
	# Only the operations whose success path needs what setup created; the rest
	# of the API is the main pass's business.
	run stateful stateful.toml "$stateful_url" \
		not_a_server_error,response_schema_conformance,status_code_conformance,foreign_origin_rejected \
		--include-path-regex '^/(zip|tar|tgz|7zip|rar|nzb|hlsv2|local-addon|\{archiveType\}|api/recordings|record|probe)(/|$)'
fi

exit "$status"
