# SPDX-FileCopyrightText: 2026 The rivulet-engine Authors
#
# SPDX-License-Identifier: MIT
"""Schemathesis hooks for the rivulet-engine API fuzzing run (scripts/api-fuzz.sh).

Loaded through SCHEMATHESIS_HOOKS. They register the project-specific check;
the built-in checks and the exclusion list live in the runner and in
schemathesis.toml.
"""

import schemathesis

FOREIGN_ORIGIN = "https://evil.example"


@schemathesis.check
def foreign_origin_rejected(ctx, response, case):
    """A browser request from a foreign Origin must be refused with 403.

    The engine has no authentication; what keeps an arbitrary web page from
    driving it is the Origin allowlist. internal/api ServeHTTP applies it before
    routing, to every method on every path (OPTIONS preflight included), so
    state-changing routes and /proxy are covered along with all the reads. The
    check replays each request of the run with a foreign Origin and fails on
    anything but 403: a 2xx means a hostile page could have used the route, any
    other status means the allowlist was bypassed.
    """
    # A case that already carries an Origin is the fuzzer exploring that header.
    if any(name.lower() == "origin" for name in response.request.headers):
        return None
    headers = dict(case.headers or {})
    headers["Origin"] = FOREIGN_ORIGIN
    probe = case.derive(headers=headers)
    kwargs = dict(ctx.internals.transport_kwargs or {})
    kwargs.pop("session", None)
    replay = case.operation.schema.transport.send(probe, **kwargs)
    ctx.internals.record_case(parent_id=case.id, case=probe)
    ctx.internals.record_response(case_id=probe.id, response=replay)
    if replay.status_code != 403:
        raise AssertionError(
            f"{case.method.upper()} {case.formatted_path} with `Origin: {FOREIGN_ORIGIN}` "
            f"answered {replay.status_code}, expected 403 (origin not allowed)"
        )
    return None
