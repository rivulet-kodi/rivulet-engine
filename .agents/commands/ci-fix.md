---
description: Diagnose and fix the failing CI checks of a PR from its own worktree
argument-hint: "<PR number>"
---
<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

Fix failing checks of PR $ARGUMENTS.

- `gh pr checks $ARGUMENTS`, then `gh run view <run-id> --log-failed` for each red job.
- Classify: real (code, lint, tests, REUSE) vs environment (network, registry, runner). Retry environment failures once with `gh run rerun <id> --failed`.
- Fix real ones in the PR's worktree (`git worktree list`); never skip tests or loosen gates.
- Rerun the equivalent locally (see the `.github/workflows/ci.yml` job): `make fmt-check vet lint test`, `reuse lint`. Commit, push, `gh pr checks $ARGUMENTS --watch`.

Hard rules: never push `main`; never force-push unless asked; work in the PR's worktree.
