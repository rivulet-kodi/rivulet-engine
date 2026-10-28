---
description: Implement a topic in its own worktree, check, open a PR and watch CI
argument-hint: "<topic> <what to implement>"
---
<!--
SPDX-FileCopyrightText: 2026 The rivulet-engine Authors

SPDX-License-Identifier: MIT
-->

Implement as a PR: $ARGUMENTS (first word = topic slug). Read `AGENTS.md` and `.agents/rules/review-skills.md` first.

- `git fetch origin && git worktree add -b <type>/<topic> ../rivulet-engine-<topic> origin/main`; work only there.
- Implement; new files get the SPDX header (`2026 The rivulet-engine Authors`, MIT); update docs in the same change.
- Run the checks the diff needs: `make fmt-check`, `make vet`, `make lint`, `make test`, `reuse lint`.
- Commit (conventional subject), `git push -u origin <branch>`, `gh pr create --fill`.
- `gh pr checks --watch`; on red, use `/ci-fix`.

Hard rules: never push `main`; one worktree per topic; no new dependencies.
