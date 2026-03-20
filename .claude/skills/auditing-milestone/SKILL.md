---
name: auditing-milestone
description: Run a quality and consistency audit after completing a milestone or before a release. Use WHEN finishing a feature, completing a sprint, or preparing a release.
---

# Auditing a Milestone

Run the quality checklist from Playbook Section 17 against the current repo state.

## Checklist

### 1. Code Quality
- [ ] `make lint` passes (or equivalent linter for repo stack)
- [ ] `make test` passes with no failures
- [ ] Coverage gates met (check CLAUDE.md for thresholds)
- [ ] No `TODO` or `FIXME` comments left untracked in backlog

### 2. Documentation
- [ ] `CLAUDE.md` is accurate — locked decisions match reality
- [ ] `docs/build-notes/quick-reference.md` reflects current state
- [ ] `docs/dev-journal.md` has entries for non-obvious decisions made
- [ ] `docs/engineering-patterns.md` has entries for new patterns introduced
- [ ] `docs/backlog-execution.md` items updated (completed items marked `[x]`)

### 3. Git Hygiene
- [ ] All commits follow conventional format: `type(scope): description`
- [ ] No merge commits on feature branch (rebase if needed)
- [ ] Branch name follows convention: `{scope}/{slug}`
- [ ] No large binary files committed

### 4. Consistency
- [ ] No hardcoded secrets, tokens, or API keys
- [ ] Environment variables documented in CLAUDE.md
- [ ] New dependencies added to pyproject.toml / Cargo.toml (not manually installed)
- [ ] PR template sections filled out completely

### 5. Cross-repo Impact
- [ ] Changes don't break dependent repos (check CLAUDE.md dependency graph)
- [ ] If shared kernel changed, downstream repos tested

## Output
Write audit results to `docs/build-notes/plans/{milestone}-audit.md` with pass/fail for each item and notes on any failures.

## Rules
- Be honest — flag failures, don't skip them.
- For each failure, note the fix needed and estimated effort.
- If lint/test commands aren't available, note it as "SKIP — no runner configured".
