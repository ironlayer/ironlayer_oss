---
name: generating-plan
description: Create a structured implementation plan document before writing code. Use WHEN starting any non-trivial task that touches 3+ files or requires architectural decisions.
---

# Generating a Plan

Create a plan document per Playbook Section 3 before writing any code.

## When to Plan
- Task touches 3+ files
- Task involves architectural decisions or new patterns
- Task has unclear scope or multiple valid approaches
- Complexity tier >= Medium

## Steps

1. **READ CONTEXT** — Read `CLAUDE.md`, `docs/build-notes/quick-reference.md`, `docs/engineering-patterns.md`, and `docs/dev-journal.md`.
2. **READ SOURCE** — Read every file you expect to modify. Mark each as VERIFIED.
3. **WRITE PLAN** — Create `docs/build-notes/plans/{item-id}-plan.md` with this structure:

```markdown
# {Item ID}: {Title}

## Summary
One paragraph describing the change and WHY.

## Complexity Tier
Low / Medium / High / Critical

## Files to Change
| File | Action | Description |
|------|--------|-------------|
| path/to/file.py | Modify | What changes |

## Approach
Numbered steps with rationale for each.

## Risks
- Risk 1 → Mitigation
- Risk 2 → Mitigation

## Tests
- [ ] Unit test for X
- [ ] Integration test for Y

## Estimated Commits
1. `{item-id}: {first commit}`
2. `{item-id}: {second commit}`
```

4. **SELF-REVIEW** — Re-read the plan against acceptance criteria and locked decisions in CLAUDE.md.

## Rules
- Never start coding without a plan for Medium+ tasks.
- Plans are living documents — update as you learn.
- Reference locked decisions by number (e.g., "per Decision #3").
- If the plan reveals the task is larger than expected, split it.
