---
name: exodus-dev-loop
description: >
  Guide through the Exodus development loops — PRIVIA (developer workflow) and
  PEVR (agent loop). Use when starting any non-trivial coding task, structuring
  your implementation plan, or reviewing completed work.
triggers:
  - "start a new feature"
  - "plan this task"
  - "development loop"
  - "help me structure this"
  - "how should I approach this"
outputs:
  - Written plan with numbered steps and acceptance criteria
  - Verify results report
  - Commit message
---

# Exodus Development Loop

## Two Loops — Know Which Applies

**PRIVIA** = developer + AI workflow (this skill)
**PEVR** = agent loop in `autopilot/agents/base.py` (code — not this skill)

---

## When to Use This Skill

- Starting any coding task spanning more than one file
- Adding a new agent, extractor, dbt model, or Terraform module
- Refactoring or redesigning existing components
- Any task with architectural implications

Skip for: single-line fixes, comment/doc rewording, config value changes.

---

## PRIVIA Loop

### Step 1: PLAN

Before writing any code, produce a written plan:

```
## Task
[One sentence: what we're building/changing]

## Context
[What existing code did you read? What patterns did you find?]

## Implementation Steps
1. [Step — file — what changes]
2. [Step — file — what changes]
...

## Acceptance Criteria
- [ ] [Testable criterion 1]
- [ ] [Testable criterion 2]
...

## Files to Change
- `path/to/file.py` — [what changes]
- `path/to/test_file.py` — [new/modified tests]

## Risk Level
[LOW / MEDIUM / HIGH] — [one line justification]

## Model Tier
[opus / sonnet / haiku] — [why this tier — reference model-routing.mdc]
```

**Do not proceed until the plan is written.**

---

### Step 2: REVIEW (plan)

Self-check before implementing:

```
## Plan Review

- [ ] All acceptance criteria are achievable with the listed steps
- [ ] Tests are included in the plan
- [ ] No steps violate Exodus standards (naming, typing, structure)
- [ ] No hidden dependencies or missing prerequisite steps
- [ ] Scope is appropriate — not too broad, not leaving gaps
- [ ] Risk level is correctly assessed

Gaps found: [list, or "None"]
Plan adjusted: [yes/no — describe if yes]
```

**If gaps found, revise before proceeding.**

---

### Step 3: IMPLEMENT

Execute step by step. For each step:

```
## Step N: [Step name]

[Brief description]

[Code changes / files created]

Step complete: [ ]
```

Rules:
- Read files before editing them
- Write tests alongside code — not after
- If plan is wrong, stop and replan
- `uv run` for all Python commands — never `python` or `pip` directly
- Reference model tiers (not IDs): `DEFAULT_MODEL = "claude-sonnet"`

---

### Step 4: VERIFY

Run all checks:

```bash
# Python
uv run ruff check .
uv run mypy .
uv run pytest tests/ -v -x

# dbt (if changed)
cd dbt && dbt compile --select modified+
dbt test --select modified+

# Terraform (if changed)
terraform fmt -check && terraform validate

# IronLayer (if changed)
uv run pytest core_engine/tests/ -v -x
```

Report:

```
## Verify Results

- ruff: [PASS / FAIL — details]
- mypy: [PASS / FAIL — details]
- pytest: [PASS / FAIL — N passed, M failed]

All checks passing: [yes/no]
```

**Do not proceed to INSPECT if checks fail — fix them first.**

---

### Step 5: INSPECT

Self-review implementation against plan:

```
## Implementation Review

Acceptance criteria:
- [ ] [Criterion 1] — [how it was met]
- [ ] [Criterion 2] — [how it was met]

Quality checks:
- [ ] Consistent with surrounding code style
- [ ] All symbols named per Exodus standards
- [ ] No hardcoded values that should be config or env vars
- [ ] No hard-coded Claude model IDs (use tier aliases)
- [ ] Error handling appropriate
- [ ] New public interfaces documented
- [ ] Implementation matches plan (flag deviations)

Deviations from plan: [list, or "None"]
Issues found: [list, or "None — ready to commit"]
```

---

### Step 6: ACCEPT or ITERATE

Issues found in VERIFY or INSPECT:
- **Minor** (formatting, naming): fix in place, re-run VERIFY
- **Substantive** (logic, architecture): return to Step 1 (PLAN)
- **Max 3 full iterations** before flagging for human review

If all clean:

```
## Ready to Commit

Commit message:
  type(scope): description
  
  [optional body]

Files changed:
  [list]
```

Use conventional commit format. Scopes: `ironlayer`, `autopilot`, `foundation`, `mcp`,
`github-app`, `agents`, `terraform`, `dbt`, `cli`, `ingestion`, `ai`, `docs`, `workspace`.

---

## References

- Cursor rule: `ai/cursor/rules/development-loop.mdc`
- Model routing: `ai/model-routing.yml` and `ai/cursor/rules/model-routing.mdc`
- Plan template: `ai/shared/prompts/plan-template.md`
- AutopilotAgent PEVR: `exodus-autopilot/autopilot/agents/base.py`
