---
name: checking-status
description: Generate a status report from git state, activity log, and backlog. Use WHEN asked for progress, after completing a milestone, or at session start.
---

# Checking Status

Generate a structured status report for the current repo.

## Steps

1. **GIT STATE** — Run:
   ```bash
   git branch --show-current
   git status --short
   git log --oneline -10
   git stash list
   ```

2. **ACTIVITY LOG** — Read last 10 entries from `docs/build-notes/bot-activity-log.jsonl`.

3. **BACKLOG** — Read `docs/backlog-execution.md`. Count items by status: `[ ]` pending, `[~]` in-progress, `[x]` done.

4. **OPEN WORK** — Check for uncommitted changes, open PRs, or unmerged branches.

5. **REPORT** — Output a status summary:

```markdown
## Status Report — {repo name} — {date}

**Branch:** {current branch}
**Last commit:** {hash} {message}
**Uncommitted changes:** {yes/no — list if yes}

### Backlog Progress
- Pending: {n}
- In progress: {n}
- Completed: {n}

### Recent Activity
- {last 3-5 activity log entries, summarized}

### Next Actions
1. {what to do next}
```

## Rules
- Report facts only — no speculation about timelines.
- If activity log is empty, note it and skip that section.
- If backlog is empty, note it and suggest populating it.
