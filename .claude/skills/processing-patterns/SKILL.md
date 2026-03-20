---
name: processing-patterns
description: Extract reusable patterns from dev-journal.md into engineering-patterns.md. Use WHEN dev-journal has 5+ unprocessed entries or at the end of a sprint.
---

# Processing Patterns

Review dev-journal entries and extract reusable patterns into engineering-patterns.md.

## Steps

1. **READ** — Read `docs/dev-journal.md` in full.
2. **IDENTIFY** — Find entries NOT yet marked with `[→ engineering-patterns.md]`.
3. **EVALUATE** — For each unprocessed entry, ask:
   - Is this a reusable pattern (not a one-off fix)?
   - Would a future developer benefit from knowing this?
   - Can it be generalized beyond the specific context?
4. **EXTRACT** — For qualifying entries, write a pattern to `docs/engineering-patterns.md`:

```markdown
### {Pattern Title}

**Problem**: {What situation triggers this pattern}

**Pattern**: {The reusable solution}

**Example**:
```code
// Concrete code example from the journal entry
```

**Source**: dev-journal {date} — {original title}
```

5. **MARK** — Add `[→ engineering-patterns.md]` to the processed journal entry.
6. **LOG** — Append to `docs/build-notes/bot-activity-log.jsonl`:
```json
{"ts": "{ISO-8601}", "action": "process-patterns", "extracted": {count}, "skipped": {count}}
```

## Rules
- Never delete journal entries — only mark them as processed.
- Patterns must be generalizable — no customer names, specific dates, or one-off hacks.
- Group related patterns under the same heading if they share a theme.
- If no entries qualify, log `"extracted": 0` and move on.
