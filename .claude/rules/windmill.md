---
paths:
  - f/**
---

# Windmill Development Rules

When working with Windmill scripts/flows in this repo.

## Quick Reference

| Type | Structure |
|------|-----------|
| Script | `name.py` + `name.script.yaml` + `name.lock` |
| Flow | `name.flow/flow.yaml` + inline scripts |
| App | `name.app/app.yaml` + inline scripts |

## After Editing

Always run: `wmill script generate-metadata`

This updates lock files. The pre-commit hook does this automatically.

## Gotchas

1. **Function must be `main`** - Required entry point
2. **Type hints = UI schema** - Untyped params hidden
3. **Lock files versioned** - Don't gitignore `*.lock`
4. **Resources in UI** - Secrets never in git
5. **Use internal hosts** - `nocodb:8080`, `redis:6379`

## Detailed Patterns

For comprehensive patterns, see the skill at:
- `~/.claude/skills/run8n-stack/services/windmill-scripts.md`
- `~/.claude/skills/run8n-stack/services/windmill-flows.md`
- `~/.claude/skills/run8n-stack/services/windmill-triggers.md`

## Docs

- [Local Development](https://www.windmill.dev/docs/advanced/local_development)
- [CLI Sync](https://www.windmill.dev/docs/advanced/cli/sync)
