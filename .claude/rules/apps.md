---
paths:
  - f/app_*/**
---

# Windmill Apps Rules

Folders containing Windmill Apps - internal tools with UI components.

## App Structure

```
my_app.app/
├── app.yaml           # App definition
└── inline_script_0.py # Inline scripts (if any)
```

## Workflow

Apps are best created in Windmill UI, then synced:

```bash
wmill sync pull   # Pull to local
# Edit locally
wmill sync push   # Push changes
```

## Detailed Patterns

For comprehensive app patterns, see:
- `~/.claude/skills/run8n-stack/services/windmill-apps.md`

## Docs

- [App Editor](https://www.windmill.dev/docs/apps/app_editor)
