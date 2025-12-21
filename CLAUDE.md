# Windmill Monorepo

This repo mirrors a Windmill workspace via `wmill sync`. **Git is the source of truth**, Windmill is the runtime + UI.

## Repo Map

```
f/
├── app_custom/   # Custom Windmill apps
├── app_groups/   # Group-based apps
├── app_themes/   # Theming/styling apps
├── _shared/      # Shared modules (to be created)
```

**Remote**: https://windmill.run8n.xyz/ (workspace: `windmill_automations`)

## Golden Rules

1. **Git = source of truth** - edit here, push to Windmill
2. **Secrets stay out of Git** - `wmill.yaml` skips secrets by default
3. **Always version control before sync** - `wmill sync push` is destructive

## Commands

| Task | Command |
|------|---------|
| Check workspace | `wmill workspace whoami` |
| Pull from Windmill | `wmill sync pull` |
| Push to Windmill | `wmill sync push` |
| Update metadata/locks | `wmill script generate-metadata` |

## Workflow

1. `wmill sync pull` (bootstrap or after UI edits)
2. Edit under `f/**`
3. `wmill script generate-metadata` (update locks)
4. `wmill sync push`
5. `git commit`

## Windmill File Conventions

- **Scripts**: `name.py` + `name.script.yaml` + `name.lock`
- **Flows**: `name.flow/flow.yaml`
- **Apps**: `name.app/app.yaml`

## Project Rules

Path-scoped rules in `.claude/rules/` are auto-loaded based on file paths:
- `windmill.md` → applies to all `f/**` files
- `apps.md` → applies to `f/app_*/**` files
