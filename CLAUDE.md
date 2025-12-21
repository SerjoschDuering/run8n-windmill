# Windmill Monorepo

This repo mirrors a Windmill workspace via `wmill sync`. **Git is the source of truth**, Windmill is the runtime + UI.

## Repo Map

```
f/
├── automations/  -> Windmill folder /automations/**
├── _shared/      -> Shared modules/scripts
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

See `.claude/rules/` for path-scoped context per project.

@.claude/rules/windmill.md
