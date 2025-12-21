# Windmill Monorepo

This repo mirrors a Windmill workspace via `wmill sync`. **Git is the source of truth**, Windmill is the runtime + UI.

## Remote Instance

| Property | Value |
|----------|-------|
| URL | https://windmill.run8n.xyz/ |
| Workspace ID | `windmill_automations` |
| Branch | `master` → production |

## Repo Map

```
f/
├── app_custom/   # Custom Windmill apps
├── app_groups/   # Group-based apps
├── app_themes/   # Theming/styling apps
├── test/         # Test scripts (validated working)
```

## Golden Rules

1. **Git = source of truth** - edit here, push to Windmill
2. **Secrets stay out of Git** - `wmill.yaml` skips secrets by default
3. **Always commit before sync** - `wmill sync push` is destructive
4. **Lock files are versioned** - ensures reproducible deployments

## CLI Commands

| Task | Command |
|------|---------|
| Check identity | `wmill workspace whoami` |
| Pull from remote | `wmill sync pull` |
| Push to remote | `wmill sync push` |
| Preview changes | `wmill sync push --show-diffs` |
| Update locks | `wmill script generate-metadata` |
| Run a script | `wmill script run f/path/name` |

## Workflow

```
1. wmill sync pull          # Get latest from Windmill
2. Edit under f/**          # Make changes locally
3. wmill script generate-metadata  # Update locks (or let pre-commit hook do it)
4. git commit               # Version control first!
5. wmill sync push          # Deploy to Windmill
```

## File Conventions

| Type | Structure |
|------|-----------|
| Script | `name.py` + `name.script.yaml` + `name.lock` |
| Flow | `name.flow/flow.yaml` + inline scripts |
| App | `name.app/app.yaml` + inline scripts |

## Project Rules

Path-scoped rules in `.claude/rules/` auto-load based on file paths:
- `windmill.md` → applies to all `f/**` files
- `apps.md` → applies to `f/app_*/**` files

## Official Documentation

| Topic | Link |
|-------|------|
| CLI Overview | https://www.windmill.dev/docs/advanced/cli |
| Sync Commands | https://www.windmill.dev/docs/advanced/cli/sync |
| Local Development | https://www.windmill.dev/docs/advanced/local_development |
| Git Sync | https://www.windmill.dev/docs/advanced/git_sync |
| Branch-specific Items | https://www.windmill.dev/docs/advanced/cli/branch-specific-items |
| wmill.yaml Settings | https://www.windmill.dev/docs/advanced/cli/gitsync-settings |
| Windmill Hub | https://hub.windmill.dev/ |

**Local Reference**: See `reference_docs/windmill-cli_latest_reference.md` for detailed CLI reference.

## Troubleshooting

### Sync deletes my local files
`wmill sync pull` makes local match remote. Always commit to git first so you can recover.

### Script not found after push
Check that file is under `f/` and matches `includes` pattern in `wmill.yaml`.

### Lock file missing
Run `wmill script generate-metadata` or commit to trigger pre-commit hook.
