# Windmill CLI Reference Documentation

**Version:** Latest (December 2024)
**Source:** Official Windmill Documentation

---

## Table of Contents

1. [Installation](#installation)
2. [Workspace Management](#workspace-management)
3. [Sync Operations](#sync-operations)
4. [wmill.yaml Configuration](#wmllyaml-configuration)
5. [Folder Structure Conventions](#folder-structure-conventions)
6. [Git Sync & gitBranches](#git-sync--gitbranches)
7. [Local Development Workflow](#local-development-workflow)
8. [Best Practices](#best-practices)

---

## Installation

```bash
npm install -g windmill-cli
```

**Requirements:** Node.js > v20

**Verify installation:**
```bash
wmill --version
```

**Upgrade:**
```bash
wmill upgrade
```

**Shell completions:**
```bash
# Bash (~/.bashrc)
source <(wmill completions bash)

# Zsh (~/.zshrc)
source <(wmill completions zsh)
```

---

## Workspace Management

### Commands

| Command | Description |
|---------|-------------|
| `wmill workspace` | List all workspaces (active one underlined) |
| `wmill workspace add [name] [id] [remote]` | Register a workspace |
| `wmill workspace switch <name>` | Switch active workspace |
| `wmill workspace remove <name>` | Remove workspace registration |
| `wmill workspace whoami` | Show current user/workspace |

### Example Setup

```bash
# Add a workspace
wmill workspace add prod windmill_automations https://windmill.run8n.xyz/

# Switch to it
wmill workspace switch prod

# Verify
wmill workspace whoami
```

---

## Sync Operations

### Core Commands

| Command | Description |
|---------|-------------|
| `wmill sync pull` | Download remote workspace to local |
| `wmill sync push` | Upload local files to remote |

**Important:** Syncing is a one-off operation with NO state maintained. It will:
- Override target items
- Remove items absent in source
- Create new items
- Request confirmation before changes

### Pull Options

| Option | Description |
|--------|-------------|
| `--yes` | Execute without confirmation |
| `--skip-variables` | Exclude variables |
| `--skip-resources` | Exclude resources |
| `--skip-secrets` | Exclude secrets (default) |
| `--include-schedules` | Include schedules |
| `--include-triggers` | Include triggers |
| `--plain-secrets` | Download unencrypted secrets |
| `--json` | Output JSON instead of YAML |
| `-i, --includes` | Comma-separated glob patterns |

### Push Options

| Option | Description |
|--------|-------------|
| `--yes` | Execute without confirmation |
| `--message "<text>"` | Attach metadata to scripts/flows/apps |

### Recommended Initial Pull

```bash
wmill sync pull --skip-variables --skip-secrets --skip-resources
```

---

## wmill.yaml Configuration

### Complete Reference

```yaml
# TypeScript runtime (bun or deno)
defaultTs: bun

# Sync scope - glob patterns
includes:
  - f/**

excludes:
  - "**/*.tmp"
  - "**/.DS_Store"

# Content filtering
skipVariables: true      # Default: true (recommended)
skipResources: true      # Default: true (recommended)
skipSecrets: true        # Default: true (NEVER disable for git repos)
includeSchedules: false  # Default: false
includeTriggers: false   # Default: false

# Branch-specific configuration
gitBranches:
  main:
    baseUrl: 'https://windmill.example.com/'
    workspaceId: production
    overrides: {}
    promotionOverrides: {}
  staging:
    baseUrl: 'https://windmill.example.com/'
    workspaceId: staging
```

### Configuration Priority

CLI flags > promotionOverrides > branch overrides > top-level settings

### Default Values (if not specified)

| Option | Default |
|--------|---------|
| `defaultTs` | bun |
| `includes` | ["f/**"] |
| `excludes` | [] |
| `skipVariables` | true |
| `skipResources` | true |
| `skipSecrets` | true |
| `includeSchedules` | false |
| `includeTriggers` | false |

---

## Folder Structure Conventions

### Standard Prefixes

| Prefix | Purpose |
|--------|---------|
| `f/` | Folder namespace (shared/team resources) |
| `u/` | User namespace (personal resources) |

### File Types

**Scripts (3 files each):**
```
f/folder/script_name.py           # Code
f/folder/script_name.script.yaml  # Metadata
f/folder/script_name.lock         # Dependencies (auto-generated)
```

**Flows:**
```
f/folder/flow_name.flow/
  flow.yaml                       # Flow definition
  inline_script_0.py              # Inline scripts
```

**Apps:**
```
f/folder/app_name.app/
  app.yaml                        # App definition
  inline_script_0.py              # Inline scripts
```

### Language Extensions

| Language | Extension |
|----------|-----------|
| Python | .py |
| TypeScript | .ts |
| Go | .go |
| Bash | .sh |
| PowerShell | .ps1 |

---

## Git Sync & gitBranches

### Modes

1. **Sync Mode (Default):** Changes commit directly to specified branch
2. **Promotion Mode:** Creates branches per object with `wm_deploy/` prefix for PR workflows

### gitBranches Configuration

```yaml
gitBranches:
  main:
    baseUrl: 'https://windmill.example.com/'
    workspaceId: production
    overrides:
      skipSecrets: true
    promotionOverrides: {}

  staging:
    baseUrl: 'https://windmill.example.com/'
    workspaceId: staging
```

### Path Filters

- `*` matches any string except `/`
- `**` matches any string including nested paths

### Type Filters (Git Sync UI)

- Scripts, Flows, Apps, Folders
- Resources, Variables (optional secrets)
- Schedules, Triggers
- Resource types, Users, Groups
- Workspace settings, Encryption keys

---

## Local Development Workflow

### Initial Setup

```bash
# 1. Create/navigate to project directory
mkdir windmill-project && cd windmill-project

# 2. Add workspace
wmill workspace add prod windmill_automations https://windmill.run8n.xyz/

# 3. Pull content (exclude sensitive data)
wmill sync pull --skip-variables --skip-secrets --skip-resources

# 4. Initialize wmill.yaml (optional but recommended)
wmill init
```

### Development Cycle

```bash
# 1. Edit scripts in your IDE

# 2. Regenerate metadata after changes
wmill script generate-metadata

# 3. Push to Windmill
wmill sync push

# 4. Commit to git
git add . && git commit -m "Update scripts"
```

### Bootstrap New Items

```bash
# New script
wmill script bootstrap f/folder/script_name python

# New flow
wmill flow bootstrap f/folder/flow_name
```

### Dependency Detection

The CLI automatically detects:
- `package.json` for Node.js dependencies
- `requirements.txt` for Python dependencies

These take precedence over manually specified dependencies in metadata files.

---

## Best Practices

### Security

1. **Never commit secrets:** Always keep `skipSecrets: true`
2. **Skip resources/variables:** Keep credentials in Windmill UI only
3. **Use `.gitignore`:** Exclude `.env`, `*.secret.*`, lockfiles if desired

### Git Workflow

1. **Version control first:** Always commit before sync operations
2. **Pull before push:** Avoid overwriting remote changes
3. **Separate environments:** Use gitBranches for staging/production

### Recommended Production Setup

```
Developers -> Staging Workspace -> Git (staging branch)
                                      |
                                      v (merge/PR)
                                   Git (main branch)
                                      |
                                      v (CI/CD trigger)
                              Production Workspace
```

### File Organization

1. Group related scripts in subfolders under `f/`
2. Use descriptive folder names
3. Keep shared utilities in `f/_shared/` or similar
4. Maintain consistent naming conventions

### Metadata Management

1. Run `wmill script generate-metadata` after code changes
2. Don't manually edit `.lock` files
3. Keep metadata files synchronized with code

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Sync conflicts | Pull first, resolve, then push |
| Missing metadata | Run `wmill script generate-metadata` |
| Auth errors | Re-run `wmill workspace add` |
| Version mismatch | Run `wmill upgrade` |

### Verification Commands

```bash
wmill workspace whoami    # Check current workspace
wmill --version           # Check CLI version
wmill sync pull --yes     # Test connection
```
