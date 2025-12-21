---
paths:
  - f/**
---

# Windmill Development Rules

When working with Windmill scripts/flows in this repo.

> **Docs**: https://www.windmill.dev/docs/advanced/local_development

## Script Structure

Every script needs 3 files:
```
f/folder/script_name.py          # Main code
f/folder/script_name.script.yaml # Metadata + schema
f/folder/script_name.lock        # Dependencies (auto-generated)
```

## Python Script Template

```python
def main(param1: str, param2: int = 10) -> dict:
    """
    Brief description of what this script does.

    Args:
        param1: Description of param1
        param2: Description of param2 (default: 10)

    Returns:
        dict with result
    """
    # Your logic here
    return {"result": param1, "count": param2}
```

## TypeScript Script Template (Bun)

```typescript
export async function main(param1: string, param2: number = 10) {
  // Your logic here
  return { result: param1, count: param2 };
}
```

## Metadata YAML Template

```yaml
summary: One-line description
description: |
  Longer description if needed.
schema:
  $schema: 'https://json-schema.org/draft/2020-12/schema'
  type: object
  properties:
    param1:
      type: string
      description: What this param does
    param2:
      type: integer
      default: 10
  required:
    - param1
```

## Flow Structure

```
f/folder/flow_name.flow/
├── flow.yaml          # Flow definition
└── inline_script_0.py # Inline scripts (if any)
```

> **Docs**: https://www.windmill.dev/docs/flows/flow_editor

## After Editing Scripts

Always run: `wmill script generate-metadata`

This updates lock files with dependency versions. The pre-commit hook does this automatically.

## Resources & Variables

- Configure in Windmill UI (not in git) - they contain sensitive connection info
- Reference in code:
  ```python
  import wmill

  db = wmill.get_resource("f/resources/postgres_db")
  api_key = wmill.get_variable("f/variables/api_key")
  ```

> **Docs**: https://www.windmill.dev/docs/core_concepts/resources_and_types

## Supported Languages

| Language | Extension | Runtime |
|----------|-----------|---------|
| Python | `.py` | Python 3.11 |
| TypeScript | `.ts` | Bun |
| JavaScript | `.js` | Bun |
| Go | `.go` | Go |
| Bash | `.sh` | Bash |
| SQL | `.sql` | Database |
| Rust | `.rs` | Rust |
| PHP | `.php` | PHP |

> **Docs**: https://www.windmill.dev/docs/getting_started/scripts_quickstart

## Windmill Hub

The [Windmill Hub](https://hub.windmill.dev/) is a community repository of reusable scripts, flows, and apps.

### Pull from Hub (use community scripts)

```bash
# Pull a script from the public hub (experimental)
wmill hub pull
```

Or browse https://hub.windmill.dev/ and import directly in the Windmill UI.

### Push to Hub (share your scripts)

For pushing to a private Hub, use the separate Hub CLI:

```bash
# Install Hub CLI
npm install -g @windmill-labs/hub-cli

# Configure .env with HUB_URL and TOKEN
# Then push
wmill-hub push
```

> **Docs**: https://www.windmill.dev/docs/misc/share_on_hub
