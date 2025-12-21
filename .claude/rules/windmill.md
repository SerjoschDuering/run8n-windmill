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

Resources and variables are configured in Windmill UI (not in git) because they contain sensitive connection info.

### Using Resources

```python
import wmill

# Get a typed resource (e.g., PostgreSQL connection)
db = wmill.get_resource("f/resources/postgres_db")
# Returns: {"host": "...", "port": 5432, "user": "...", "password": "...", "dbname": "..."}

# Use it
import psycopg2
conn = psycopg2.connect(**db)
```

### Using Variables

```python
import wmill

# Get a variable (can be secret or plain)
api_key = wmill.get_variable("f/variables/api_key")
base_url = wmill.get_variable("f/variables/base_url")
```

> **Docs**: https://www.windmill.dev/docs/core_concepts/resources_and_types

## Calling Scripts from Scripts

You can call other Windmill scripts from within a script. All scripts in `f/` are available.

> **Docs**: https://www.windmill.dev/docs/advanced/clients/python_client

### Python - Synchronous

```python
import wmill

def main():
    # Run another script and wait for result
    result = wmill.run_script_by_path(
        "f/utils/fetch_data",  # Script path (same as in this repo!)
        args={"param1": "value", "param2": 42}
    )
    return {"fetched": result}
```

### Python - Async (Background)

```python
import wmill

def main():
    # Start script without waiting (returns job UUID)
    job_id = wmill.run_script_by_path_async(
        "f/long_running/process_data",
        args={"batch_id": 123}
    )
    return {"started_job": job_id}
```

### TypeScript

```typescript
import * as wmill from "windmill-client";

export async function main() {
  // Run another script synchronously
  const result = await wmill.runScriptByPath(
    "f/utils/fetch_data",
    { param1: "value", param2: 42 }
  );
  return { fetched: result };
}
```

### Alternative: Direct Python Imports

You can also import functions directly from other scripts:

```python
# Import from another script in the repo
from f.utils.helpers import my_function

def main():
    result = my_function("input")
    return result
```

> **Docs**: https://www.windmill.dev/docs/advanced/sharing_common_logic

## Available Scripts in This Repo

All scripts under `f/` can be called by path:

```
f/test/hello_world        → wmill.run_script_by_path("f/test/hello_world", {...})
f/etl/fetch_data          → wmill.run_script_by_path("f/etl/fetch_data", {...})
f/utils/send_notification → wmill.run_script_by_path("f/utils/send_notification", {...})
```

Scripts are synced to Windmill, so any script in this repo is callable.

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
