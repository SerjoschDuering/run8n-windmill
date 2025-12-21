---
paths:
  - f/**
---

# Windmill Development Rules

When working with Windmill scripts/flows in this repo:

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

## After Editing Scripts
Always run: `wmill script generate-metadata`

## Resources & Variables
- Configure in Windmill UI (not in git)
- Reference in code: `wmill.get_resource("resource_path")`
- Variables: `wmill.get_variable("variable_path")`
