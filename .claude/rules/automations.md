---
paths:
  - f/automations/**
---

# Automations Project Rules

This folder syncs to Windmill workspace `windmill_automations` at https://windmill.run8n.xyz/

## Folder Organization

```
f/automations/
├── etl/           # Data extraction/transformation
├── notifications/ # Alerts, emails, Slack messages
├── integrations/  # External API connectors
├── scheduled/     # Cron-triggered jobs
└── webhooks/      # HTTP-triggered endpoints
```

## Naming Conventions

- Scripts: `snake_case.py` or `camelCase.ts`
- Flows: `descriptive_name.flow/`
- Keep names short but descriptive

## Common Patterns

### Scheduled ETL
```python
# f/automations/etl/daily_sync.py
def main():
    # Fetch from source
    # Transform
    # Load to destination
    return {"synced": count}
```

### Webhook Handler
```python
# f/automations/webhooks/handle_event.py
def main(payload: dict, headers: dict = None):
    event_type = payload.get("type")
    # Process event
    return {"processed": True}
```

### Notification
```python
# f/automations/notifications/send_alert.py
def main(channel: str, message: str, severity: str = "info"):
    # Send to Slack/email/etc
    return {"sent": True}
```
