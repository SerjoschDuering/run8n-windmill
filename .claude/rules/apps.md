---
paths:
  - f/app_*/**
---

# Windmill Apps Rules

These folders contain Windmill Apps - internal tools with UI components.

> **Docs**: https://www.windmill.dev/docs/apps/app_editor

## Remote

| Property | Value |
|----------|-------|
| URL | https://windmill.run8n.xyz/ |
| Workspace ID | `windmill_automations` |

## Folder Structure

```
f/
├── app_custom/    # Custom internal tools
├── app_groups/    # Group-specific apps
└── app_themes/    # Theming and styling
```

## App File Structure

Apps are folder-based with this structure:
```
my_app.app/
├── app.yaml          # App definition (layout, components)
└── inline_script_0.py  # Inline scripts (if any)
```

> **Docs**: https://www.windmill.dev/docs/apps/app_configuration

## Creating New Apps

Apps are best created in the Windmill UI (visual builder), then synced:

```bash
# After creating app in UI
wmill sync pull   # Pull to local
# Edit locally if needed
wmill sync push   # Push changes back
```

> **Docs**: https://www.windmill.dev/docs/apps/app_editor

## Naming Conventions

- App folders: `descriptive_name.app/`
- Use lowercase with underscores
- Group related apps in subdirectories

## Common App Patterns

### Data Display App
- Fetch data from resources via backend scripts
- Display in tables/charts using components
- Add filters and search inputs

### Form App
- Input validation with component properties
- Submit to API/database via runnable
- Success/error handling with conditional display

### Dashboard App
- Multiple data sources with parallel runnables
- Real-time updates with refresh intervals
- KPI widgets using text/stat components

## App Components Reference

| Component | Use Case |
|-----------|----------|
| Table | Display tabular data |
| Form | Collect user input |
| Button | Trigger actions |
| Text | Display static/dynamic text |
| Chart | Visualize data |
| Container | Layout grouping |

> **Docs**: https://www.windmill.dev/docs/apps/app_configuration#components
