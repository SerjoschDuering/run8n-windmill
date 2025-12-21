---
paths:
  - f/app_*/**
---

# Windmill Apps Rules

These folders contain Windmill Apps - internal tools with UI components.

## Remote
- **URL**: https://windmill.run8n.xyz/
- **Workspace ID**: windmill_automations

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

## Creating New Apps

Use the Windmill UI for initial app creation (visual builder), then:
```bash
wmill sync pull   # Pull to local
# Edit locally
wmill sync push   # Push changes
```

## Naming Conventions

- App folders: `descriptive_name.app/`
- Use lowercase with underscores
- Group related apps in subdirectories

## Common Patterns

### Data Display App
- Fetch data from resources
- Display in tables/charts
- Add filters and search

### Form App
- Input validation
- Submit to API/database
- Success/error handling

### Dashboard App
- Multiple data sources
- Real-time updates
- KPI widgets
