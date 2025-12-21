Push local changes to Windmill remote for the current workspace.

IMPORTANT: This is a destructive operation - remote will be overwritten.

Steps:
1. Check we're in a workspace folder (has wmill.yaml)
2. Run `git status` to ensure changes are committed
3. If uncommitted changes exist, warn and ask to proceed
4. Run `wmill sync push --show-diffs` to preview
5. Confirm before executing
