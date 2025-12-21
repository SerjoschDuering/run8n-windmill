Push local changes to Windmill remote for the current workspace.

IMPORTANT: This is a destructive operation - remote will be overwritten.

## Safety Checks (MUST DO ALL)

1. **Check branch is allowed**: Run `git branch --show-current`
   - Only `master` is configured in wmill.yaml gitBranches
   - If on any other branch, STOP and warn: "Branch not configured in wmill.yaml. Pushing from this branch could overwrite production!"
   - Ask user to confirm they want to proceed anyway

2. **Check we're in repo root**: Verify `wmill.yaml` exists

3. **Check git status**: Run `git status`
   - If uncommitted changes exist, warn and ask to proceed
   - Recommend: commit first so you can revert if sync goes wrong

4. **Preview changes**: Run `wmill sync push --show-diffs`
   - Show the diff output to user
   - Highlight any DELETIONS (files being removed from remote)

5. **Confirm before executing**: Ask user to confirm push

6. **Run generate-metadata first** (if Python/TS scripts changed):
   ```bash
   wmill script generate-metadata
   ```

7. **Execute push**: `wmill sync push --yes`
