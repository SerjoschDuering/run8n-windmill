import requests
import wmill
from datetime import datetime, timedelta, timezone


GITHUB_ORG = "Infrared-city"
NOCODB_BASE_URL = "http://nocodb:8080/api/v2"  # internal Docker network

# NocoDB table IDs
TABLES = {
    "commits": "m2vcm24zaoqn7d5",
    "prs": "m655yfcw608losa",
    "org_daily_stats": "migh1jsmzez9soe",
}


def gh_get(path: str, token: str, params: dict = None) -> list | dict:
    """GET from GitHub API with pagination awareness."""
    url = f"https://api.github.com/{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
    if resp.status_code == 409:  # empty repo
        return []
    resp.raise_for_status()
    return resp.json()


def noco_headers(token: str) -> dict:
    return {"xc-token": token, "Content-Type": "application/json"}


def noco_insert(table_id: str, rows: list, token: str) -> int:
    """Insert rows into NocoDB. Returns count inserted."""
    if not rows:
        return 0
    # NocoDB accepts max ~100 rows per request
    inserted = 0
    for i in range(0, len(rows), 100):
        batch = rows[i : i + 100]
        resp = requests.post(
            f"{NOCODB_BASE_URL}/tables/{table_id}/records",
            json=batch,
            headers=noco_headers(token),
            timeout=30,
        )
        resp.raise_for_status()
        inserted += len(batch)
    return inserted


def noco_existing_shas(table_id: str, token: str, since_date: str) -> set:
    """Get existing commit SHAs from NocoDB to avoid duplicates."""
    shas = set()
    try:
        offset = 0
        while True:
            resp = requests.get(
                f"{NOCODB_BASE_URL}/tables/{table_id}/records",
                headers=noco_headers(token),
                params={"fields": "sha", "limit": 200, "offset": offset},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  Dedup query failed ({resp.status_code}), skipping")
                return set()
            data = resp.json()
            for row in data.get("list", []):
                if row.get("sha"):
                    shas.add(row["sha"])
            if data.get("pageInfo", {}).get("isLastPage", True):
                break
            offset += 200
    except Exception as e:
        print(f"  Dedup check failed: {e}, skipping")
    return shas


def noco_existing_prs(table_id: str, token: str, since_date: str) -> set:
    """Get existing PR keys (repo:number) to avoid duplicates."""
    keys = set()
    try:
        offset = 0
        while True:
            resp = requests.get(
                f"{NOCODB_BASE_URL}/tables/{table_id}/records",
                headers=noco_headers(token),
                params={"fields": "repo,pr_number", "limit": 200, "offset": offset},
                timeout=30,
            )
            if resp.status_code != 200:
                print(f"  PR dedup query failed ({resp.status_code}), skipping")
                return set()
            data = resp.json()
            for row in data.get("list", []):
                keys.add(f"{row.get('repo')}:{row.get('pr_number')}")
            if data.get("pageInfo", {}).get("isLastPage", True):
                break
            offset += 200
    except Exception as e:
        print(f"  PR dedup check failed: {e}, skipping")
    return keys


def fetch_active_repos(gh_token: str) -> list[str]:
    """Get non-archived repo names."""
    repos = []
    page = 1
    while True:
        data = gh_get(
            f"orgs/{GITHUB_ORG}/repos",
            gh_token,
            {"per_page": 100, "page": page, "sort": "pushed"},
        )
        if not data:
            break
        for r in data:
            if not r.get("archived", False):
                repos.append(r["name"])
        if len(data) < 100:
            break
        page += 1
    return repos


def fetch_commits(repo: str, since: str, gh_token: str) -> list[dict]:
    """Fetch commits for a repo since a given ISO datetime."""
    raw = gh_get(
        f"repos/{GITHUB_ORG}/{repo}/commits",
        gh_token,
        {"since": since, "per_page": 100},
    )
    if not isinstance(raw, list):
        return []
    rows = []
    for c in raw:
        commit = c.get("commit", {})
        author_obj = commit.get("author", {}) or {}
        msg = (commit.get("message") or "")
        # First line only, cap at 500 chars
        msg_first = msg.split("\n")[0][:500]
        rows.append({
            "repo": repo,
            "sha": (c.get("sha") or "")[:40],
            "author": (
                (c.get("author") or {}).get("login")
                or author_obj.get("name")
                or "unknown"
            ),
            "message": msg_first,
            "date": author_obj.get("date", ""),
            "additions": None,  # not available from list endpoint
            "deletions": None,
        })
    return rows


def fetch_branch_commits(repo: str, branch_sha: str, since: str, gh_token: str) -> list[dict]:
    """Fetch commits from a specific branch head (for open PR branches).
    Uses the PR's head SHA as the starting point so we only get branch-specific commits.
    """
    raw = gh_get(
        f"repos/{GITHUB_ORG}/{repo}/commits",
        gh_token,
        {"sha": branch_sha, "since": since, "per_page": 50},
    )
    if not isinstance(raw, list):
        return []
    rows = []
    for c in raw:
        commit = c.get("commit", {})
        author_obj = commit.get("author", {}) or {}
        msg = (commit.get("message") or "").split("\n")[0][:500]
        rows.append({
            "repo": repo,
            "sha": (c.get("sha") or "")[:40],
            "author": (
                (c.get("author") or {}).get("login")
                or author_obj.get("name")
                or "unknown"
            ),
            "message": msg,
            "date": author_obj.get("date", ""),
            "additions": None,
            "deletions": None,
        })
    return rows


def fetch_prs(repo: str, since: str, gh_token: str) -> list[dict]:
    """Fetch open + recently merged PRs with detail."""
    rows = []

    # Open PRs
    open_prs = gh_get(
        f"repos/{GITHUB_ORG}/{repo}/pulls",
        gh_token,
        {"state": "open", "per_page": 50},
    )
    if isinstance(open_prs, list):
        for pr in open_prs:
            rows.append(_pr_to_row(repo, pr, "open", gh_token))

    # Recently closed (filter to merged after since)
    closed_prs = gh_get(
        f"repos/{GITHUB_ORG}/{repo}/pulls",
        gh_token,
        {"state": "closed", "sort": "updated", "direction": "desc", "per_page": 20},
    )
    if isinstance(closed_prs, list):
        for pr in closed_prs:
            if pr.get("merged_at") and pr["merged_at"] >= since:
                rows.append(_pr_to_row(repo, pr, "merged", gh_token))

    return rows


def _pr_to_row(repo: str, pr: dict, state: str, gh_token: str) -> dict:
    """Convert PR list item to NocoDB row. Fetches detail for +/- lines."""
    number = pr["number"]
    # Fetch detail for additions/deletions (not in list endpoint)
    detail = gh_get(
        f"repos/{GITHUB_ORG}/{repo}/pulls/{number}", gh_token
    )
    body = (detail.get("body") or "")[:500]
    return {
        "repo": repo,
        "pr_number": number,
        "title": (pr.get("title") or "")[:200],
        "description": body,
        "author": (pr.get("user") or {}).get("login", "unknown"),
        "state": state,
        "created_at": pr.get("created_at", ""),
        "merged_at": detail.get("merged_at") or "",
        "merged_by": (detail.get("merged_by") or {}).get("login", ""),
        "additions": detail.get("additions", 0),
        "deletions": detail.get("deletions", 0),
        "files_changed": detail.get("changed_files", 0),
        "labels": ",".join(l.get("name", "") for l in (pr.get("labels") or [])),
        "draft": pr.get("draft", False),
    }


def main(
    github_token: str = "",
    nocodb_token: str = "",
    lookback_days: int = 2,
) -> dict:
    """Daily GitHub → NocoDB sync for Infrared-city org.

    Args:
        github_token: GitHub PAT with repo read access.
        nocodb_token: NocoDB xc-token.
        lookback_days: How many days back to fetch (default 2 for overlap safety).
    """
    # Resolve tokens from Windmill variables if not passed directly
    if not github_token:
        github_token = wmill.get_variable("f/ir_audit/github_pat")
    if not nocodb_token:
        nocodb_token = wmill.get_variable("f/ir_audit/nocodb_token")

    since = (
        datetime.now(timezone.utc) - timedelta(days=lookback_days)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    since_date = since[:10]

    # 1. Get active repos
    repos = fetch_active_repos(github_token)
    print(f"Found {len(repos)} active repos")

    # 2. Get existing data for dedup
    existing_shas = noco_existing_shas(TABLES["commits"], nocodb_token, since_date)
    existing_prs = noco_existing_prs(TABLES["prs"], nocodb_token, since_date)
    print(f"Existing: {len(existing_shas)} commits, {len(existing_prs)} PRs in window")

    # 3. Fetch + deduplicate commits
    all_commits = []
    all_prs = []
    total_additions = 0
    total_deletions = 0
    contributors = set()
    active_repos = 0

    for repo in repos:
        # Default-branch commits
        commits = fetch_commits(repo, since, github_token)
        new_commits = [c for c in commits if c["sha"] not in existing_shas]
        if new_commits:
            all_commits.extend(new_commits)
            active_repos += 1
            for c in new_commits:
                contributors.add(c["author"])
            print(f"  {repo}: {len(new_commits)} new commits")

        prs = fetch_prs(repo, since, github_token)
        new_prs = [
            p for p in prs
            if f"{p['repo']}:{p['pr_number']}" not in existing_prs
        ]
        if new_prs:
            all_prs.extend(new_prs)
            for p in new_prs:
                total_additions += p.get("additions") or 0
                total_deletions += p.get("deletions") or 0
            print(f"  {repo}: {len(new_prs)} new PRs")

        # Branch commits from open PRs — catches work that never hit default branch.
        # Fetches commits on each open PR's head branch so long-lived feature branches
        # show up in the commits table and contributor stats.
        open_prs_raw = gh_get(
            f"repos/{GITHUB_ORG}/{repo}/pulls",
            github_token,
            {"state": "open", "per_page": 20},
        )
        if isinstance(open_prs_raw, list):
            for pr in open_prs_raw:
                head_sha = pr.get("head", {}).get("sha")
                if not head_sha:
                    continue
                branch_commits = fetch_branch_commits(repo, head_sha, since, github_token)
                new_branch = [c for c in branch_commits if c["sha"] not in existing_shas]
                if new_branch:
                    all_commits.extend(new_branch)
                    for c in new_branch:
                        contributors.add(c["author"])
                        existing_shas.add(c["sha"])  # prevent cross-PR dupes
                    if repo not in [c["repo"] for c in new_commits]:
                        active_repos += 1
                    print(f"  {repo} PR#{pr['number']} branch: {len(new_branch)} new commits")

    # 4. Insert into NocoDB
    commits_inserted = noco_insert(TABLES["commits"], all_commits, nocodb_token)
    prs_inserted = noco_insert(TABLES["prs"], all_prs, nocodb_token)

    # 5. Org daily stats
    prs_opened = sum(1 for p in all_prs if p["state"] == "open")
    prs_merged = sum(1 for p in all_prs if p["state"] == "merged")
    stats_row = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "active_repos": active_repos,
        "total_commits": len(all_commits),
        "total_prs_opened": prs_opened,
        "total_prs_merged": prs_merged,
        "total_additions": total_additions,
        "total_deletions": total_deletions,
        "unique_contributors": len(contributors),
        "contributor_list": ",".join(sorted(contributors)),
    }
    noco_insert(TABLES["org_daily_stats"], [stats_row], nocodb_token)

    summary = {
        "status": "success",
        "repos_scanned": len(repos),
        "commits_inserted": commits_inserted,
        "prs_inserted": prs_inserted,
        "active_repos_today": active_repos,
        "unique_contributors": len(contributors),
        "lines_added": total_additions,
        "lines_deleted": total_deletions,
    }
    print(f"\nDone: {summary}")
    return summary
