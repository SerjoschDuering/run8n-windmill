import re
import requests
import wmill
from datetime import datetime, timedelta, timezone


NOCODB_BASE_URL = "http://nocodb:8080/api/v2"

TABLES = {
    "commits": "m2vcm24zaoqn7d5",
    "prs": "m655yfcw608losa",
    "org_daily_stats": "migh1jsmzez9soe",
    "repo_health_scores": "mnj5ouc64gu0kuw",
    "pr_review_scores": "mzf3tcizmt89grr",
}

ALLOWED_COMMANDS = {
    "recent-commits", "recent-prs", "org-stats", "changed-repos",
    "last-health-date", "last-pr-review-date",
    "insert-health", "insert-pr-review",
    "contributor-activity",
}

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
REPO_RE = re.compile(r"^[a-zA-Z0-9._-]{1,100}$")

# Allowed fields for insert records (whitelist)
HEALTH_FIELDS = {
    "repo", "date", "activity", "activity_reasoning",
    "security", "security_reasoning", "bus_factor", "bus_factor_reasoning",
    "ci_health", "ci_health_reasoning", "dependency_freshness",
    "dependency_freshness_reasoning", "overall", "flags",
    "commit_count_30d", "contributors_30d", "open_prs",
    "dependabot_critical", "dependabot_high", "dependabot_total", "notes",
}

PR_REVIEW_FIELDS = {
    "repo", "pr_number", "title", "author", "date_merged",
    "files_changed", "additions", "deletions",
    "scope", "scope_reasoning", "test_coverage", "test_coverage_reasoning",
    "security", "security_reasoning", "description_score", "description_reasoning",
    "review_quality", "review_quality_reasoning", "breaking_risk",
    "breaking_risk_reasoning", "overall", "flags", "summary",
}

MAX_RECORDS_PER_INSERT = 50
MAX_DAYS = 90


def _validate_days(args: dict) -> int:
    days = args.get("days", 7)
    if not isinstance(days, (int, float)):
        raise ValueError(f"days must be a number, got {type(days).__name__}")
    days = int(days)
    if days < 1 or days > MAX_DAYS:
        raise ValueError(f"days must be 1-{MAX_DAYS}, got {days}")
    return days


def _validate_date(val: str) -> str:
    if not DATE_RE.match(val):
        raise ValueError(f"invalid date format: {val}, expected YYYY-MM-DD")
    datetime.strptime(val, "%Y-%m-%d")  # verify it's a real date
    return val


def _validate_repo(val: str) -> str:
    if not REPO_RE.match(val):
        raise ValueError(f"invalid repo name: {val}")
    return val


def _validate_records(records, allowed_fields: set) -> list:
    if not isinstance(records, list):
        raise ValueError("records must be a list")
    if len(records) > MAX_RECORDS_PER_INSERT:
        raise ValueError(f"max {MAX_RECORDS_PER_INSERT} records per insert")
    if len(records) == 0:
        raise ValueError("records list is empty")
    cleaned = []
    for r in records:
        if not isinstance(r, dict):
            raise ValueError("each record must be a dict")
        row = {}
        for k, v in r.items():
            if k not in allowed_fields:
                continue  # silently drop unknown fields
            if isinstance(v, str) and len(v) > 2000:
                v = v[:2000]  # truncate long strings
            row[k] = v
        cleaned.append(row)
    return cleaned


def noco_get(token: str, table_id: str, params: dict = None) -> dict:
    url = f"{NOCODB_BASE_URL}/tables/{table_id}/records"
    headers = {"xc-token": token, "Content-Type": "application/json"}
    resp = requests.get(url, headers=headers, params=params or {}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def noco_post(token: str, table_id: str, data: list) -> dict:
    url = f"{NOCODB_BASE_URL}/tables/{table_id}/records"
    headers = {"xc-token": token, "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


def main(command: str, args: dict = {}):
    """
    NocoDB query proxy for ir-github-audit plugin.
    Validates all inputs. Webhook-token scoped to this script only.
    """
    if command not in ALLOWED_COMMANDS:
        return {"error": f"unknown command: {command}",
                "allowed": sorted(ALLOWED_COMMANDS)}

    try:
        token = wmill.get_variable("f/ir_audit/nocodb_token")

        if command == "recent-commits":
            days = _validate_days(args)
            since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            result = noco_get(token, TABLES["commits"], {
                "where": f"(date,gte,exactDate,{since})",
                "sort": "-date", "limit": "200",
                "fields": "repo,sha,author,message,date",
            })
            return result.get("list", [])

        elif command == "recent-prs":
            days = _validate_days(args)
            since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            result = noco_get(token, TABLES["prs"], {
                "where": f"(created_at,gte,exactDate,{since})",
                "sort": "-created_at", "limit": "100",
                "fields": "repo,pr_number,title,author,state,additions,deletions,files_changed,created_at,merged_at",
            })
            return result.get("list", [])

        elif command == "org-stats":
            days = _validate_days(args)
            since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
            result = noco_get(token, TABLES["org_daily_stats"], {
                "where": f"(date,gte,exactDate,{since})",
                "sort": "-date", "limit": str(days),
            })
            return result.get("list", [])

        elif command == "changed-repos":
            since = _validate_date(args.get("since", ""))
            result = noco_get(token, TABLES["commits"], {
                "where": f"(date,gte,exactDate,{since})",
                "fields": "repo", "limit": "1000",
            })
            repos = sorted(set(r["repo"] for r in result.get("list", [])))
            return repos

        elif command == "last-health-date":
            params = {"sort": "-date", "limit": "1", "fields": "date,repo"}
            repo = args.get("repo")
            if repo:
                repo = _validate_repo(repo)
                params["where"] = f"(repo,eq,{repo})"
            result = noco_get(token, TABLES["repo_health_scores"], params)
            rows = result.get("list", [])
            return rows[0]["date"] if rows else "never"

        elif command == "last-pr-review-date":
            result = noco_get(token, TABLES["pr_review_scores"], {
                "sort": "-date_merged", "limit": "1",
                "fields": "date_merged,repo",
            })
            rows = result.get("list", [])
            return rows[0]["date_merged"] if rows else "never"

        elif command == "insert-health":
            records = _validate_records(args.get("records", []), HEALTH_FIELDS)
            return noco_post(token, TABLES["repo_health_scores"], records)

        elif command == "insert-pr-review":
            records = _validate_records(args.get("records", []), PR_REVIEW_FIELDS)
            return noco_post(token, TABLES["pr_review_scores"], records)

        elif command == "contributor-activity":
            # Returns unified commits + PRs for a contributor.
            # NocoDB alone only captures default-branch commits — PRs surface
            # branch-level work that never hit main. Union both for full picture.
            author = args.get("author", "")
            if not author or not REPO_RE.match(author):
                return {"error": "author is required (GitHub login)"}
            days = _validate_days(args)
            since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

            commits_result = noco_get(token, TABLES["commits"], {
                "where": f"(date,gte,exactDate,{since})~and(author,eq,{author})",
                "sort": "-date", "limit": "200",
                "fields": "repo,sha,author,message,date",
            })
            prs_result = noco_get(token, TABLES["prs"], {
                "where": f"(created_at,gte,exactDate,{since})~and(author,eq,{author})",
                "sort": "-created_at", "limit": "100",
                "fields": "repo,pr_number,title,author,state,additions,deletions,files_changed,created_at,merged_at",
            })
            commits = commits_result.get("list", [])
            prs = prs_result.get("list", [])
            return {
                "author": author,
                "days": days,
                "commit_count": len(commits),
                "pr_count": len(prs),
                "commits": commits,
                "prs": prs,
                "note": "NocoDB commits = default-branch only. PRs capture branch work. For complete history use GitHub API: gh api search/commits?q=org:Infrared-city+author:{author}",
            }

    except ValueError as e:
        return {"error": f"validation failed: {str(e)}"}
    except requests.RequestException as e:
        return {"error": f"NocoDB request failed: {str(e)}"}
