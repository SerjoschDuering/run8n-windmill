import re
import requests
import wmill
from datetime import datetime, timezone

NOCODB_BASE_URL = "http://nocodb:8080/api/v2"
MEMORIES_TABLE = "m428y1kbcf86mdm"

ALLOWED_COMMANDS = {
    "insert", "fetch", "resolve", "search",
}

REPO_RE = re.compile(r"^[a-zA-Z0-9._/-]{1,200}$")
TYPE_VALUES = {"learning", "issue", "pattern", "convention"}
SEVERITY_VALUES = {"info", "warning", "critical"}

MEMORY_FIELDS = {
    "repo", "type", "title", "body", "tags", "severity",
    "user", "branch", "files", "timestamp", "resolved",
}

MAX_RECORDS_PER_INSERT = 10


def _validate_repo(val: str) -> str:
    if not REPO_RE.match(val):
        raise ValueError(f"invalid repo name: {val}")
    return val


def _validate_record(record: dict) -> dict:
    if not isinstance(record, dict):
        raise ValueError("record must be a dict")
    row = {}
    for k, v in record.items():
        if k not in MEMORY_FIELDS:
            continue
        if isinstance(v, str) and len(v) > 5000:
            v = v[:5000]
        row[k] = v
    if "repo" not in row or "type" not in row or "title" not in row:
        raise ValueError("record must have repo, type, title")
    if row.get("type") and row["type"] not in TYPE_VALUES:
        raise ValueError(f"type must be one of {TYPE_VALUES}")
    if row.get("severity") and row["severity"] not in SEVERITY_VALUES:
        raise ValueError(f"severity must be one of {SEVERITY_VALUES}")
    return row


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


def noco_patch(token: str, table_id: str, data: list) -> dict:
    url = f"{NOCODB_BASE_URL}/tables/{table_id}/records"
    headers = {"xc-token": token, "Content-Type": "application/json"}
    resp = requests.patch(url, headers=headers, json=data, timeout=15)
    resp.raise_for_status()
    return resp.json()


def main(command: str, args: dict = {}):
    """
    Memory store proxy for ir-dev plugin.
    Webhook-token scoped to this script only.
    """
    if command not in ALLOWED_COMMANDS:
        return {"error": f"unknown command: {command}",
                "allowed": sorted(ALLOWED_COMMANDS)}

    try:
        token = wmill.get_variable("f/ir_audit/nocodb_token")

        if command == "insert":
            records = args.get("records", [])
            if isinstance(records, dict):
                records = [records]
            if len(records) > MAX_RECORDS_PER_INSERT:
                raise ValueError(f"max {MAX_RECORDS_PER_INSERT} records")
            cleaned = [_validate_record(r) for r in records]
            for r in cleaned:
                if "timestamp" not in r:
                    r["timestamp"] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ")
                if "resolved" not in r:
                    r["resolved"] = False
            return noco_post(token, MEMORIES_TABLE, cleaned)

        elif command == "fetch":
            repo = _validate_repo(args.get("repo", ""))
            limit = min(int(args.get("limit", 20)), 100)
            mem_type = args.get("type", "")
            where = f"(repo,eq,{repo})~and(resolved,eq,0)"
            if mem_type and mem_type in TYPE_VALUES:
                where += f"~and(type,eq,{mem_type})"
            result = noco_get(token, MEMORIES_TABLE, {
                "where": where,
                "sort": "-CreatedAt",
                "limit": str(limit),
            })
            return result.get("list", [])

        elif command == "resolve":
            mem_id = args.get("id")
            if not mem_id:
                raise ValueError("id is required")
            return noco_patch(token, MEMORIES_TABLE,
                              [{"Id": int(mem_id), "resolved": True}])

        elif command == "search":
            query = args.get("query", "")
            if not query or len(query) < 2:
                raise ValueError("query must be at least 2 chars")
            limit = min(int(args.get("limit", 20)), 100)
            result = noco_get(token, MEMORIES_TABLE, {
                "where": f"(title,like,%{query}%)~or(body,like,%{query}%)",
                "sort": "-CreatedAt",
                "limit": str(limit),
            })
            return result.get("list", [])

    except ValueError as e:
        return {"error": f"validation failed: {str(e)}"}
    except requests.RequestException as e:
        return {"error": f"NocoDB request failed: {str(e)}"}
