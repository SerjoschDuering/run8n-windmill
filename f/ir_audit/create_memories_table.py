import requests
import wmill

NOCODB_BASE_URL = "http://nocodb:8080/api/v2"


def main():
    """
    One-off script: Create the 'memories' table in the ir_audit NocoDB base.
    Run once, then disable or delete.
    """
    token = wmill.get_variable("f/ir_audit/nocodb_token")
    headers = {"xc-token": token, "Content-Type": "application/json"}

    # Get the base ID from an existing table
    # Table m2vcm24zaoqn7d5 = commits table
    resp = requests.get(
        f"{NOCODB_BASE_URL}/meta/tables/m2vcm24zaoqn7d5",
        headers=headers, timeout=15
    )
    resp.raise_for_status()
    base_id = resp.json()["base_id"]

    # Create memories table
    table_def = {
        "table_name": "memories",
        "columns": [
            {"column_name": "repo", "uidt": "SingleLineText"},
            {"column_name": "type", "uidt": "SingleLineText"},
            {"column_name": "title", "uidt": "SingleLineText"},
            {"column_name": "body", "uidt": "LongText"},
            {"column_name": "tags", "uidt": "SingleLineText"},
            {"column_name": "severity", "uidt": "SingleLineText"},
            {"column_name": "user", "uidt": "SingleLineText"},
            {"column_name": "branch", "uidt": "SingleLineText"},
            {"column_name": "files", "uidt": "LongText"},
            {"column_name": "timestamp", "uidt": "SingleLineText"},
            {"column_name": "resolved", "uidt": "Checkbox"},
        ],
    }

    resp = requests.post(
        f"{NOCODB_BASE_URL}/meta/bases/{base_id}/tables",
        headers=headers, json=table_def, timeout=15
    )
    resp.raise_for_status()
    result = resp.json()

    return {
        "status": "created",
        "table_id": result["id"],
        "table_name": result["title"],
        "base_id": base_id,
    }
