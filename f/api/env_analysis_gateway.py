"""
Windmill Script: Environmental Analysis API Gateway
Path: f/api/env_analysis_gateway

Single entry point for all env-analysis MCP server calls.
Authentication handled by Windmill HTTP Route (API Key auth).

HTTP Route Config:
  Path: /env-analysis
  Method: POST
  Script: f/api/env_analysis_gateway
  Auth: API Key (header: X-API-Key, resource: f/api/env_analysis_api_key)

Usage:
  POST https://windmill.run8n.xyz/api/r/windmill_automations/env-analysis
  Header: X-API-Key: <your_custom_key>
  Body: { "action": "geocode", "params": {"query": "Hamburg"} }

Async Actions (for batch processing):
  - start_simulation_async: Start simulation, returns job_id immediately
  - check_job_status: Check status of job IDs
  - get_job_result: Get result for completed job
"""

import wmill
import time


# Allowed actions: (path, is_flow)
# For async actions, path is "internal" to indicate special handling
ALLOWED_ACTIONS: dict[str, tuple[str, bool]] = {
    "geocode": ("f/geo/geocode", False),
    "fetch_buildings": ("f/geo/flows/fetch_buildings", True),
    "fetch_trees": ("f/geo/trees/fetch", True),
    "simulate": ("f/infrared/simulate", False),
    "kpis": ("f/infrared/kpis", False),
    "gemini_context": ("f/infrared/gemini_context", False),
    "report_ai_summary": ("f/infrared/report_ai_summary", False),
    "report_markdown": ("f/infrared/report_markdown", False),
    "report_pdf": ("f/infrared/report_pdf", False),
    "render_map_image": ("f/geo/render_map_image", False),
    # S3 upload
    "upload_to_s3": ("f/storage/upload_screenshot", False),
    # Session store + enrichment
    "session_store": ("f/infrared/session_store", False),
    # Async actions for batch processing
    "start_simulation_async": ("f/infrared/simulate", False),
    "start_report_async": ("f/infrared/report_pdf", False),
    "start_enrich_async": ("f/infrared/enrich_simulation", False),
    "start_assemble_pdf_async": ("f/infrared/assemble_pdf", False),
    "check_job_status": ("internal", False),
    "get_job_result": ("internal", False),
}


def run_flow_sync(path: str, args: dict, timeout: int = 300) -> dict:
    """Run a flow and wait for the result."""
    # Start the flow asynchronously
    job_id = wmill.run_flow_async(path=path, args=args)

    # Poll for completion - SDK returns UPPERCASE: "RUNNING", "WAITING", "COMPLETED"
    start_time = time.time()
    while time.time() - start_time < timeout:
        status = wmill.get_job_status(job_id)
        if status == "COMPLETED":
            # Job completed - get result (may throw if failed)
            return wmill.get_result(job_id)
        time.sleep(0.5)

    raise Exception(f"Flow timed out after {timeout}s")


def handle_start_simulation_async(params: dict) -> dict:
    """Start a simulation asynchronously, return job_id immediately."""
    path = "f/infrared/simulate"
    try:
        job_id = wmill.run_script_by_path_async(path=path, args=params)
        return {
            "job_id": job_id,
            "status": "queued",
            "started_at": time.time(),
        }
    except Exception as e:
        return {
            "job_id": None,
            "status": "error",
            "error": str(e),
        }


def handle_start_report_async(params: dict) -> dict:
    """Start a report PDF asynchronously, return job_id immediately."""
    path = "f/infrared/report_pdf"
    try:
        job_id = wmill.run_script_by_path_async(path=path, args=params)
        return {
            "job_id": job_id,
            "status": "queued",
            "started_at": time.time(),
        }
    except Exception as e:
        return {
            "job_id": None,
            "status": "error",
            "error": str(e),
        }


def handle_start_enrich_async(params: dict) -> dict:
    """Start enrichment asynchronously, return job_id immediately."""
    path = "f/infrared/enrich_simulation"
    try:
        job_id = wmill.run_script_by_path_async(path=path, args=params)
        return {
            "job_id": job_id,
            "status": "queued",
            "started_at": time.time(),
        }
    except Exception as e:
        return {
            "job_id": None,
            "status": "error",
            "error": str(e),
        }


def handle_start_assemble_pdf_async(params: dict) -> dict:
    """Start PDF assembly asynchronously, return job_id immediately."""
    path = "f/infrared/assemble_pdf"
    try:
        job_id = wmill.run_script_by_path_async(path=path, args=params)
        return {
            "job_id": job_id,
            "status": "queued",
            "started_at": time.time(),
        }
    except Exception as e:
        return {
            "job_id": None,
            "status": "error",
            "error": str(e),
        }


def handle_check_job_status(params: dict) -> dict:
    """Check status of one or more job IDs."""
    job_ids = params.get("job_ids", [])

    if not job_ids:
        return {"error": "No job_ids provided", "status": "error"}

    # Handle single job_id passed as string
    if isinstance(job_ids, str):
        job_ids = [job_ids]

    if not isinstance(job_ids, list):
        return {"error": "job_ids must be a list", "status": "error"}

    if len(job_ids) > 20:
        return {"error": "Too many job_ids (max 20)", "status": "error"}

    statuses = {}
    for job_id in job_ids:
        try:
            status = wmill.get_job_status(job_id)
            # SDK returns UPPERCASE: "RUNNING", "WAITING", "COMPLETED"
            statuses[job_id] = {"status": status.lower()}
        except Exception as e:
            statuses[job_id] = {"status": "unknown", "error": str(e)}

    return {"statuses": statuses, "status": "ok"}


def handle_get_job_result(params: dict) -> dict:
    """Get result for a completed job."""
    job_id = params.get("job_id")
    if not job_id:
        return {"error": "No job_id provided", "status": "error"}

    try:
        status = wmill.get_job_status(job_id)
        # SDK returns UPPERCASE: "RUNNING", "WAITING", "COMPLETED"
        if status == "COMPLETED":
            try:
                result = wmill.get_result(job_id)
                return {"status": "completed", "result": result}
            except Exception as e:
                # get_result may throw if job failed
                return {"status": "failed", "result": None, "error": str(e)}
        elif status in ("RUNNING", "WAITING"):
            return {"status": status.lower(), "result": None}
        else:
            return {"status": status.lower() if status else "unknown", "result": None}
    except Exception as e:
        return {"status": "error", "result": None, "error": str(e)}


def main(
    action: str,
    params: dict | None = None,
) -> dict:
    """
    Gateway for environmental analysis API calls.
    Auth is handled by Windmill HTTP Route - if you reach this, you're authenticated.

    Args:
        action: One of the allowed actions (geocode, fetch_buildings, etc.)
        params: Parameters to pass to the target script/flow

    Returns:
        Result from the target script/flow or error
    """
    params = params or {}

    # Validate action
    if action not in ALLOWED_ACTIONS:
        return {
            "error": f"Unknown action: {action}",
            "status": "error",
        }

    # Handle async actions specially
    if action == "start_simulation_async":
        result = handle_start_simulation_async(params)
        return {"data": result, "action": action, "status": "ok"}

    if action == "start_report_async":
        result = handle_start_report_async(params)
        return {"data": result, "action": action, "status": "ok"}

    if action == "start_enrich_async":
        result = handle_start_enrich_async(params)
        return {"data": result, "action": action, "status": "ok"}

    if action == "start_assemble_pdf_async":
        result = handle_start_assemble_pdf_async(params)
        return {"data": result, "action": action, "status": "ok"}

    if action == "check_job_status":
        result = handle_check_job_status(params)
        return {"data": result, "action": action, "status": result.get("status", "ok")}

    if action == "get_job_result":
        result = handle_get_job_result(params)
        return {"data": result, "action": action, "status": "ok"}

    # Get target path and type for regular actions
    path, is_flow = ALLOWED_ACTIONS[action]

    try:
        # Run script or flow based on type
        if is_flow:
            result = run_flow_sync(path=path, args=params)
        else:
            result = wmill.run_script(path=path, args=params)

        return {
            "data": result,
            "action": action,
            "status": "ok",
        }

    except Exception as e:
        return {
            "error": str(e),
            "action": action,
            "status": "error",
        }
