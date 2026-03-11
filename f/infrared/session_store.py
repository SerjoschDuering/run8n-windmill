"""
Windmill Script: Session Store for Batch Data
Path: f/infrared/session_store

CRUD for batch session state using Windmill internal state.
Stores KPIs, image URLs, pins, stats per batch.

Actions: save_batch, get_batch, update_sim, update_pins
"""

import json
from typing import Optional, Literal
import wmill


def _state_key(batch_id: str) -> str:
    return f"env_batch_{batch_id}"


def _load(batch_id: str) -> dict:
    """Load batch state from Windmill state store."""
    key = _state_key(batch_id)
    try:
        state = wmill.get_state() or {}
        return state.get(key, {})
    except Exception:
        return {}


def _save(batch_id: str, data: dict) -> None:
    """Save batch state to Windmill state store."""
    key = _state_key(batch_id)
    try:
        state = wmill.get_state() or {}
    except Exception:
        state = {}
    state[key] = data
    wmill.set_state(state)


def handle_save_batch(params: dict) -> dict:
    """Create/overwrite a full batch entry."""
    batch_id = params["batch_id"]
    data = {
        "batch_id": batch_id,
        "location_name": params.get("location_name", ""),
        "bbox": params.get("bbox"),
        "simulations": params.get("simulations", {}),
        "pins": params.get("pins", []),
    }
    _save(batch_id, data)
    return {"status": "ok", "batch_id": batch_id}


def handle_get_batch(params: dict) -> dict:
    """Retrieve batch state."""
    batch_id = params["batch_id"]
    data = _load(batch_id)
    if not data:
        return {"status": "not_found", "batch_id": batch_id}
    return {"status": "ok", **data}


def handle_update_sim(params: dict) -> dict:
    """Update a single simulation entry within a batch."""
    batch_id = params["batch_id"]
    job_id = params["job_id"]
    data = _load(batch_id)
    if not data:
        data = {"batch_id": batch_id, "simulations": {}, "pins": []}
    sims = data.get("simulations", {})
    existing = sims.get(job_id, {})
    # Merge new fields into existing sim entry
    for key in ("name", "analysis_type", "kpis", "image_url",
                "stats", "weather", "status"):
        if key in params:
            existing[key] = params[key]
    sims[job_id] = existing
    data["simulations"] = sims
    _save(batch_id, data)
    return {"status": "ok", "job_id": job_id}


def handle_update_pins(params: dict) -> dict:
    """Update pins for a batch."""
    batch_id = params["batch_id"]
    data = _load(batch_id)
    if not data:
        data = {"batch_id": batch_id, "simulations": {}, "pins": []}
    data["pins"] = params.get("pins", [])
    _save(batch_id, data)
    return {"status": "ok", "pin_count": len(data["pins"])}


ACTION_HANDLERS = {
    "save_batch": handle_save_batch,
    "get_batch": handle_get_batch,
    "update_sim": handle_update_sim,
    "update_pins": handle_update_pins,
}


def main(
    action: Literal["save_batch", "get_batch", "update_sim", "update_pins"],
    batch_id: Optional[str] = None,
    **kwargs,
) -> dict:
    """
    Session state CRUD for batch data.

    Args:
        action: One of save_batch, get_batch, update_sim, update_pins
        batch_id: Batch identifier
        **kwargs: Action-specific params (job_id, pins, kpis, etc.)
    """
    if action not in ACTION_HANDLERS:
        return {"status": "error", "error": f"Unknown action: {action}"}

    params = {"batch_id": batch_id, **kwargs}
    if not batch_id:
        return {"status": "error", "error": "batch_id is required"}

    try:
        return ACTION_HANDLERS[action](params)
    except Exception as e:
        return {"status": "error", "error": str(e)}
