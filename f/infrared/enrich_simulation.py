"""
Windmill Script: Enrich Simulation Result
Path: f/infrared/enrich_simulation

Fires after a simulation completes. Computes KPIs, renders hi-res image
(no pins -- pins are added at PDF time), uploads to S3, and stores
enrichment data in the session store.

Steps:
  1. KPIs via f/infrared/kpis
  2. Render 1024px image via f/geo/render_map_image (no pins)
  3. Upload image to S3 via f/storage/upload_screenshot
  4. Save to session store via f/infrared/session_store
"""

from typing import Optional
import wmill


def main(
    batch_id: str,
    job_id: str,
    grid: list,
    bbox: dict,
    analysis_type: str,
    buildings: Optional[dict] = None,
    trees: Optional[dict] = None,
    name: Optional[str] = None,
    weather: Optional[dict] = None,
) -> dict:
    """
    Enrich a single simulation result with KPIs and rendered image.

    Returns: { kpis, image_url, status }
    """
    result = {"kpis": None, "image_url": None, "status": "ok"}

    # 1. Compute KPIs
    try:
        kpis = wmill.run_script(
            path="f/infrared/kpis",
            args={"grid": grid, "analysis_type": analysis_type},
        )
        result["kpis"] = kpis
    except Exception as e:
        print(f"[enrich] KPIs failed for {job_id}: {e}")

    # 2. Render hi-res image (NO pins -- those are added at PDF assembly time)
    image_base64 = None
    try:
        render = wmill.run_script(
            path="f/geo/render_map_image",
            args={
                "bbox": bbox,
                "grid": grid,
                "analysis_type": analysis_type,
                "buildings": buildings,
                "trees": trees,
                "pins": [],  # No pins in pre-render
                "image_width": 1024,
                "image_height": 1024,
                "show_legend": True,
                "opacity": 0.7,
            },
        )
        if isinstance(render, dict) and render.get("image_base64"):
            image_base64 = render["image_base64"]
    except Exception as e:
        print(f"[enrich] Render failed for {job_id}: {e}")

    # 3. Upload to S3
    if image_base64:
        try:
            upload = wmill.run_script(
                path="f/storage/upload_screenshot",
                args={
                    "screenshot_base64": image_base64,
                    "prefix": f"renders/{batch_id}",
                    "filename": f"{job_id}.png",
                    "content_type": "image/png",
                },
            )
            if isinstance(upload, dict) and upload.get("url"):
                result["image_url"] = upload["url"]
        except Exception as e:
            print(f"[enrich] S3 upload failed for {job_id}: {e}")

    # 4. Save to session store
    try:
        stats = None
        if result["kpis"] and isinstance(result["kpis"], dict):
            s = result["kpis"].get("stats", {})
            stats = {
                "min": s.get("min", 0),
                "max": s.get("max", 0),
                "mean": s.get("mean", 0),
            }

        wmill.run_script(
            path="f/infrared/session_store",
            args={
                "action": "update_sim",
                "batch_id": batch_id,
                "job_id": job_id,
                "name": name or f"{analysis_type.upper()} Analysis",
                "analysis_type": analysis_type,
                "kpis": result["kpis"],
                "image_url": result["image_url"],
                "stats": stats,
                "weather": weather,
                "status": "enriched",
            },
        )
    except Exception as e:
        print(f"[enrich] Session store update failed for {job_id}: {e}")

    return result
