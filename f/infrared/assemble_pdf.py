"""
Windmill Script: Fast PDF Assembly
Path: f/infrared/assemble_pdf

Receives ALL pre-computed data and assembles a PDF report.
Steps:
  1. Re-render images WITH pins if pins exist (parallel), else use S3 URLs
  2. ONE Gemini call for executive summary + per-analysis blurbs
  3. Assemble PDF via StyledReportPDF
  4. Upload PDF to S3, return download_url + pdf_base64
"""

#extra_requirements:
#fpdf2==2.8.3
#requests==2.31.0

import base64
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import requests
import wmill

from f.infrared.pdf_styles import StyledReportPDF


ALLOWED_IMAGE_DOMAINS = {"f003.backblazeb2.com"}


def _download_image(url: str) -> Optional[str]:
    """Download image from S3 URL and return base64. Only allows trusted domains."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        if parsed.hostname not in ALLOWED_IMAGE_DOMAINS:
            print(f"[assemble_pdf] Blocked download from untrusted domain: {parsed.hostname}")
            return None
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return base64.b64encode(resp.content).decode("utf-8")
    except Exception as e:
        print(f"[assemble_pdf] Download failed for {url}: {e}")
    return None


def _render_and_upload(analysis: dict, batch_prefix: str = "renders/report") -> tuple:
    """Render image + compute KPIs inline for an analysis missing enrichment data."""
    aid = analysis.get("id", "unknown")
    grid = analysis.get("grid")
    bbox = analysis.get("bbox")
    a_type = analysis.get("analysis_type", "utci")
    buildings = analysis.get("buildings")
    trees = analysis.get("trees")
    kpis = analysis.get("kpis")
    image_b64 = None

    # Compute KPIs if missing
    if not kpis and grid:
        try:
            kpis = wmill.run_script(
                path="f/infrared/kpis",
                args={"grid": grid, "analysis_type": a_type},
            )
        except Exception as e:
            print(f"[assemble_pdf] Inline KPI computation failed for {aid}: {e}")

    # Render image if missing
    if grid and bbox:
        try:
            render = wmill.run_script(
                path="f/geo/render_map_image",
                args={
                    "bbox": bbox,
                    "grid": grid,
                    "analysis_type": a_type,
                    "buildings": buildings,
                    "trees": trees,
                    "pins": [],
                    "image_width": 1024,
                    "image_height": 1024,
                    "show_legend": True,
                    "opacity": 0.7,
                },
            )
            if isinstance(render, dict) and render.get("image_base64"):
                image_b64 = render["image_base64"]
        except Exception as e:
            print(f"[assemble_pdf] Inline render failed for {aid}: {e}")

    return aid, kpis, image_b64


def _get_images(analyses) -> dict:
    """Download pre-rendered images from S3, or render inline if missing."""
    images = {}
    if not analyses:
        return images

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {}
        render_futures = {}
        for a in analyses:
            aid = a.get("id", "")
            if a.get("image_url"):
                fut = executor.submit(_download_image, a["image_url"])
                futures[fut] = aid
            elif a.get("image_base64"):
                images[aid] = a["image_base64"]
            elif a.get("grid"):
                # No enrichment data — render inline
                fut = executor.submit(_render_and_upload, a)
                render_futures[fut] = aid

        for fut in as_completed(futures):
            aid = futures[fut]
            try:
                result = fut.result()
                if result:
                    images[aid] = result
            except Exception as e:
                print(f"[assemble_pdf] Image download failed for {aid}: {e}")

        for fut in as_completed(render_futures):
            aid = render_futures[fut]
            try:
                _, kpis, image_b64 = fut.result()
                if image_b64:
                    images[aid] = image_b64
                # Patch KPIs back into the analysis dict
                if kpis:
                    for a in analyses:
                        if a.get("id") == aid and not a.get("kpis"):
                            a["kpis"] = kpis
            except Exception as e:
                print(f"[assemble_pdf] Inline render failed for {aid}: {e}")

    return images


def _call_gemini(analyses, agent_notes, location_name) -> Optional[dict]:
    """One Gemini call for executive summary + per-analysis blurbs."""
    try:
        # Build compact analysis summaries for Gemini
        analysis_data = []
        for a in analyses:
            entry = {
                "name": a.get("name", "Analysis"),
                "type": a.get("analysis_type", "utci"),
                "weather": a.get("weather", {}),
                "pin_results": a.get("pin_results", []),
            }
            kpis = a.get("kpis")
            if isinstance(kpis, dict):
                entry["stats"] = kpis.get("stats", {})
                entry["comfort"] = kpis.get("comfort_analysis", {})
                entry["categories"] = kpis.get("categories", {})
            entry["buildings_count"] = a.get("buildings_count", 0)
            entry["trees_count"] = a.get("trees_count", 0)
            analysis_data.append(entry)

        ai_summary = wmill.run_script(
            path="f/infrared/report_ai_summary",
            args={
                "location_name": location_name,
                "analyses": analysis_data,
                "agent_notes": agent_notes,
            },
        )
        return ai_summary
    except Exception as e:
        print(f"[assemble_pdf] Gemini call failed: {e}")
        return None


def _build_pdf(
    location_name: str,
    analyses: list,
    images: dict,
    ai_summary: Optional[dict],
    pins: list,
) -> bytes:
    """Assemble PDF from pre-computed data."""
    pdf = StyledReportPDF(location_name)
    pdf.alias_nb_pages()

    # Cover page
    pdf.add_page()
    sim_count = len(analyses)
    pin_count = len(pins)
    pdf.cover_page(
        subtitle=datetime.now().strftime("%B %Y"),
        sim_count=sim_count,
        pin_count=pin_count,
    )

    # Overview page
    pdf.add_page()
    if analyses:
        pdf.section_title("Overview")
        pdf.key_value("Analyses", f"{len(analyses)} simulation(s)")
        first_weather = analyses[0].get("weather")
        if first_weather:
            pdf.weather_bar(first_weather)
        bc = analyses[0].get("buildings_count", 0)
        tc = analyses[0].get("trees_count", 0)
        if bc or tc:
            pdf.key_value("Site Data", f"{bc} buildings, {tc} trees")
        pdf.ln(4)

    # AI executive summary
    if ai_summary and isinstance(ai_summary.get("summary"), dict):
        s = ai_summary["summary"]
        executive = s.get("executive_summary", "")
        takeaways = s.get("overall_takeaways", [])
        if executive:
            pdf.section_title("Executive Summary")
            pdf.body_text(executive)
        if takeaways:
            for t in takeaways:
                pdf.body_text(f"  -  {t}")
            pdf.ln(2)

    # Per-analysis pages
    for analysis in analyses:
        pdf.add_page()
        a_type = analysis.get("analysis_type", "")
        a_unit = "\u00b0C" if a_type == "utci" else "m/s"
        a_name = analysis.get("name", "Analysis")
        a_weather = analysis.get("weather", {})
        a_kpis = analysis.get("kpis", {})
        a_pins = analysis.get("pin_results", [])
        a_id = analysis.get("id", "")

        pdf.section_title(a_name)
        if a_weather:
            pdf.weather_bar(a_weather)

        # AI blurb
        if ai_summary and isinstance(ai_summary.get("summary"), dict):
            for ai_a in ai_summary["summary"].get("analyses", []):
                if ai_a.get("id") == a_id or ai_a.get("name") == a_name:
                    blurb = ai_a.get("summary", "")
                    if blurb:
                        pdf.callout_box(blurb)
                    break

        # Map image
        img_b64 = images.get(a_id)
        if img_b64:
            pdf.add_image_b64(img_b64, max_w=170)

        # Stat cards
        if isinstance(a_kpis, dict):
            stats = a_kpis.get("stats", {})
            if stats:
                pdf.stat_cards(stats, a_unit)

        # Pin results table
        if a_pins:
            rows = []
            for p in a_pins:
                val = p.get("value")
                val_s = f"{val:.1f} {a_unit}" if isinstance(val, (int, float)) else "--"
                rad = p.get("radius_m")
                rad_s = f"{rad}m" if isinstance(rad, (int, float)) else "--"
                lat, lon = p.get("lat"), p.get("lon")
                loc_s = (
                    f"{lat:.5f}, {lon:.5f}"
                    if isinstance(lat, (int, float)) and isinstance(lon, (int, float))
                    else "--"
                )
                rows.append([p.get("label", "?"), val_s, rad_s, loc_s])
            pdf.styled_table(["Pin", "Value", "Radius", "Location"], rows)

    # Methodology
    method_lines = [
        "Analysis performed using the Infrared.city simulation platform:",
        "  -  Building geometry from OpenStreetMap and TUM LOD1 data",
        "  -  Tree locations from OSM and Overture Maps",
    ]
    types = set(a.get("analysis_type", "") for a in analyses)
    if "utci" in types:
        method_lines.append("  -  UTCI thermal comfort model (ISO 7730)")
    if "wind" in types:
        method_lines.append(
            "  -  CFD-based wind simulation at pedestrian level (1.75m)"
        )
    pdf.methodology_box(method_lines)
    pdf.attribution()

    return pdf.output()


def main(
    location_name: str,
    analyses: list,
    agent_notes: Optional[str] = None,
    pins: Optional[list] = None,
    center_lat: Optional[float] = None,
    center_lon: Optional[float] = None,
    buildings: Optional[dict] = None,
    trees: Optional[dict] = None,
) -> dict:
    """
    Fast PDF assembly from pre-computed data.

    All KPIs and images are pre-computed by enrichment.
    This script only downloads S3 images, calls Gemini once, and assembles the PDF.

    Returns: { pdf_base64, download_url, status, timing_ms }
    """
    t0 = time.time()

    try:
        # Steps 1 + 2 in parallel (S3 image downloads + Gemini summary)
        # KPIs and images are pre-computed by enrichment -- no grids needed here
        images = {}
        ai_summary = None

        with ThreadPoolExecutor(max_workers=2) as executor:
            img_future = executor.submit(_get_images, analyses)
            gemini_future = executor.submit(
                _call_gemini, analyses, agent_notes, location_name
            )

            images = img_future.result()
            ai_summary = gemini_future.result()

        # Step 3: Assemble PDF
        pdf_bytes = _build_pdf(
            location_name, analyses, images, ai_summary, pins or []
        )
        pdf_base64 = base64.b64encode(pdf_bytes).decode("utf-8")

        # Step 4: Upload to S3
        download_url = None
        try:
            upload = wmill.run_script(
                path="f/storage/upload_screenshot",
                args={
                    "screenshot_base64": pdf_base64,
                    "prefix": "reports",
                    "content_type": "application/pdf",
                    "filename": f"env-report-{int(time.time())}.pdf",
                },
            )
            if isinstance(upload, dict) and upload.get("url"):
                download_url = upload["url"]
        except Exception as e:
            print(f"[assemble_pdf] S3 upload failed: {e}")

        elapsed = int((time.time() - t0) * 1000)
        return {
            "pdf_base64": pdf_base64,
            "download_url": download_url,
            "status": "ok",
            "timing_ms": elapsed,
        }

    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        return {
            "error": str(e),
            "status": "error",
            "timing_ms": elapsed,
        }
