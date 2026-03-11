"""
Generate PDF report for environmental analysis.

Uses fpdf2 (pure Python, zero system deps) to build the PDF programmatically.
When called with grids + flags, self-enriches by computing KPIs, Gemini context,
AI summary, and rendering images internally (async-friendly).
"""

#extra_requirements:
#fpdf2==2.8.3

import base64
from datetime import datetime
from typing import Optional
import wmill

from f.infrared.pdf_styles import StyledReportPDF


def _enrich_analyses(
    analyses: list,
    location_name: str,
    include_gemini_context: bool,
    include_ai_summary: bool,
    center_lat: Optional[float],
    center_lon: Optional[float],
    agent_notes: Optional[str],
    buildings: Optional[dict],
    trees: Optional[dict],
    pins: Optional[list],
) -> tuple:
    """Compute KPIs, render images, fetch Gemini context, AI summary."""

    # 1. Compute KPIs + render images for analyses that need them
    for analysis in analyses:
        grid = analysis.get("grid")
        a_type = analysis.get("analysis_type", "utci")

        if grid and not analysis.get("kpis"):
            try:
                kpis = wmill.run_script(
                    path="f/infrared/kpis",
                    args={"grid": grid, "analysis_type": a_type},
                )
                analysis["kpis"] = kpis
            except Exception as e:
                print(f"[report_pdf] KPIs failed for {analysis.get('id')}: {e}")

        if grid and not analysis.get("image_base64"):
            try:
                render = wmill.run_script(
                    path="f/geo/render_map_image",
                    args={
                        "bbox": analysis.get("bbox"),
                        "grid": grid,
                        "analysis_type": a_type,
                        "buildings": buildings,
                        "trees": trees,
                        "pins": pins or [],
                        "image_width": 1024,
                        "image_height": 1024,
                        "show_legend": True,
                        "opacity": 0.7,
                    },
                )
                if isinstance(render, dict) and render.get("image_base64"):
                    analysis["image_base64"] = render["image_base64"]
            except Exception as e:
                print(f"[report_pdf] Render failed for {analysis.get('id')}: {e}")

    # 2. Gemini context
    gemini_context = None
    if include_gemini_context and center_lat and center_lon:
        try:
            a_type = analyses[0].get("analysis_type", "utci") if analyses else "utci"
            gemini_context = wmill.run_script(
                path="f/infrared/gemini_context",
                args={
                    "location_name": location_name,
                    "latitude": center_lat,
                    "longitude": center_lon,
                    "topics": ["climate", "urban_planning", "heat_mitigation"],
                    "analysis_type": a_type,
                },
            )
        except Exception as e:
            print(f"[report_pdf] Gemini context failed: {e}")

    # 3. AI summary
    ai_summary = None
    if include_ai_summary:
        try:
            ai_summary = wmill.run_script(
                path="f/infrared/report_ai_summary",
                args={
                    "location_name": location_name,
                    "analyses": [
                        {
                            "id": a.get("id"),
                            "name": a.get("name"),
                            "analysis_type": a.get("analysis_type"),
                            "kpis": a.get("kpis"),
                            "weather": a.get("weather"),
                            "pin_results": a.get("pin_results"),
                        }
                        for a in analyses
                    ],
                    "agent_notes": agent_notes,
                },
            )
        except Exception as e:
            print(f"[report_pdf] AI summary failed: {e}")

    return gemini_context, ai_summary


def main(
    location_name: str,
    analyses: Optional[list] = None,
    bbox: Optional[dict] = None,
    analysis_type: Optional[str] = None,
    kpis: Optional[dict] = None,
    weather: Optional[dict] = None,
    buildings_count: int = 0,
    trees_count: int = 0,
    gemini_context: Optional[dict] = None,
    screenshot_base64: Optional[str] = None,
    agent_notes: Optional[str] = None,
    pin_results: Optional[list] = None,
    ai_summary: Optional[dict] = None,
    image_base64: Optional[str] = None,
    # New flags for self-enrichment (async PDF flow)
    include_gemini_context: bool = False,
    include_ai_summary: bool = False,
    center_lat: Optional[float] = None,
    center_lon: Optional[float] = None,
    buildings: Optional[dict] = None,
    trees: Optional[dict] = None,
    pins: Optional[list] = None,
) -> dict:
    try:
        # Self-enrich if flags are set (async PDF flow)
        needs_enrichment = (
            (include_gemini_context and not gemini_context)
            or (include_ai_summary and not ai_summary)
            or (analyses and any(
                a.get("grid") and (not a.get("kpis") or not a.get("image_base64"))
                for a in (analyses or [])
            ))
        )

        if needs_enrichment:
            enriched_gemini, enriched_ai = _enrich_analyses(
                analyses=analyses or [],
                location_name=location_name,
                include_gemini_context=include_gemini_context and not gemini_context,
                include_ai_summary=include_ai_summary and not ai_summary,
                center_lat=center_lat,
                center_lon=center_lon,
                agent_notes=agent_notes,
                buildings=buildings,
                trees=trees,
                pins=pins,
            )
            if enriched_gemini and not gemini_context:
                gemini_context = enriched_gemini
            if enriched_ai and not ai_summary:
                ai_summary = enriched_ai

        pdf = StyledReportPDF(location_name)
        pdf.alias_nb_pages()

        # ── Cover page ──────────────────────────────────────
        pdf.add_page()
        sim_count = len(analyses) if analyses else (1 if image_base64 else 0)
        pin_count = len(pin_results or [])
        if analyses:
            pin_count = max(pin_count, max(
                (len(a.get('pin_results', [])) for a in analyses), default=0
            ))
        pdf.cover_page(
            subtitle=datetime.now().strftime('%B %Y'),
            sim_count=sim_count,
            pin_count=pin_count,
        )

        # ── Overview page ───────────────────────────────────
        pdf.add_page()
        if weather or buildings_count or trees_count or bbox or analyses:
            pdf.section_title('Overview')
            if analysis_type:
                label = 'UTCI Thermal Comfort' if analysis_type == 'utci' else 'Wind Comfort'
                pdf.key_value('Analysis Type', label)
            elif analyses:
                pdf.key_value('Analyses', f'{len(analyses)} simulation(s)')
            if weather:
                pdf.weather_bar(weather)
            if buildings_count or trees_count:
                pdf.key_value('Site Data', f'{buildings_count} buildings, {trees_count} trees')
            pdf.ln(4)

        # ── AI Executive Summary ────────────────────────────
        if ai_summary and isinstance(ai_summary.get('summary'), dict):
            s = ai_summary['summary']
            executive = s.get('executive_summary', '')
            takeaways = s.get('overall_takeaways', [])
            if executive:
                pdf.section_title('Executive Summary')
                pdf.body_text(executive)
            if takeaways:
                for t in takeaways:
                    pdf.body_text(f'  -  {t}')
                pdf.ln(2)

        # ── Gemini context ──────────────────────────────────
        if gemini_context:
            ctx = gemini_context.get('context', {})
            texts = []
            for key in ('urban_planning', 'climate', 'heat_mitigation'):
                v = ctx.get(key, '') or gemini_context.get(key, '')
                if v:
                    texts.append((key.replace('_', ' ').title(), v))
            if texts:
                pdf.section_title('Site Context')
                for label, text in texts:
                    pdf.set_font('Helvetica', 'B', 10)
                    pdf.set_text_color(50, 50, 50)
                    pdf.cell(0, 6, label)
                    pdf.ln(5)
                    pdf.body_text(text)

        # ── Screenshot ──────────────────────────────────────
        if screenshot_base64:
            pdf.section_title('Site Overview')
            pdf.add_image_b64(screenshot_base64, max_w=160)

        # ── Analysis results ────────────────────────────────
        if analyses:
            for analysis in analyses:
                pdf.add_page()
                a_type = analysis.get('analysis_type', '')
                a_unit = '\u00b0C' if a_type == 'utci' else 'm/s'
                a_name = analysis.get('name', 'Analysis')
                a_weather = analysis.get('weather', {})
                a_kpis = analysis.get('kpis', {})
                a_pins = analysis.get('pin_results', [])
                a_img = analysis.get('image_base64') or ''

                pdf.section_title(a_name)
                if a_weather:
                    pdf.weather_bar(a_weather)

                # AI blurb in callout box
                if ai_summary and isinstance(ai_summary.get('summary'), dict):
                    for ai_a in ai_summary['summary'].get('analyses', []):
                        if ai_a.get('id') == analysis.get('id'):
                            blurb = ai_a.get('summary', '')
                            if blurb:
                                pdf.callout_box(blurb)
                            break

                # Rendered map image
                if a_img:
                    pdf.add_image_b64(a_img, max_w=170)

                # Stat cards (min/max/mean)
                if isinstance(a_kpis, dict):
                    stats = a_kpis.get('stats', {})
                    if stats:
                        pdf.stat_cards(stats, a_unit)

                # Pin results table
                if a_pins:
                    rows = []
                    for p in a_pins:
                        val = p.get('value')
                        val_s = f'{val:.1f} {a_unit}' if isinstance(val, (int, float)) else '--'
                        rad = p.get('radius_m')
                        rad_s = f'{rad}m' if isinstance(rad, (int, float)) else '--'
                        lat, lon = p.get('lat'), p.get('lon')
                        loc_s = f'{lat:.5f}, {lon:.5f}' if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) else '--'
                        rows.append([p.get('label', '?'), val_s, rad_s, loc_s])
                    pdf.styled_table(['Pin', 'Value', 'Radius', 'Location'], rows)

        elif image_base64:
            # Single-analysis fallback
            pdf.section_title('Analysis Results')
            pdf.add_image_b64(image_base64, max_w=170)
            if kpis:
                rows = []
                for k, v in kpis.items():
                    if isinstance(v, (int, float)):
                        rows.append([k.replace('_', ' ').title(), f'{v:.1f}' if isinstance(v, float) else str(v)])
                if rows:
                    pdf.styled_table(['Metric', 'Value'], rows)

        # Legacy pin results (top-level)
        if pin_results and not analyses:
            unit = '\u00b0C' if analysis_type == 'utci' else 'm/s'
            pdf.section_title('Pin Sampling Results')
            rows = []
            for p in pin_results:
                val = p.get('value')
                val_s = f'{val:.1f} {unit}' if isinstance(val, (int, float)) else '--'
                rad = p.get('radius_m')
                rad_s = f'{rad}m' if isinstance(rad, (int, float)) else '--'
                rows.append([p.get('label', '?'), val_s, rad_s])
            pdf.styled_table(['Pin', 'Value', 'Radius'], rows)

        # ── Methodology ─────────────────────────────────────
        method_lines = [
            'Analysis performed using the Infrared.city simulation platform:',
            '  -  Building geometry from OpenStreetMap and TUM LOD1 data',
            '  -  Tree locations from OSM and Overture Maps',
        ]
        types = set()
        if analyses:
            types = set(a.get('analysis_type', '') for a in analyses)
        elif analysis_type:
            types = {analysis_type}
        if 'utci' in types:
            method_lines.append('  -  UTCI thermal comfort model (ISO 7730)')
        if 'wind' in types:
            method_lines.append('  -  CFD-based wind simulation at pedestrian level (1.75m)')
        pdf.methodology_box(method_lines)
        pdf.attribution()

        # Output
        pdf_bytes = pdf.output()
        pdf_base64 = base64.b64encode(pdf_bytes).decode('utf-8')

        return {
            "pdf_base64": pdf_base64,
            "image_base64": image_base64,
            "status": "ok",
        }

    except Exception as e:
        return {
            "error": str(e),
            "status": "error",
        }
