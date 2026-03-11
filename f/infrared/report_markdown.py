"""
Windmill Script: Markdown Report Generator
Path: f/infrared/report_markdown

Generates structured Markdown reports from environmental analysis results.

Webhook: POST https://windmill.run8n.xyz/api/w/windmill_automations/jobs/run_wait_result/f/infrared/report_markdown
"""

from datetime import datetime
from typing import Optional


def format_utci_interpretation(kpis: dict) -> str:
    """Generate human-readable interpretation of UTCI results."""
    stats = kpis.get("stats", {})
    comfort = kpis.get("comfort_analysis", {})
    thresholds = kpis.get("thresholds", {})

    mean_temp = stats.get("mean", 0)
    max_temp = stats.get("max", 0)
    hot_spots = thresholds.get("hot_spots_pct", 0)

    # Determine overall assessment
    if mean_temp < 9:
        assessment = "Cold conditions predominate. Consider wind protection and solar exposure for outdoor comfort."
    elif mean_temp < 26:
        assessment = "Comfortable thermal conditions for most outdoor activities."
    elif mean_temp < 32:
        assessment = "Moderate heat stress present. Shade and cooling features recommended."
    else:
        assessment = "Significant heat stress. Limit prolonged outdoor exposure during peak hours."

    lines = [
        f"The analysis shows UTCI values ranging from **{stats.get('min', 0):.1f}°C** to **{max_temp:.1f}°C** ",
        f"with a mean of **{mean_temp:.1f}°C**.",
        "",
        f"**{comfort.get('comfortable_pct', 0):.0f}%** of the area falls within the comfortable range (9-26°C), ",
        f"while **{hot_spots:.0f}%** experiences strong heat stress (>32°C).",
        "",
        f"*Assessment:* {assessment}",
    ]

    return "\n".join(lines)


def format_wind_interpretation(kpis: dict) -> str:
    """Generate human-readable interpretation of wind results."""
    stats = kpis.get("stats", {})
    comfort = kpis.get("comfort_analysis", {})
    thresholds = kpis.get("thresholds", {})

    mean_wind = stats.get("mean", 0)
    max_wind = stats.get("max", 0)
    problematic = comfort.get("problematic_pct", 0)

    # Determine overall assessment
    if mean_wind < 2:
        assessment = "Calm conditions suitable for all outdoor activities including seated dining."
    elif mean_wind < 4:
        assessment = "Light wind conditions suitable for standing and casual activities."
    elif mean_wind < 6:
        assessment = "Moderate wind suitable for walking. May affect outdoor dining comfort."
    else:
        assessment = "Strong wind conditions. Consider wind barriers for pedestrian comfort."

    lines = [
        f"Wind speeds range from **{stats.get('min', 0):.1f} m/s** to **{max_wind:.1f} m/s** ",
        f"with a mean of **{mean_wind:.1f} m/s**.",
        "",
        f"**{comfort.get('comfortable_pct', 0):.0f}%** of the area has calm to light winds suitable for sitting/standing, ",
        f"while **{problematic:.0f}%** experiences uncomfortable conditions (>6 m/s).",
        "",
        f"*Assessment:* {assessment}",
    ]

    return "\n".join(lines)


def format_recommendations(kpis: dict, analysis_type: str) -> list[str]:
    """Generate recommendations based on analysis results."""
    recommendations = []

    if analysis_type == "utci":
        hot_spots = kpis.get("thresholds", {}).get("hot_spots_pct", 0)
        tree_coverage = kpis.get("site_context", {}).get("trees_count", 0)

        if hot_spots > 30:
            recommendations.append("**Increase shade provision** - Add trees or shade structures in exposed areas")
        if hot_spots > 20:
            recommendations.append("**Use cool materials** - Light-colored pavements can reduce surface temperatures by 5-10°C")
        if tree_coverage < 50:
            recommendations.append("**Enhance green infrastructure** - Tree canopy provides effective cooling through shade and evapotranspiration")
        recommendations.append("**Consider water features** - Fountains and misting provide localized cooling during hot periods")

    else:  # wind
        gusty = kpis.get("thresholds", {}).get("gusty_zones_pct", 0)

        if gusty > 20:
            recommendations.append("**Install wind barriers** - Hedges, screens, or building setbacks can reduce pedestrian-level wind")
        if gusty > 10:
            recommendations.append("**Review building configurations** - Tall buildings can create wind acceleration at corners")
        recommendations.append("**Design protected seating areas** - Locate outdoor dining away from wind corridors")

    return recommendations


def main(
    location_name: str,
    analyses: Optional[list] = None,
    bbox: Optional[dict] = None,
    analysis_type: Optional[str] = None,
    analysis_name: Optional[str] = None,
    kpis: Optional[dict] = None,
    weather: Optional[dict] = None,
    site_context: Optional[dict] = None,
    gemini_context: Optional[dict] = None,
    buildings_count: Optional[int] = None,
    trees_count: Optional[int] = None,
    screenshot_base64: Optional[str] = None,
    agent_notes: Optional[str] = None,
    pin_results: Optional[list] = None,
    ai_summary: Optional[dict] = None,
) -> dict:
    """
    Generate Markdown environmental analysis report.

    Args:
        location_name: Human-readable location name
        bbox: Bounding box {west, south, east, north}
        analysis_type: "utci" or "wind"
        kpis: KPIs from f/infrared/kpis
        weather: Weather parameters used
        site_context: Optional site context from gemini
        gemini_context: Optional Gemini enrichment results
        buildings_count: Number of buildings in analysis
        trees_count: Number of trees in analysis
        screenshot_base64: Optional screenshot from UI

    Returns:
        Markdown report string and metadata
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    analysis_label = "Thermal Comfort (UTCI)" if analysis_type == "utci" else "Wind Comfort"
    analysis_title = analysis_name or analysis_label

    lines = [
        f"# Environmental Analysis Report",
        f"## {location_name}",
        "",
        f"**Analysis Type:** {analysis_title}" if analysis_type else "**Analysis Type:** Multi-layer report",
        f"**Generated:** {now}",
        f"**Coordinates:** {bbox.get('south', 0):.4f}°N, {bbox.get('west', 0):.4f}°E",
        "",
    ]

    # Add map screenshot if available
    if screenshot_base64:
        # Strip data URL prefix if present
        img_data = screenshot_base64
        if screenshot_base64.startswith("data:"):
            img_data = screenshot_base64.split(",", 1)[1] if "," in screenshot_base64 else screenshot_base64
        lines.extend([
            "---",
            "",
            "## Site Overview",
            "",
            f"![Map Screenshot](data:image/png;base64,{img_data})",
            "",
            "*Map showing buildings, trees, and analysis area*",
            "",
        ])

    lines.extend([
        "---",
        "",
        "## Executive Summary",
        "",
    ])

    # AI summary if provided
    if ai_summary and ai_summary.get("summary"):
        summary = ai_summary["summary"]
        lines.extend([
            summary.get("executive_summary", ""),
            "",
        ])
        takeaways = summary.get("overall_takeaways", [])
        if takeaways:
            lines.append("### Key Takeaways")
            lines.append("")
            for i, takeaway in enumerate(takeaways, 1):
                lines.append(f"{i}. {takeaway}")
            lines.append("")

    # Single-analysis interpretation
    if analysis_type and kpis:
        if analysis_type == "utci":
            lines.append(format_utci_interpretation(kpis))
        else:
            lines.append(format_wind_interpretation(kpis))

    # Weather conditions
    if weather:
        lines.extend([
            "",
            "---",
            "",
            "## Weather Conditions",
            "",
            "| Parameter | Value |",
            "|-----------|-------|",
            f"| Temperature | {weather.get('temperature', 'N/A')}°C |",
            f"| Humidity | {weather.get('humidity', 'N/A')}% |",
            f"| Wind Speed | {weather.get('windSpeed', 'N/A')} m/s |",
            f"| Wind Direction | {weather.get('windDirection', 'N/A')}° |",
            "",
        ])

    # Site context
    if buildings_count is not None or trees_count is not None:
        lines.extend([
            "---",
            "",
            "## Site Context",
            "",
        ])
        if buildings_count is not None:
            lines.append(f"- **Buildings analyzed:** {buildings_count}")
        if trees_count is not None:
            lines.append(f"- **Trees analyzed:** {trees_count}")
        lines.append("")

    # Agent notes if provided
    if agent_notes:
        lines.extend([
            "---",
            "",
            "## Agent Notes",
            "",
            agent_notes,
            "",
        ])

    # Pin results if provided
    if pin_results:
        lines.extend([
            "---",
            "",
            "## Pin Sampling Results",
            "",
            "| Pin | Value | Radius | Location |",
            "|-----|-------|--------|----------|",
        ])
        unit = "°C" if analysis_type == "utci" else "m/s"
        for pin in pin_results:
            label = pin.get("label", "?")
            value = pin.get("value")
            radius = pin.get("radius_m")
            lat = pin.get("lat")
            lon = pin.get("lon")
            value_str = f"{value:.1f} {unit}" if isinstance(value, (int, float)) else "--"
            radius_str = f"{radius}m" if isinstance(radius, (int, float)) else "--"
            loc_str = f"{lat:.5f}, {lon:.5f}" if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) else "--"
            lines.append(f"| {label} | {value_str} | {radius_str} | {loc_str} |")
        lines.append("")
    # Gemini context if available
    if gemini_context and "context" in gemini_context:
        ctx = gemini_context["context"]
        if ctx.get("climate"):
            lines.extend([
                "### Local Climate",
                "",
                ctx["climate"],
                "",
            ])
        if ctx.get("urban_planning"):
            lines.extend([
                "### Urban Context",
                "",
                ctx["urban_planning"],
                "",
            ])

    # Multi-analysis sections
    if analyses:
        lines.extend([
            "---",
            "",
            "## Analysis Results",
            "",
        ])
        # Build lookup for per-analysis AI blurbs
        ai_blurbs = {}
        if ai_summary and isinstance(ai_summary.get("summary"), dict):
            for ai_a in ai_summary["summary"].get("analyses", []):
                ai_blurbs[ai_a.get("id", "")] = ai_a.get("summary", "")

        for analysis in analyses:
            a_type = analysis.get("analysis_type")
            a_name = analysis.get("name", "Analysis")
            a_id = analysis.get("id", "")
            a_kpis = analysis.get("kpis", {})
            a_stats = (a_kpis or {}).get("stats", {})
            a_unit = "°C" if a_type == "utci" else "m/s"
            lines.extend([
                f"### {a_name}",
                "",
                f"**Type:** {a_type}",
                "",
            ])
            a_blurb = ai_blurbs.get(a_id, "")
            if a_blurb:
                lines.extend([
                    f"*{a_blurb}*",
                    "",
                ])
            lines.extend([
                "| Metric | Value |",
                "|--------|-------|",
                f"| Minimum | {a_stats.get('min', 0):.1f} {a_unit} |",
                f"| Maximum | {a_stats.get('max', 0):.1f} {a_unit} |",
                f"| Mean | {a_stats.get('mean', 0):.1f} {a_unit} |",
                "",
            ])
            a_pins = analysis.get("pin_results") or []
            if a_pins:
                lines.extend([
                    "Pin results:",
                    "",
                    "| Pin | Value | Radius | Location |",
                    "|-----|-------|--------|----------|",
                ])
                for pin in a_pins:
                    label = pin.get("label", "?")
                    value = pin.get("value")
                    radius = pin.get("radius_m")
                    lat = pin.get("lat")
                    lon = pin.get("lon")
                    value_str = f"{value:.1f} {a_unit}" if isinstance(value, (int, float)) else "--"
                    radius_str = f"{radius}m" if isinstance(radius, (int, float)) else "--"
                    loc_str = f"{lat:.5f}, {lon:.5f}" if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) else "--"
                    lines.append(f"| {label} | {value_str} | {radius_str} | {loc_str} |")
                lines.append("")
    elif kpis and analysis_type:
        stats = kpis.get("stats", {})
        unit = "°C" if analysis_type == "utci" else "m/s"
        lines.extend([
            "---",
            "",
            "## Analysis Results",
            "",
            "### Statistics",
            "",
            "| Metric | Value |",
            "|--------|-------|",
            f"| Minimum | {stats.get('min', 0):.1f} {unit} |",
            f"| Maximum | {stats.get('max', 0):.1f} {unit} |",
            f"| Mean | {stats.get('mean', 0):.1f} {unit} |",
            f"| Std Dev | {stats.get('std', 0):.1f} {unit} |",
            f"| Median (P50) | {stats.get('p50', 0):.1f} {unit} |",
            "",
        ])

    # Category distribution
    categories = kpis.get("categories", {}) if kpis else {}
    if categories:
        lines.extend([
            "### Category Distribution",
            "",
            "| Category | Area % |",
            "|----------|--------|",
        ])
        for cat, pct in categories.items():
            if pct > 0:
                label = cat.replace("_", " ").title()
                lines.append(f"| {label} | {pct:.1f}% |")
        lines.append("")

    # Recommendations
    recommendations = format_recommendations(kpis, analysis_type) if kpis and analysis_type else []
    if recommendations:
        lines.extend([
            "---",
            "",
            "## Recommendations",
            "",
        ])
        for i, rec in enumerate(recommendations, 1):
            lines.append(f"{i}. {rec}")
        lines.append("")

    # Methodology
    lines.extend([
        "---",
        "",
        "## Methodology",
        "",
    ])

    if analysis_type == "utci":
        lines.extend([
            "This analysis uses the **Universal Thermal Climate Index (UTCI)**, which combines:",
            "- Air temperature",
            "- Relative humidity",
            "- Wind speed",
            "- Mean radiant temperature (from solar radiation)",
            "",
            "UTCI provides a single temperature-equivalent value representing the thermal stress experienced by a person.",
            "",
        ])
    else:
        lines.extend([
            "This analysis uses **Computational Fluid Dynamics (CFD)** to model wind flow around buildings.",
            "",
            "Wind comfort is assessed using the **Lawson criteria**, which categorizes wind speeds by suitability for different activities.",
            "",
        ])

    lines.extend([
        "### Data Sources",
        "",
        "- Building footprints: OpenStreetMap, TUM, Overture Maps",
        "- Tree locations: OpenStreetMap, OpenTrees",
        "- Simulation: Infrared.city API",
        "",
        "### Limitations",
        "",
        "- Grid resolution: 1m (512×512m tile)",
        "- Building heights may be estimated where measured data unavailable",
        "- Tree canopy modeled as simplified 3D volumes",
        "",
    ])

    markdown = "\n".join(lines)

    return {
        "markdown": markdown,
        "title": f"Environmental Analysis: {location_name}",
        "analysis_type": analysis_type,
        "generated_at": now,
        "word_count": len(markdown.split()),
        "status": "ok",
    }
