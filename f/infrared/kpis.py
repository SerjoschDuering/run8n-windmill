"""
Windmill Script: Environmental KPIs Calculator
Path: f/infrared/kpis

Calculates thermal comfort and wind comfort KPIs from simulation grid data.

Webhook: POST https://windmill.run8n.xyz/api/w/windmill_automations/jobs/run_wait_result/f/infrared/kpis
"""

from typing import Literal, Optional
import math

# UTCI Thermal Comfort Categories (ISO 7730 / ASHRAE)
UTCI_CATEGORIES = {
    "extreme_cold": {"min": -50, "max": -40, "label": "Extreme Cold Stress", "color": "#0000FF"},
    "very_strong_cold": {"min": -40, "max": -27, "label": "Very Strong Cold Stress", "color": "#3399FF"},
    "strong_cold": {"min": -27, "max": -13, "label": "Strong Cold Stress", "color": "#66CCFF"},
    "moderate_cold": {"min": -13, "max": 0, "label": "Moderate Cold Stress", "color": "#99FFFF"},
    "slight_cold": {"min": 0, "max": 9, "label": "Slight Cold Stress", "color": "#CCFFCC"},
    "comfortable": {"min": 9, "max": 26, "label": "No Thermal Stress", "color": "#00FF00"},
    "moderate_heat": {"min": 26, "max": 32, "label": "Moderate Heat Stress", "color": "#FFFF00"},
    "strong_heat": {"min": 32, "max": 38, "label": "Strong Heat Stress", "color": "#FF9900"},
    "very_strong_heat": {"min": 38, "max": 46, "label": "Very Strong Heat Stress", "color": "#FF3300"},
    "extreme_heat": {"min": 46, "max": 60, "label": "Extreme Heat Stress", "color": "#FF0000"},
}

# Lawson Wind Comfort Criteria
WIND_CATEGORIES = {
    "calm": {"min": 0, "max": 2, "label": "Calm - Sitting", "suitable_for": "Outdoor dining, reading", "color": "#00FF00"},
    "light": {"min": 2, "max": 4, "label": "Light - Standing", "suitable_for": "Waiting, casual standing", "color": "#99FF00"},
    "moderate": {"min": 4, "max": 6, "label": "Moderate - Strolling", "suitable_for": "Leisurely walking", "color": "#FFFF00"},
    "uncomfortable": {"min": 6, "max": 8, "label": "Uncomfortable", "suitable_for": "Fast walking only", "color": "#FF9900"},
    "dangerous": {"min": 8, "max": 100, "label": "Dangerous", "suitable_for": "Avoid pedestrian use", "color": "#FF0000"},
}


def categorize_utci(value: float) -> str:
    """Return UTCI category key for a value."""
    for key, cat in UTCI_CATEGORIES.items():
        if cat["min"] <= value < cat["max"]:
            return key
    return "extreme_heat" if value >= 46 else "extreme_cold"


def categorize_wind(value: float) -> str:
    """Return wind category key for a value."""
    for key, cat in WIND_CATEGORIES.items():
        if cat["min"] <= value < cat["max"]:
            return key
    return "dangerous"


def calculate_percentiles(values: list[float], percentiles: list[int]) -> dict[str, float]:
    """Calculate percentiles for a list of values."""
    if not values:
        return {f"p{p}": 0 for p in percentiles}

    sorted_vals = sorted(values)
    n = len(sorted_vals)
    result = {}

    for p in percentiles:
        idx = (p / 100) * (n - 1)
        lower = int(idx)
        frac = idx - lower
        if lower + 1 < n:
            result[f"p{p}"] = sorted_vals[lower] * (1 - frac) + sorted_vals[lower + 1] * frac
        else:
            result[f"p{p}"] = sorted_vals[lower]

    return result


def calculate_utci_kpis(grid: list[list[float]], total_cells: int) -> dict:
    """Calculate UTCI-specific KPIs."""
    values = []
    category_counts = {key: 0 for key in UTCI_CATEGORIES}

    for row in grid:
        for val in row:
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                values.append(val)
                cat = categorize_utci(val)
                category_counts[cat] += 1

    valid_count = len(values)
    if valid_count == 0:
        return {"error": "No valid values in grid"}

    # Category percentages
    category_pcts = {
        key: round(count / valid_count * 100, 1)
        for key, count in category_counts.items()
    }

    # Comfort zone analysis
    comfortable_count = sum(
        count for key, count in category_counts.items()
        if key in ["comfortable", "slight_cold"]
    )

    heat_stress_count = sum(
        count for key, count in category_counts.items()
        if "heat" in key
    )

    cold_stress_count = sum(
        count for key, count in category_counts.items()
        if "cold" in key and key != "slight_cold"
    )

    return {
        "type": "utci",
        "unit": "°C",
        "stats": {
            "min": round(min(values), 1),
            "max": round(max(values), 1),
            "mean": round(sum(values) / len(values), 1),
            "std": round(math.sqrt(sum((v - sum(values)/len(values))**2 for v in values) / len(values)), 1),
            **calculate_percentiles(values, [5, 25, 50, 75, 95]),
        },
        "coverage": {
            "valid_cells": valid_count,
            "total_cells": total_cells,
            "coverage_pct": round(valid_count / total_cells * 100, 1),
        },
        "categories": category_pcts,
        "comfort_analysis": {
            "comfortable_pct": round(comfortable_count / valid_count * 100, 1),
            "heat_stress_pct": round(heat_stress_count / valid_count * 100, 1),
            "cold_stress_pct": round(cold_stress_count / valid_count * 100, 1),
        },
        "thresholds": {
            "hot_spots_pct": round(sum(1 for v in values if v > 32) / valid_count * 100, 1),
            "cool_zones_pct": round(sum(1 for v in values if v < 26) / valid_count * 100, 1),
        },
        "category_reference": UTCI_CATEGORIES,
    }


def calculate_wind_kpis(grid: list[list[float]], total_cells: int) -> dict:
    """Calculate wind-specific KPIs."""
    values = []
    category_counts = {key: 0 for key in WIND_CATEGORIES}

    for row in grid:
        for val in row:
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                values.append(val)
                cat = categorize_wind(val)
                category_counts[cat] += 1

    valid_count = len(values)
    if valid_count == 0:
        return {"error": "No valid values in grid"}

    # Category percentages
    category_pcts = {
        key: round(count / valid_count * 100, 1)
        for key, count in category_counts.items()
    }

    # Comfort analysis
    comfortable_count = category_counts["calm"] + category_counts["light"]
    acceptable_count = comfortable_count + category_counts["moderate"]
    problematic_count = category_counts["uncomfortable"] + category_counts["dangerous"]

    return {
        "type": "wind",
        "unit": "m/s",
        "stats": {
            "min": round(min(values), 2),
            "max": round(max(values), 2),
            "mean": round(sum(values) / len(values), 2),
            "std": round(math.sqrt(sum((v - sum(values)/len(values))**2 for v in values) / len(values)), 2),
            **calculate_percentiles(values, [5, 25, 50, 75, 95]),
        },
        "coverage": {
            "valid_cells": valid_count,
            "total_cells": total_cells,
            "coverage_pct": round(valid_count / total_cells * 100, 1),
        },
        "categories": category_pcts,
        "comfort_analysis": {
            "comfortable_pct": round(comfortable_count / valid_count * 100, 1),
            "acceptable_pct": round(acceptable_count / valid_count * 100, 1),
            "problematic_pct": round(problematic_count / valid_count * 100, 1),
        },
        "thresholds": {
            "calm_zones_pct": round(sum(1 for v in values if v < 2) / valid_count * 100, 1),
            "gusty_zones_pct": round(sum(1 for v in values if v > 6) / valid_count * 100, 1),
        },
        "category_reference": WIND_CATEGORIES,
    }


def main(
    grid: list[list[float]],
    analysis_type: Literal["utci", "wind"],
    buildings_count: Optional[int] = None,
    trees_count: Optional[int] = None,
) -> dict:
    """
    Calculate KPIs from simulation grid.

    Args:
        grid: 512x512 simulation result grid
        analysis_type: "utci" or "wind"
        buildings_count: Optional building count for context
        trees_count: Optional tree count for context

    Returns:
        Comprehensive KPIs dictionary
    """
    if not grid or not grid[0]:
        return {"error": "Empty grid provided"}

    total_cells = len(grid) * len(grid[0])

    if analysis_type == "utci":
        kpis = calculate_utci_kpis(grid, total_cells)
    else:
        kpis = calculate_wind_kpis(grid, total_cells)

    # Add context
    if buildings_count is not None or trees_count is not None:
        kpis["site_context"] = {}
        if buildings_count is not None:
            kpis["site_context"]["buildings_count"] = buildings_count
        if trees_count is not None:
            kpis["site_context"]["trees_count"] = trees_count

    return kpis
