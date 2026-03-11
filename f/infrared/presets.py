"""
Windmill Script: Weather Presets for Infrared Analysis
Path: f/infrared/presets

Returns standardized weather presets for UTCI and wind simulations.

Webhook: POST https://windmill.run8n.xyz/api/w/windmill_automations/jobs/run_wait_result/f/infrared/presets
"""

from typing import Optional

# Weather preset definitions
PRESETS = {
    "summer_day": {
        "id": "summer_day",
        "name": "Summer Day",
        "description": "Typical summer afternoon conditions",
        "temperature": 28,
        "humidity": 50,
        "wind_speed": 2.5,
        "wind_direction": 225,
        "month": 7,
        "hour": 14,
        "analysis_types": ["utci", "wind"],
    },
    "summer_peak": {
        "id": "summer_peak",
        "name": "Hot Summer",
        "description": "Heat wave conditions - thermal stress assessment",
        "temperature": 35,
        "humidity": 40,
        "wind_speed": 2.0,
        "wind_direction": 180,
        "month": 8,
        "hour": 15,
        "analysis_types": ["utci"],
    },
    "winter_day": {
        "id": "winter_day",
        "name": "Winter Day",
        "description": "Cold winter afternoon conditions",
        "temperature": 5,
        "humidity": 70,
        "wind_speed": 4.0,
        "wind_direction": 45,
        "month": 1,
        "hour": 14,
        "analysis_types": ["utci", "wind"],
    },
    "spring_evening": {
        "id": "spring_evening",
        "name": "Spring Evening",
        "description": "Pleasant spring evening conditions",
        "temperature": 15,
        "humidity": 60,
        "wind_speed": 3.0,
        "wind_direction": 180,
        "month": 4,
        "hour": 18,
        "analysis_types": ["utci"],
    },
    "windy": {
        "id": "windy",
        "name": "Windy Day",
        "description": "Strong wind conditions for pedestrian comfort assessment",
        "temperature": 20,
        "humidity": 50,
        "wind_speed": 8.0,
        "wind_direction": 270,
        "month": 10,
        "hour": 12,
        "analysis_types": ["wind"],
    },
    "calm_morning": {
        "id": "calm_morning",
        "name": "Calm Morning",
        "description": "Still morning with minimal wind",
        "temperature": 18,
        "humidity": 65,
        "wind_speed": 1.0,
        "wind_direction": 90,
        "month": 6,
        "hour": 9,
        "analysis_types": ["utci", "wind"],
    },
}


def main(preset_id: Optional[str] = None) -> dict:
    """
    Get weather presets for environmental analysis.

    Args:
        preset_id: Optional specific preset to return. If None, returns all presets.

    Returns:
        Single preset dict or dict of all presets
    """
    if preset_id:
        if preset_id in PRESETS:
            return PRESETS[preset_id]
        else:
            return {"error": f"Unknown preset: {preset_id}", "available": list(PRESETS.keys())}

    return {
        "presets": list(PRESETS.values()),
        "default": "summer_day",
    }
