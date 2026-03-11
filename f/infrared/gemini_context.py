"""
Windmill Script: Gemini Site Context Enrichment
Path: f/infrared/gemini_context

Uses Gemini API with grounding (web search) to enrich analysis with local context.

Webhook: POST https://windmill.run8n.xyz/api/w/windmill_automations/jobs/run_wait_result/f/infrared/gemini_context
"""

import wmill
import requests
from typing import Optional

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash:generateContent"


def get_api_key() -> str:
    """Get Gemini API key from Windmill resource."""
    try:
        resource = wmill.get_resource("f/cardscan/gemini_api")
        return resource.get("api_key", "")
    except Exception:
        import os
        return os.environ.get("GEMINI_API_KEY", "")


def query_gemini(prompt: str, api_key: str, use_grounding: bool = True, image_base64: Optional[str] = None) -> str:
    """Query Gemini API with optional web grounding and image input."""
    parts = []

    # Add image if provided (for multimodal analysis)
    if image_base64:
        # Strip data URL prefix if present
        if image_base64.startswith("data:"):
            image_base64 = image_base64.split(",", 1)[1]
        parts.append({
            "inline_data": {
                "mime_type": "image/png",
                "data": image_base64
            }
        })

    parts.append({"text": prompt})

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "temperature": 0.7,
            "maxOutputTokens": 1024,
        },
    }

    if use_grounding and not image_base64:
        # Grounding doesn't work well with images, use it only for text queries
        payload["tools"] = [{"google_search": {}}]

    response = requests.post(
        f"{GEMINI_API_URL}?key={api_key}",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=60,  # Longer timeout for image analysis
    )

    if response.status_code != 200:
        return f"API error: {response.status_code}"

    result = response.json()
    try:
        return result["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        return "No response generated"


def main(
    location_name: str,
    latitude: float,
    longitude: float,
    topics: Optional[list[str]] = None,
    screenshot_base64: Optional[str] = None,
    analysis_type: Optional[str] = None,
) -> dict:
    """
    Get site context using Gemini with web search and optional screenshot analysis.

    Args:
        location_name: Human-readable location name (e.g., "Munich Marienplatz")
        latitude: Site latitude
        longitude: Site longitude
        topics: Optional list of topics to research (default: climate, urban, heat)
        screenshot_base64: Optional screenshot from UI for visual analysis
        analysis_type: 'utci' or 'wind' - affects visual analysis prompts

    Returns:
        Context information from web search and visual analysis
    """
    api_key = get_api_key()
    if not api_key:
        return {"error": "Missing Gemini API key", "status": "error"}

    if topics is None:
        topics = ["climate", "urban_planning", "heat_mitigation"]

    results = {}

    prompts = {
        "climate": f"""What is the typical summer and winter climate in {location_name}?
Include: average temperatures, humidity levels, prevailing wind patterns.
Keep response under 150 words, focus on facts relevant for outdoor thermal comfort.""",

        "urban_planning": f"""What are the key urban planning characteristics of {location_name}?
Include: building density, green spaces, pedestrian areas.
Keep response under 150 words, focus on factors affecting microclimate.""",

        "heat_mitigation": f"""What heat island mitigation or climate adaptation initiatives exist in or near {location_name}?
Include: green infrastructure, cool pavements, urban greening projects.
Keep response under 150 words.""",

        "historical_context": f"""What is the historical significance and typical land use of {location_name}?
Keep response under 100 words.""",
    }

    # Run text-based queries with web grounding
    for topic in topics:
        if topic in prompts:
            try:
                response = query_gemini(prompts[topic], api_key, use_grounding=True)
                results[topic] = response
            except Exception as e:
                results[topic] = f"Error: {str(e)}"

    # Visual analysis of screenshot if provided
    visual_analysis = None
    if screenshot_base64:
        try:
            analysis_label = "thermal comfort (UTCI)" if analysis_type == "utci" else "wind comfort"
            visual_prompt = f"""Analyze this environmental analysis map of {location_name}.

The map shows a {analysis_label} simulation overlaid on buildings (gray 3D blocks) and trees (green dots).
The heatmap colors indicate comfort levels - for {'UTCI: blue=cold stress, green=comfortable, orange/red=heat stress' if analysis_type == 'utci' else 'wind: green=calm, orange=moderate, red=uncomfortable/dangerous'}.

Please provide:
1. A brief description of what the map shows (building layout, green spaces, analysis coverage)
2. Key observations about the {analysis_label} patterns visible
3. Areas of concern (hot spots or high wind zones)
4. Suggestions for improvement based on the visible urban form

Keep response under 250 words, focus on actionable insights."""

            visual_analysis = query_gemini(visual_prompt, api_key, use_grounding=False, image_base64=screenshot_base64)
            results["visual_analysis"] = visual_analysis
        except Exception as e:
            results["visual_analysis"] = f"Error analyzing screenshot: {str(e)}"

    return {
        "location": location_name,
        "coordinates": {"latitude": latitude, "longitude": longitude},
        "context": results,
        "topics_searched": topics,
        "has_visual_analysis": visual_analysis is not None,
        "status": "ok",
    }
