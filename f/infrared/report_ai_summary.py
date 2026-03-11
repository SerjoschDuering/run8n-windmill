"""
Windmill Script: AI Summary for Environmental Reports
Path: f/infrared/report_ai_summary

Generates structured JSON summaries for executive overview and per-analysis blurbs.
"""

import json
import requests
import wmill
from typing import Optional

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent"


def get_api_key() -> str:
    """Get Gemini API key from Windmill resource."""
    try:
        resource = wmill.get_resource("f/cardscan/gemini_api")
        return resource.get("api_key", "")
    except Exception:
        import os
        return os.environ.get("GEMINI_API_KEY", "")


def main(
    location_name: str,
    analyses: list,
    agent_notes: Optional[str] = None,
) -> dict:
    """
    Generate a JSON-only summary for the report.

    Args:
        location_name: Human-readable location
        analyses: Array of analysis objects (kpis, weather, pins)
        agent_notes: Optional context/instructions
    """
    api_key = get_api_key()
    if not api_key:
        return {"error": "Missing Gemini API key", "status": "error"}

    schema_hint = {
        "executive_summary": "string",
        "overall_takeaways": ["string"],
        "analyses": [
            {
                "id": "string",
                "name": "string",
                "analysis_type": "utci|wind",
                "summary": "string"
            }
        ]
    }

    agent_guidance = ""
    if agent_notes:
        agent_guidance = (
            "\n\nIMPORTANT — Agent guidance (use these notes to shape your "
            "writing tone, focus areas, and recommendations. Incorporate "
            "the intent naturally into your summary — do NOT quote or "
            "reproduce these notes verbatim):\n"
            f"{agent_notes}\n"
        )

    prompt = (
        "You are an expert environmental analyst writing a professional report. "
        "Return ONLY valid JSON. No markdown, no prose outside JSON. "
        "Use this schema exactly:\n"
        + json.dumps(schema_hint, indent=2)
        + "\n\n"
        "Write a concise executive summary (2-3 paragraphs) and 3-6 overall "
        "takeaways as actionable bullet points. "
        "For each analysis, write a 2-3 sentence interpretation of the data.\n\n"
        f"Location: {location_name}\n"
        + agent_guidance
        + "Analyses JSON:\n"
        + json.dumps(analyses, indent=2)
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 10000,
        },
    }

    response = requests.post(
        f"{GEMINI_API_URL}?key={api_key}",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=60,
    )

    if response.status_code != 200:
        return {
            "error": f"API error: {response.status_code}",
            "status": "error"
        }

    result = response.json()
    try:
        text = result["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        return {"error": "No response generated", "status": "error"}

    # Strip markdown code fences that LLMs often wrap around JSON
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    try:
        parsed = json.loads(text)
        return {"status": "ok", "summary": parsed}
    except Exception:
        return {"status": "error", "error": "Invalid JSON from model", "raw": text}
