"""
Geocode location string to coordinates using Nominatim.

Returns coordinates, display name, and suggested zoom based on place type.
Includes city lookup table for instant results on major cities.
"""

import requests
import time
from typing import Optional

# Major cities lookup for instant response (no API call needed)
CITY_LOOKUP = {
    # Germany
    "berlin": {"lat": 52.520, "lon": 13.405, "display_name": "Berlin, Germany", "zoom": 12},
    "hamburg": {"lat": 53.551, "lon": 9.993, "display_name": "Hamburg, Germany", "zoom": 12},
    "munich": {"lat": 48.137, "lon": 11.576, "display_name": "Munich, Germany", "zoom": 12},
    "cologne": {"lat": 50.938, "lon": 6.960, "display_name": "Cologne, Germany", "zoom": 13},
    "frankfurt": {"lat": 50.110, "lon": 8.682, "display_name": "Frankfurt, Germany", "zoom": 13},
    "stuttgart": {"lat": 48.776, "lon": 9.183, "display_name": "Stuttgart, Germany", "zoom": 13},
    "dusseldorf": {"lat": 51.225, "lon": 6.776, "display_name": "Dusseldorf, Germany", "zoom": 13},
    "dortmund": {"lat": 51.514, "lon": 7.468, "display_name": "Dortmund, Germany", "zoom": 13},
    "essen": {"lat": 51.455, "lon": 7.012, "display_name": "Essen, Germany", "zoom": 13},
    "leipzig": {"lat": 51.340, "lon": 12.375, "display_name": "Leipzig, Germany", "zoom": 13},
    "bremen": {"lat": 53.079, "lon": 8.802, "display_name": "Bremen, Germany", "zoom": 13},
    "dresden": {"lat": 51.051, "lon": 13.738, "display_name": "Dresden, Germany", "zoom": 13},
    "hannover": {"lat": 52.376, "lon": 9.732, "display_name": "Hannover, Germany", "zoom": 13},
    "nuremberg": {"lat": 49.454, "lon": 11.077, "display_name": "Nuremberg, Germany", "zoom": 13},
    # Europe
    "london": {"lat": 51.507, "lon": -0.128, "display_name": "London, UK", "zoom": 12},
    "paris": {"lat": 48.857, "lon": 2.352, "display_name": "Paris, France", "zoom": 12},
    "amsterdam": {"lat": 52.370, "lon": 4.895, "display_name": "Amsterdam, Netherlands", "zoom": 13},
    "vienna": {"lat": 48.209, "lon": 16.372, "display_name": "Vienna, Austria", "zoom": 12},
    "zurich": {"lat": 47.377, "lon": 8.542, "display_name": "Zurich, Switzerland", "zoom": 13},
    "brussels": {"lat": 50.850, "lon": 4.352, "display_name": "Brussels, Belgium", "zoom": 13},
    "copenhagen": {"lat": 55.676, "lon": 12.569, "display_name": "Copenhagen, Denmark", "zoom": 13},
    "stockholm": {"lat": 59.329, "lon": 18.069, "display_name": "Stockholm, Sweden", "zoom": 12},
    "oslo": {"lat": 59.913, "lon": 10.752, "display_name": "Oslo, Norway", "zoom": 13},
    "helsinki": {"lat": 60.170, "lon": 24.941, "display_name": "Helsinki, Finland", "zoom": 13},
    "barcelona": {"lat": 41.385, "lon": 2.173, "display_name": "Barcelona, Spain", "zoom": 13},
    "madrid": {"lat": 40.417, "lon": -3.703, "display_name": "Madrid, Spain", "zoom": 12},
    "rome": {"lat": 41.903, "lon": 12.496, "display_name": "Rome, Italy", "zoom": 12},
    "milan": {"lat": 45.464, "lon": 9.190, "display_name": "Milan, Italy", "zoom": 13},
    "prague": {"lat": 50.088, "lon": 14.421, "display_name": "Prague, Czech Republic", "zoom": 13},
    "warsaw": {"lat": 52.230, "lon": 21.012, "display_name": "Warsaw, Poland", "zoom": 12},
    # World
    "new york": {"lat": 40.713, "lon": -74.006, "display_name": "New York, USA", "zoom": 12},
    "los angeles": {"lat": 34.052, "lon": -118.244, "display_name": "Los Angeles, USA", "zoom": 11},
    "chicago": {"lat": 41.878, "lon": -87.630, "display_name": "Chicago, USA", "zoom": 12},
    "san francisco": {"lat": 37.775, "lon": -122.419, "display_name": "San Francisco, USA", "zoom": 13},
    "tokyo": {"lat": 35.690, "lon": 139.692, "display_name": "Tokyo, Japan", "zoom": 11},
    "singapore": {"lat": 1.352, "lon": 103.820, "display_name": "Singapore", "zoom": 12},
    "sydney": {"lat": -33.869, "lon": 151.209, "display_name": "Sydney, Australia", "zoom": 12},
    "dubai": {"lat": 25.205, "lon": 55.271, "display_name": "Dubai, UAE", "zoom": 12},
}

# Nominatim zoom levels by place type
ZOOM_BY_TYPE = {
    "city": 12,
    "town": 13,
    "village": 14,
    "suburb": 14,
    "neighbourhood": 15,
    "residential": 15,
    "district": 14,
    "county": 11,
    "state": 8,
    "country": 6,
    "building": 18,
    "house": 18,
    "address": 17,
    "road": 16,
    "street": 16,
}


def main(query: str, limit: int = 1) -> dict:
    """
    Geocode location string to coordinates.

    Args:
        query: Location search string (e.g., "Hamburg", "Berlin Mitte", "Marienplatz Munich")
        limit: Maximum number of results (default 1)

    Returns:
        {
            "lat": 53.551,
            "lon": 9.993,
            "display_name": "Hamburg, Germany",
            "zoom": 12,
            "success": true,
            "source": "lookup" | "nominatim"
        }
    """
    if not query or not query.strip():
        return {
            "success": False,
            "error": "Empty query",
            "lat": None,
            "lon": None,
        }

    query_normalized = query.lower().strip()

    # Try city lookup only for simple queries (city name only, no street/address)
    # Skip cache if query contains commas or numbers (likely a street address)
    is_simple_query = "," not in query_normalized and not any(c.isdigit() for c in query_normalized)

    if is_simple_query:
        # Exact match
        if query_normalized in CITY_LOOKUP:
            result = CITY_LOOKUP[query_normalized].copy()
            result["success"] = True
            result["source"] = "lookup"
            return result

        # Partial match (only for short queries that look like city names)
        if len(query_normalized.split()) <= 2:
            for city_key, city_data in CITY_LOOKUP.items():
                if city_key in query_normalized or query_normalized in city_key:
                    result = city_data.copy()
                    result["success"] = True
                    result["source"] = "lookup"
                    return result

    # Fall back to Nominatim API
    try:
        # Rate limit compliance (1 req/sec for Nominatim)
        time.sleep(1.1)

        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": query,
                "format": "json",
                "limit": limit,
                "addressdetails": 1,
            },
            headers={
                "User-Agent": "EnvironmentalAnalysisMCP/1.0 (contact@example.com)",
                "Accept-Language": "en",
            },
            timeout=10,
        )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Nominatim API error: {response.status_code}",
                "lat": None,
                "lon": None,
            }

        results = response.json()

        if not results:
            return {
                "success": False,
                "error": f"No results found for: {query}",
                "lat": None,
                "lon": None,
            }

        first = results[0]
        place_type = first.get("type", "city")

        # Determine appropriate zoom level
        zoom = ZOOM_BY_TYPE.get(place_type, 14)

        # If bounding box available, calculate zoom from it
        if "boundingbox" in first:
            bbox = first["boundingbox"]
            lat_diff = abs(float(bbox[1]) - float(bbox[0]))
            lon_diff = abs(float(bbox[3]) - float(bbox[2]))
            max_diff = max(lat_diff, lon_diff)

            # Approximate zoom from bbox size
            if max_diff > 1:
                zoom = 10
            elif max_diff > 0.5:
                zoom = 11
            elif max_diff > 0.2:
                zoom = 12
            elif max_diff > 0.1:
                zoom = 13
            elif max_diff > 0.05:
                zoom = 14
            elif max_diff > 0.02:
                zoom = 15
            elif max_diff > 0.01:
                zoom = 16
            else:
                zoom = 17

        return {
            "lat": float(first["lat"]),
            "lon": float(first["lon"]),
            "display_name": first.get("display_name", query),
            "zoom": zoom,
            "success": True,
            "source": "nominatim",
            "place_type": place_type,
        }

    except requests.exceptions.Timeout:
        return {
            "success": False,
            "error": "Nominatim API timeout",
            "lat": None,
            "lon": None,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "lat": None,
            "lon": None,
        }
