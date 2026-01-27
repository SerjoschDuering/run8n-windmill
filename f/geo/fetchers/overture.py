"""
Fetch building footprints with heights from Overture Maps via REST API.

Uses the hosted Overture Maps API (overturemapsapi.com) for fast queries.
Converts bbox to center+radius for API compatibility.

API Docs: https://www.overturemapsapi.com/docs/intro
"""

import wmill
import requests
import math
from typing import TypedDict


# Load from Windmill resource, fallback to demo key
def get_api_config() -> tuple[str, str]:
    try:
        resource = wmill.get_resource("f/resources/overture_api")
        return resource.get("base_url", "https://api.overturemapsapi.com"), resource.get("api_key", "DEMO-API-KEY")
    except Exception:
        return "https://api.overturemapsapi.com", "DEMO-API-KEY"

MAX_FEATURES = 2000
TIMEOUT_SECONDS = 30


class BBox(TypedDict):
    west: float
    south: float
    east: float
    north: float


def bbox_to_center_radius(bbox: BBox) -> tuple[float, float, float]:
    """Convert bbox to center point and radius in meters."""
    center_lat = (bbox["north"] + bbox["south"]) / 2
    center_lng = (bbox["east"] + bbox["west"]) / 2

    # Calculate radius using Haversine approximation
    lat_diff = bbox["north"] - bbox["south"]
    lng_diff = bbox["east"] - bbox["west"]

    # Approximate meters per degree at this latitude
    meters_per_deg_lat = 111320
    meters_per_deg_lng = 111320 * math.cos(math.radians(center_lat))

    # Use diagonal distance as radius
    lat_meters = lat_diff * meters_per_deg_lat
    lng_meters = lng_diff * meters_per_deg_lng
    radius = math.sqrt(lat_meters**2 + lng_meters**2) / 2

    # Cap radius at 5000m (API limit)
    radius = min(radius, 5000)

    return center_lat, center_lng, radius


def fetch_buildings(lat: float, lng: float, radius: float) -> dict:
    """Fetch buildings from Overture Maps API."""
    api_base, api_key = get_api_config()
    response = requests.get(
        f"{api_base}/buildings",
        params={
            "lat": lat,
            "lng": lng,
            "radius": int(radius),
            "limit": MAX_FEATURES,
            "format": "geojson",
        },
        headers={"x-api-key": api_key},
        timeout=TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.json()


def normalize_feature(feature: dict) -> dict:
    """Normalize Overture feature to our standard format."""
    props = feature.get("properties", {})

    height = props.get("height")
    num_floors = props.get("num_floors")

    # Determine height source
    height_source = None
    if height is not None:
        height_source = "overture:height"
    elif num_floors is not None:
        height_source = "overture:floors"

    return {
        "type": "Feature",
        "id": f"overture-{props.get('id', '')}",
        "geometry": feature.get("geometry"),
        "properties": {
            "id": f"overture-{props.get('id', '')}",
            "overture_id": props.get("id"),
            "height": float(height) if height is not None else None,
            "num_floors": int(num_floors) if num_floors is not None else None,
            "subtype": props.get("subtype"),
            "class_": props.get("class"),
            "height_source": height_source,
            "roof_shape": props.get("roof_shape"),
            "roof_height": props.get("roof_height"),
        },
    }


def main(
    west: float,
    south: float,
    east: float,
    north: float,
) -> dict:
    """
    Fetch Overture Maps buildings for a bounding box.

    Args:
        west: Western longitude bound
        south: Southern latitude bound
        east: Eastern longitude bound
        north: Northern latitude bound

    Returns:
        GeoJSON FeatureCollection with building polygons and heights.
    """
    bbox: BBox = {"west": west, "south": south, "east": east, "north": north}

    try:
        # Convert bbox to center + radius
        lat, lng, radius = bbox_to_center_radius(bbox)

        # Fetch from API
        data = fetch_buildings(lat, lng, radius)

        # Normalize features
        features = []
        for feature in data.get("features", []):
            normalized = normalize_feature(feature)
            if normalized.get("geometry"):
                features.append(normalized)

        # Stats
        with_height = sum(1 for f in features if f["properties"].get("height") is not None)
        with_floors = sum(1 for f in features if f["properties"].get("num_floors") is not None)

        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "source": "overture",
                "api": "overturemapsapi.com",
                "bbox": bbox,
                "query": {"lat": lat, "lng": lng, "radius": radius},
                "total_features": len(features),
                "with_height": with_height,
                "with_floors": with_floors,
                "status": "ok",
            },
        }

    except requests.Timeout:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "overture",
                "error": "API request timed out",
                "status": "timeout",
                "bbox": bbox,
                "total_features": 0,
            },
        }

    except requests.HTTPError as e:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "overture",
                "error": f"API error: {e.response.status_code} {e.response.reason}",
                "status": "api_error",
                "bbox": bbox,
                "total_features": 0,
            },
        }

    except Exception as e:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "overture",
                "error": f"Unexpected error: {str(e)}",
                "status": "error",
                "bbox": bbox,
                "total_features": 0,
            },
        }
