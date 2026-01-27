"""
Fetch building footprints from OpenStreetMap via Overpass API.

Returns GeoJSON FeatureCollection with building polygons and available height data.
Supports polygons with holes (courtyards) via relation multipolygon parsing.
Multiple Overpass endpoints for fallback resilience.

Usage:
    result = main(west=11.5, south=48.1, east=11.6, north=48.2)
"""

import json
import requests
from typing import TypedDict

# Overpass API endpoints (fallback order)
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

TIMEOUT_SECONDS = 60
MAX_FEATURES = 10000


class BBox(TypedDict):
    west: float
    south: float
    east: float
    north: float


class BuildingProperties(TypedDict, total=False):
    id: str
    osm_id: int
    osm_type: str
    height: float | None
    levels: int | None
    building_type: str | None
    name: str | None
    height_source: str
    has_holes: bool


def build_overpass_query(bbox: BBox) -> str:
    """Build Overpass QL query for buildings with geometry."""
    return f"""
[out:json][timeout:{TIMEOUT_SECONDS}];
(
  way["building"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
  relation["building"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
);
out geom;
"""


def parse_height(value: str | None) -> float | None:
    """Parse OSM height tag to meters."""
    if not value:
        return None
    try:
        # Handle "10 m", "10m", "10"
        cleaned = value.lower().replace("m", "").replace(" ", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_levels(value: str | None) -> int | None:
    """Parse building:levels tag to integer."""
    if not value:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def geometry_to_ring(geometry: list[dict]) -> list[list[float]] | None:
    """Convert Overpass geometry nodes to a closed ring."""
    if not geometry or len(geometry) < 3:
        return None
    coords = [[pt["lon"], pt["lat"]] for pt in geometry]
    # Close the ring if needed
    if coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords


def parse_relation_geometry(element: dict) -> list[list[list[float]]] | None:
    """
    Parse relation members into polygon rings (outer + inner holes).

    Returns coordinates array: [[outer_ring], [hole1], [hole2], ...]
    GeoJSON spec: first ring is exterior, subsequent rings are holes.
    """
    members = element.get("members", [])
    if not members:
        return None

    outer_rings: list[list[list[float]]] = []
    inner_rings: list[list[list[float]]] = []

    for member in members:
        role = member.get("role", "")
        geometry = member.get("geometry", [])

        ring = geometry_to_ring(geometry)
        if not ring:
            continue

        if role == "outer":
            outer_rings.append(ring)
        elif role == "inner":
            inner_rings.append(ring)

    # Need at least one outer ring
    if not outer_rings:
        return None

    # For simple case: one outer + N inner holes
    # GeoJSON Polygon: [outer, hole1, hole2, ...]
    if len(outer_rings) == 1:
        return [outer_rings[0]] + inner_rings

    # Multiple outer rings = MultiPolygon case
    # For now, take largest outer ring and its holes
    # (proper solution would need point-in-polygon to assign holes to outers)
    largest_outer = max(outer_rings, key=lambda r: abs(ring_area(r)))
    return [largest_outer] + inner_rings


def ring_area(ring: list[list[float]]) -> float:
    """Calculate signed area of a ring using shoelace formula."""
    n = len(ring)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n - 1):
        area += ring[i][0] * ring[i + 1][1]
        area -= ring[i + 1][0] * ring[i][1]
    return area / 2.0


def osm_element_to_feature(element: dict) -> dict | None:
    """
    Convert Overpass element to GeoJSON Feature.

    Handles both ways (simple polygons) and relations (polygons with holes).
    """
    osm_type = element.get("type")
    osm_id = element.get("id")
    tags = element.get("tags", {})

    # Build polygon coordinates based on element type
    has_holes = False

    if osm_type == "relation":
        # Relations can have inner/outer members (holes)
        coordinates = parse_relation_geometry(element)
        if not coordinates:
            return None
        has_holes = len(coordinates) > 1
    else:
        # Ways are simple closed polygons (no holes)
        geometry = element.get("geometry", [])
        ring = geometry_to_ring(geometry)
        if not ring:
            return None
        coordinates = [ring]

    # Parse height data
    height = parse_height(tags.get("height") or tags.get("building:height"))
    levels = parse_levels(tags.get("building:levels"))

    # Determine height source
    height_source = None
    if height is not None:
        height_source = "osm:height"
    elif levels is not None:
        height_source = "osm:levels"

    properties: BuildingProperties = {
        "id": f"osm-{osm_type}-{osm_id}",
        "osm_id": osm_id,
        "osm_type": osm_type,
        "height": height,
        "levels": levels,
        "building_type": tags.get("building"),
        "name": tags.get("name"),
        "height_source": height_source,
        "has_holes": has_holes,
    }

    return {
        "type": "Feature",
        "id": properties["id"],
        "geometry": {
            "type": "Polygon",
            "coordinates": coordinates,
        },
        "properties": properties,
    }


def fetch_from_endpoint(endpoint: str, query: str) -> dict:
    """Fetch from a single Overpass endpoint."""
    response = requests.post(
        endpoint,
        data={"data": query},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=TIMEOUT_SECONDS + 10,
    )
    response.raise_for_status()
    return response.json()


def main(
    west: float,
    south: float,
    east: float,
    north: float,
) -> dict:
    """
    Fetch OSM buildings for a bounding box.

    Args:
        west: Western longitude bound
        south: Southern latitude bound
        east: Eastern longitude bound
        north: Northern latitude bound

    Returns:
        GeoJSON FeatureCollection with building polygons
    """
    bbox: BBox = {"west": west, "south": south, "east": east, "north": north}
    query = build_overpass_query(bbox)

    last_error = None

    # Try each endpoint until one works
    for endpoint in OVERPASS_ENDPOINTS:
        try:
            data = fetch_from_endpoint(endpoint, query)
            elements = data.get("elements", [])

            # Convert to GeoJSON features
            features = []
            for element in elements:
                feature = osm_element_to_feature(element)
                if feature:
                    features.append(feature)
                    if len(features) >= MAX_FEATURES:
                        break

            # Stats
            with_height = sum(1 for f in features if f["properties"].get("height") is not None)
            with_levels = sum(1 for f in features if f["properties"].get("levels") is not None)
            with_holes = sum(1 for f in features if f["properties"].get("has_holes"))
            relations = sum(1 for f in features if f["properties"].get("osm_type") == "relation")

            return {
                "type": "FeatureCollection",
                "features": features,
                "metadata": {
                    "source": "osm",
                    "endpoint": endpoint,
                    "bbox": bbox,
                    "total_features": len(features),
                    "with_height": with_height,
                    "with_levels": with_levels,
                    "with_holes": with_holes,
                    "relations": relations,
                },
            }

        except requests.exceptions.RequestException as e:
            last_error = f"{endpoint}: {str(e)}"
            continue
        except json.JSONDecodeError as e:
            last_error = f"{endpoint}: Invalid JSON - {str(e)}"
            continue

    # All endpoints failed
    return {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {
            "source": "osm",
            "error": f"All Overpass endpoints failed. Last error: {last_error}",
            "bbox": bbox,
            "total_features": 0,
        },
    }
