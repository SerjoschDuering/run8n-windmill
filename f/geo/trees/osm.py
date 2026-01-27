"""
Fetch trees from OpenStreetMap via Overpass API.

Returns GeoJSON FeatureCollection with tree points and available height/diameter data.
Multiple Overpass endpoints for fallback resilience.

Usage:
    result = main(west=11.5, south=48.1, east=11.6, north=48.2)
"""

import json
import math
import requests
from typing import TypedDict

# Overpass API endpoints (fallback order)
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

TIMEOUT_SECONDS = 60
MAX_FEATURES = 20000

# Genus-based height defaults (research-based averages)
GENUS_HEIGHT_DEFAULTS = {
    "quercus": 15,       # Oak
    "tilia": 18,         # Linden
    "platanus": 22,      # Plane tree
    "acer": 12,          # Maple
    "betula": 15,        # Birch
    "pinus": 18,         # Pine
    "fraxinus": 20,      # Ash
    "ulmus": 18,         # Elm
    "fagus": 20,         # Beech
    "prunus": 8,         # Cherry/Plum
    "malus": 6,          # Apple
    "salix": 12,         # Willow
    "phoenix": 10,       # Date palm (Middle East)
    "washingtonia": 15,  # Fan palm
    "carpinus": 12,      # Hornbeam
    "populus": 20,       # Poplar
    "picea": 25,         # Spruce
    "abies": 30,         # Fir
    "cedrus": 25,        # Cedar
    "larix": 25,         # Larch
    "robinia": 15,       # Black locust
    "ailanthus": 15,     # Tree of heaven
}

DEFAULT_HEIGHT = 8.0  # Conservative urban average
DEFAULT_DIAMETER_RATIO = 0.7  # diameter ≈ height × 0.7


class BBox(TypedDict):
    west: float
    south: float
    east: float
    north: float


class TreeProperties(TypedDict, total=False):
    GlobalID: str
    Height: float
    Diameter: float
    height_source: str
    height_confidence: str
    species: str | None
    genus: str | None
    leaf_type: str | None
    data_source: str


def build_overpass_query(bbox: BBox) -> str:
    """Build Overpass QL query for trees."""
    return f"""
[out:json][timeout:{TIMEOUT_SECONDS}];
node["natural"="tree"]({bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']});
out meta;
"""


def parse_height(value: str | None) -> float | None:
    """Parse OSM height tag to meters."""
    if not value:
        return None
    try:
        # Handle "10 m", "10m", "10", "10.5 meter"
        cleaned = value.lower().replace("meter", "").replace("m", "").replace(" ", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_diameter(value: str | None) -> float | None:
    """Parse diameter_crown tag to meters."""
    if not value:
        return None
    try:
        # Handle "5 m", "5m", "5"
        cleaned = value.lower().replace("meter", "").replace("m", "").replace(" ", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_circumference(value: str | None) -> float | None:
    """Parse circumference tag to meters."""
    if not value:
        return None
    try:
        cleaned = value.lower().replace("meter", "").replace("m", "").replace(" ", "").strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def extract_genus(tags: dict) -> str | None:
    """Extract genus from species or genus tag."""
    # Check direct genus tag first
    genus = tags.get("genus")
    if genus:
        return genus.lower().split()[0]  # Take first word

    # Try to extract from species (typically "Genus species")
    species = tags.get("species")
    if species:
        parts = species.split()
        if parts:
            return parts[0].lower()

    # Try taxon tag
    taxon = tags.get("taxon")
    if taxon:
        parts = taxon.split()
        if parts:
            return parts[0].lower()

    return None


def estimate_height(tags: dict) -> tuple[float, str, str]:
    """
    Estimate tree height using priority cascade.

    Returns: (height, source, confidence)

    Priority:
    1. OSM height tag (high confidence)
    2. diameter_crown * 0.9 (medium confidence - height ≈ spread)
    3. circumference / π * 0.5 (medium - allometric)
    4. genus lookup (medium confidence)
    5. default (low confidence)
    """
    # Priority 1: Direct height measurement
    height = parse_height(tags.get("height"))
    if height is not None and height > 0:
        return (height, "osm:height", "high")

    # Priority 2: Estimate from crown diameter
    diameter = parse_diameter(tags.get("diameter_crown"))
    if diameter is not None and diameter > 0:
        estimated = diameter * 0.9  # Height roughly equals spread for mature trees
        return (estimated, "osm:diameter_crown", "medium")

    # Priority 3: Estimate from trunk circumference (allometric)
    circumference = parse_circumference(tags.get("circumference"))
    if circumference is not None and circumference > 0:
        # Rough allometric relationship: trunk diameter relates to height
        trunk_diameter = circumference / math.pi
        # Very rough: height ≈ 0.5 × circumference/π (based on typical ratios)
        estimated = trunk_diameter * 4  # Conservative multiplier
        estimated = min(max(estimated, 3), 35)  # Clamp to reasonable range
        return (estimated, "osm:circumference", "medium")

    # Priority 4: Genus-based default
    genus = extract_genus(tags)
    if genus and genus in GENUS_HEIGHT_DEFAULTS:
        return (GENUS_HEIGHT_DEFAULTS[genus], f"genus:{genus}", "medium")

    # Priority 5: Fall back to urban default
    return (DEFAULT_HEIGHT, "default", "low")


def estimate_diameter(height: float, tags: dict) -> float:
    """Estimate canopy diameter from height or tags."""
    # First check if we have diameter_crown
    diameter = parse_diameter(tags.get("diameter_crown"))
    if diameter is not None and diameter > 0:
        return diameter

    # Estimate from height
    return round(height * DEFAULT_DIAMETER_RATIO, 1)


def osm_node_to_feature(node: dict) -> dict | None:
    """Convert OSM node to GeoJSON Point Feature."""
    osm_id = node.get("id")
    lat = node.get("lat")
    lon = node.get("lon")
    tags = node.get("tags", {})

    if lat is None or lon is None:
        return None

    # Estimate height
    height, height_source, height_confidence = estimate_height(tags)

    # Estimate diameter
    diameter = estimate_diameter(height, tags)

    # Extract metadata
    genus = extract_genus(tags)
    species = tags.get("species")
    leaf_type = tags.get("leaf_type")

    properties: TreeProperties = {
        "GlobalID": f"osm-tree-{osm_id}",
        "Height": round(height, 1),
        "Diameter": round(diameter, 1),
        "height_source": height_source,
        "height_confidence": height_confidence,
        "data_source": "osm",
    }

    # Add optional metadata
    if species:
        properties["species"] = species
    if genus:
        properties["genus"] = genus
    if leaf_type:
        properties["leaf_type"] = leaf_type

    return {
        "type": "Feature",
        "id": properties["GlobalID"],
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat],
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
    Fetch OSM trees for a bounding box.

    Args:
        west: Western longitude bound
        south: Southern latitude bound
        east: Eastern longitude bound
        north: Northern latitude bound

    Returns:
        GeoJSON FeatureCollection with tree points
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
                feature = osm_node_to_feature(element)
                if feature:
                    features.append(feature)
                    if len(features) >= MAX_FEATURES:
                        break

            # Stats
            confidence_counts = {"high": 0, "medium": 0, "low": 0}
            source_counts = {}
            for f in features:
                conf = f["properties"].get("height_confidence", "low")
                confidence_counts[conf] = confidence_counts.get(conf, 0) + 1
                src = f["properties"].get("height_source", "unknown")
                source_counts[src] = source_counts.get(src, 0) + 1

            return {
                "type": "FeatureCollection",
                "features": features,
                "metadata": {
                    "source": "osm",
                    "endpoint": endpoint,
                    "bbox": bbox,
                    "total_features": len(features),
                    "height_confidence": confidence_counts,
                    "height_sources": source_counts,
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
