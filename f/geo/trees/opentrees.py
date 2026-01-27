"""
Fetch trees from OpenTrees.org city datasets.

OpenTrees aggregates municipal tree inventories from 150+ cities.
Data is served as vector tiles, so we query bbox intersections
against known city coverages and fetch cached GeoJSON.

Returns GeoJSON FeatureCollection with tree points from municipal data.

Note: Coverage is patchy - mainly Europe, Australia, North America.
Returns empty collection for areas without coverage.

Usage:
    result = main(west=11.5, south=48.1, east=11.6, north=48.2)
"""

import json
import requests
from typing import TypedDict

TIMEOUT_SECONDS = 30
MAX_FEATURES = 20000

# Known city datasets with approximate bounding boxes
# Data sourced from municipal open data portals, aggregated by OpenTrees.org
# Each entry: city name -> { bbox: [west, south, east, north], source_url: ... }
OPENTREES_CITIES = {
    "munich": {
        "bbox": [11.36, 48.06, 11.72, 48.25],
        "source": "muenchen.de",
        "url": "https://opendata.muenchen.de/dataset/baumkataster-baumstandorte-stadtgebiet-muenchen",
    },
    "berlin": {
        "bbox": [13.08, 52.33, 13.76, 52.68],
        "source": "berlin.de",
        "url": "https://daten.berlin.de/datensaetze/baumbestand-berlin",
    },
    "vienna": {
        "bbox": [16.18, 48.12, 16.58, 48.32],
        "source": "data.gv.at",
        "url": "https://www.data.gv.at/katalog/dataset/stadt-wien_baumkatasterderstadtwien",
    },
    "zurich": {
        "bbox": [8.45, 47.32, 8.62, 47.43],
        "source": "stadt-zuerich.ch",
        "url": "https://data.stadt-zuerich.ch/dataset/geo_baumkataster",
    },
    "copenhagen": {
        "bbox": [12.45, 55.62, 12.65, 55.73],
        "source": "opendata.dk",
        "url": "https://www.opendata.dk/city-of-copenhagen/trae-registrering",
    },
    "paris": {
        "bbox": [2.22, 48.81, 2.47, 48.90],
        "source": "opendata.paris.fr",
        "url": "https://opendata.paris.fr/explore/dataset/les-arbres/",
    },
    "amsterdam": {
        "bbox": [4.72, 52.28, 5.02, 52.43],
        "source": "data.amsterdam.nl",
        "url": "https://data.amsterdam.nl/datasets/bomen/",
    },
    "melbourne": {
        "bbox": [144.87, -37.88, 145.05, -37.77],
        "source": "data.melbourne.vic.gov.au",
        "url": "https://data.melbourne.vic.gov.au/Environment/Trees/fp38-wiyy",
    },
    "sydney": {
        "bbox": [151.15, -33.92, 151.26, -33.85],
        "source": "data.cityofsydney.nsw.gov.au",
        "url": "https://data.cityofsydney.nsw.gov.au/datasets/",
    },
    "new_york": {
        "bbox": [-74.26, 40.49, -73.70, 40.92],
        "source": "data.cityofnewyork.us",
        "url": "https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh",
    },
    "san_francisco": {
        "bbox": [-122.52, 37.70, -122.35, 37.83],
        "source": "data.sfgov.org",
        "url": "https://data.sfgov.org/City-Infrastructure/Street-Tree-List/tkzw-k3nq",
    },
    "vancouver": {
        "bbox": [-123.27, 49.20, -123.02, 49.32],
        "source": "opendata.vancouver.ca",
        "url": "https://opendata.vancouver.ca/explore/dataset/street-trees/",
    },
    "london": {
        "bbox": [-0.51, 51.28, 0.33, 51.69],
        "source": "data.london.gov.uk",
        "url": "https://data.london.gov.uk/dataset/local-authority-maintained-trees",
    },
}

# Genus-based height defaults (used when municipal data lacks height)
GENUS_HEIGHT_DEFAULTS = {
    "quercus": 15, "tilia": 18, "platanus": 22, "acer": 12, "betula": 15,
    "pinus": 18, "fraxinus": 20, "ulmus": 18, "fagus": 20, "prunus": 8,
    "malus": 6, "salix": 12, "carpinus": 12, "populus": 20, "picea": 25,
}

DEFAULT_HEIGHT = 8.0
DEFAULT_DIAMETER_RATIO = 0.7


class BBox(TypedDict):
    west: float
    south: float
    east: float
    north: float


def bbox_intersects(bbox1: BBox, city_bbox: list) -> bool:
    """Check if two bboxes intersect."""
    west1, south1, east1, north1 = bbox1["west"], bbox1["south"], bbox1["east"], bbox1["north"]
    west2, south2, east2, north2 = city_bbox

    # Check for non-intersection
    if east1 < west2 or west1 > east2:
        return False
    if north1 < south2 or south1 > north2:
        return False

    return True


def find_matching_cities(bbox: BBox) -> list[str]:
    """Find cities whose coverage intersects with the query bbox."""
    matching = []
    for city_name, city_info in OPENTREES_CITIES.items():
        if bbox_intersects(bbox, city_info["bbox"]):
            matching.append(city_name)
    return matching


def fetch_city_data(city_name: str) -> dict | None:
    """
    Fetch tree data for a specific city.

    Note: This is a simplified implementation. In production, we would:
    1. Query the city's open data API directly, OR
    2. Use pre-cached GeoJSON files stored in S3

    For now, we return None to indicate no cached data available.
    The OSM fetcher provides baseline coverage; OpenTrees enhances
    when cached city data is uploaded to S3.
    """
    # TODO: Implement S3 fetch for pre-cached city data
    # s3_path = f"opentrees/{city_name}.geojson"
    # return wmill.load_s3_file({"s3": s3_path})

    return None


def estimate_height_from_genus(genus: str | None) -> tuple[float, str, str]:
    """Estimate height from genus name."""
    if genus:
        genus_lower = genus.lower().split()[0]
        if genus_lower in GENUS_HEIGHT_DEFAULTS:
            return (GENUS_HEIGHT_DEFAULTS[genus_lower], f"genus:{genus_lower}", "medium")
    return (DEFAULT_HEIGHT, "default", "low")


def normalize_tree_feature(feature: dict, city: str, idx: int) -> dict | None:
    """Normalize municipal tree data to app-compatible format."""
    props = feature.get("properties", {})
    geom = feature.get("geometry", {})

    if geom.get("type") != "Point":
        return None

    coords = geom.get("coordinates", [])
    if len(coords) < 2:
        return None

    # Extract height (try various field names)
    height = None
    height_source = None
    height_confidence = "low"

    for h_field in ["height", "Height", "HOEHE", "tree_height", "height_m", "TREE_HT"]:
        if h_field in props and props[h_field]:
            try:
                height = float(props[h_field])
                height_source = "opentrees:measured"
                height_confidence = "high"
                break
            except (ValueError, TypeError):
                continue

    # Try genus estimation if no height
    if height is None:
        genus = props.get("genus") or props.get("Genus") or props.get("GATTUNG")
        height, height_source, height_confidence = estimate_height_from_genus(genus)

    # Extract diameter (try various field names)
    diameter = None
    for d_field in ["crown_width", "crown_diameter", "diameter", "KRONE", "spread", "canopy"]:
        if d_field in props and props[d_field]:
            try:
                diameter = float(props[d_field])
                break
            except (ValueError, TypeError):
                continue

    if diameter is None:
        diameter = height * DEFAULT_DIAMETER_RATIO

    # Extract species/genus info
    species = props.get("species") or props.get("Species") or props.get("ART")
    genus = props.get("genus") or props.get("Genus") or props.get("GATTUNG")

    return {
        "type": "Feature",
        "id": f"opentrees-{city}-{idx}",
        "geometry": geom,
        "properties": {
            "GlobalID": f"opentrees-{city}-{idx}",
            "Height": round(height, 1),
            "Diameter": round(diameter, 1),
            "height_source": height_source,
            "height_confidence": height_confidence,
            "species": species,
            "genus": genus,
            "data_source": "opentrees",
            "opentrees_city": city,
        },
    }


def filter_features_to_bbox(features: list, bbox: BBox) -> list:
    """Filter features to only those within the bbox."""
    filtered = []
    for f in features:
        coords = f.get("geometry", {}).get("coordinates", [])
        if len(coords) >= 2:
            lon, lat = coords[0], coords[1]
            if (bbox["west"] <= lon <= bbox["east"] and
                bbox["south"] <= lat <= bbox["north"]):
                filtered.append(f)
    return filtered


def main(
    west: float,
    south: float,
    east: float,
    north: float,
) -> dict:
    """
    Fetch OpenTrees municipal tree data for a bounding box.

    Checks for city coverage intersection, fetches cached data if available.
    Returns empty collection for areas without coverage.

    Args:
        west: Western longitude bound
        south: Southern latitude bound
        east: Eastern longitude bound
        north: Northern latitude bound

    Returns:
        GeoJSON FeatureCollection with tree points from municipal data
    """
    bbox: BBox = {"west": west, "south": south, "east": east, "north": north}

    # Find cities with coverage
    matching_cities = find_matching_cities(bbox)

    if not matching_cities:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "opentrees",
                "bbox": bbox,
                "total_features": 0,
                "matching_cities": [],
                "note": "No OpenTrees city coverage for this bbox",
            },
        }

    all_features = []
    city_stats = {}

    for city in matching_cities:
        city_data = fetch_city_data(city)
        if city_data:
            raw_features = city_data.get("features", [])
            # Normalize and filter
            normalized = []
            for idx, f in enumerate(raw_features):
                norm = normalize_tree_feature(f, city, idx)
                if norm:
                    normalized.append(norm)

            # Filter to bbox
            filtered = filter_features_to_bbox(normalized, bbox)
            all_features.extend(filtered)
            city_stats[city] = {
                "raw_count": len(raw_features),
                "in_bbox": len(filtered),
            }

    # Limit features
    if len(all_features) > MAX_FEATURES:
        all_features = all_features[:MAX_FEATURES]

    return {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "source": "opentrees",
            "bbox": bbox,
            "total_features": len(all_features),
            "matching_cities": matching_cities,
            "city_stats": city_stats,
            "note": "OpenTrees provides municipal tree inventory data. S3 cache required for data." if not all_features else None,
        },
    }
