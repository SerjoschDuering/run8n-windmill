"""
Fetch building footprints with heights from TUM Global 3D Building Model (WFS).

TUM provides measured building heights from satellite imagery analysis.
Service may be temporarily unavailable - designed for graceful failure.

Endpoint: https://tubvsig-so2sat-vm1.srv.mwn.de/geoserver/ows
Layer: global3D:lod1_global

Usage:
    result = main(west=11.5, south=48.1, east=11.6, north=48.2)
"""

import json
import requests
from typing import TypedDict

TUM_WFS_ENDPOINT = "https://tubvsig-so2sat-vm1.srv.mwn.de/geoserver/ows"
TUM_LAYER = "global3D:lod1_global"
MAX_FEATURES = 5000
TIMEOUT_SECONDS = 30


class BBox(TypedDict):
    west: float
    south: float
    east: float
    north: float


class TUMProperties(TypedDict, total=False):
    id: str
    tum_id: str | int
    height: float
    height_var: float | None
    height_source: str


def build_wfs_url(bbox: BBox) -> str:
    """Build WFS GetFeature URL for TUM 3D buildings."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": TUM_LAYER,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "count": str(MAX_FEATURES),
        # WFS 2.0 with EPSG:4326 expects lat,lon order (south,west,north,east)
        "bbox": f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']},EPSG:4326",
    }
    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{TUM_WFS_ENDPOINT}?{query_string}"


def normalize_geometry(geometry: dict) -> dict | None:
    """Convert MultiPolygon to Polygon (take first polygon)."""
    if not geometry:
        return None

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if geom_type == "Polygon":
        return geometry
    elif geom_type == "MultiPolygon" and coords:
        # Take the first polygon from MultiPolygon
        return {
            "type": "Polygon",
            "coordinates": coords[0],
        }
    return None


def tum_feature_to_geojson(feature: dict, index: int) -> dict | None:
    """Convert TUM WFS feature to standardized GeoJSON Feature."""
    geometry = normalize_geometry(feature.get("geometry"))
    if not geometry:
        return None

    props = feature.get("properties", {})
    tum_id = feature.get("id", index)

    height = props.get("height")
    if height is None or height <= 0:
        return None  # Skip buildings without valid height

    properties: TUMProperties = {
        "id": f"tum-{tum_id}",
        "tum_id": tum_id,
        "height": float(height),
        "height_var": props.get("var"),
        "height_source": "tum:measured",
    }

    return {
        "type": "Feature",
        "id": properties["id"],
        "geometry": geometry,
        "properties": properties,
    }


def main(
    west: float,
    south: float,
    east: float,
    north: float,
) -> dict:
    """
    Fetch TUM 3D building heights for a bounding box.

    Args:
        west: Western longitude bound
        south: Southern latitude bound
        east: Eastern longitude bound
        north: Northern latitude bound

    Returns:
        GeoJSON FeatureCollection with building polygons and measured heights.
        Returns empty collection with error metadata if service unavailable.
    """
    bbox: BBox = {"west": west, "south": south, "east": east, "north": north}
    url = build_wfs_url(bbox)

    try:
        response = requests.get(url, timeout=TIMEOUT_SECONDS)

        # Handle server errors gracefully
        if response.status_code == 502:
            return {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "source": "tum",
                    "error": "TUM WFS service unavailable (502 Bad Gateway)",
                    "status": "service_down",
                    "bbox": bbox,
                    "total_features": 0,
                },
            }

        if response.status_code == 503:
            return {
                "type": "FeatureCollection",
                "features": [],
                "metadata": {
                    "source": "tum",
                    "error": "TUM WFS service temporarily unavailable (503)",
                    "status": "service_down",
                    "bbox": bbox,
                    "total_features": 0,
                },
            }

        response.raise_for_status()
        data = response.json()

        # Convert features
        features = []
        raw_features = data.get("features", [])

        for i, feature in enumerate(raw_features):
            converted = tum_feature_to_geojson(feature, i)
            if converted:
                features.append(converted)
                if len(features) >= MAX_FEATURES:
                    break

        return {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "source": "tum",
                "endpoint": TUM_WFS_ENDPOINT,
                "layer": TUM_LAYER,
                "bbox": bbox,
                "total_features": len(features),
                "raw_features": len(raw_features),
                "status": "ok",
            },
        }

    except requests.exceptions.Timeout:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "tum",
                "error": f"TUM WFS request timed out after {TIMEOUT_SECONDS}s",
                "status": "timeout",
                "bbox": bbox,
                "total_features": 0,
            },
        }

    except requests.exceptions.ConnectionError as e:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "tum",
                "error": f"TUM WFS connection failed: {str(e)}",
                "status": "connection_error",
                "bbox": bbox,
                "total_features": 0,
            },
        }

    except requests.exceptions.RequestException as e:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "tum",
                "error": f"TUM WFS request failed: {str(e)}",
                "status": "error",
                "bbox": bbox,
                "total_features": 0,
            },
        }

    except json.JSONDecodeError as e:
        return {
            "type": "FeatureCollection",
            "features": [],
            "metadata": {
                "source": "tum",
                "error": f"TUM WFS returned invalid JSON: {str(e)}",
                "status": "parse_error",
                "bbox": bbox,
                "total_features": 0,
            },
        }
