"""
Windmill Script: Infrared Environmental Simulation
Path: f/infrared/simulate

Runs UTCI or wind-speed analysis via Infrared.city API.
Transforms GeoJSON buildings/trees to 3D meshes with coordinate conversion.

Webhook: POST https://windmill.run8n.xyz/api/w/windmill_automations/jobs/run_wait_result/f/infrared/simulate
"""

import wmill
import requests
import json
import gzip
import base64
import zipfile
import io
import math
from typing import Literal, Optional

# Constants
INFRARED_API_URL = "https://fbiw2nq5ac.execute-api.eu-central-1.amazonaws.com/development-v1/api/run-analysis"
TIMEOUT_SECONDS = 120
GRID_SIZE = 512
METERS_PER_DEG_LAT = 111320


def get_api_key() -> str:
    """Get Infrared API key from Windmill resource, env var, or hardcoded fallback (testing only)."""
    # Try Windmill resource first
    try:
        resource = wmill.get_resource("f/resources/infrared_api")
        api_key = resource.get("api_key", "")
        if api_key:
            return api_key
    except Exception:
        pass

    # Try environment variable
    import os
    api_key = os.environ.get("INFRARED_API_KEY", "")
    if api_key:
        return api_key

    # Fallback for testing (TODO: remove in production)
    return "P53XvU3y2V2zPw0cGoS4p1pkaEybAOaG43EQRjhb"


def encode_payload(payload: dict) -> str:
    """Encode payload: JSON -> GZIP -> Base64."""
    json_bytes = json.dumps(payload).encode("utf-8")
    gzipped = gzip.compress(json_bytes)
    return base64.b64encode(gzipped).decode("utf-8")


class CoordinateTransform:
    """Transforms WGS84 coordinates to Infrared local space (0-512m).

    IMPORTANT: Preserves aspect ratio - does NOT scale to fit 512x512.
    Coordinates are converted to meters relative to SW corner.
    Values exceeding 512m are clamped.
    """

    def __init__(self, west: float, south: float, east: float, north: float):
        self.west = west
        self.south = south
        self.east = east
        self.north = north

        center_lat = (north + south) / 2
        self.meters_per_deg_lon = METERS_PER_DEG_LAT * math.cos(math.radians(center_lat))

        # Calculate actual bbox size in meters (for reference only)
        self.bbox_width_m = (east - west) * self.meters_per_deg_lon
        self.bbox_height_m = (north - south) * METERS_PER_DEG_LAT

    def transform(self, lon: float, lat: float) -> tuple[float, float]:
        """Transform WGS84 to local meters space (preserves aspect ratio).

        Returns coordinates in meters relative to SW corner, clamped to bbox extent.
        """
        x = (lon - self.west) * self.meters_per_deg_lon
        y = (lat - self.south) * METERS_PER_DEG_LAT

        # Clamp to valid range (0-512m)
        x = max(0, min(GRID_SIZE, round(x, 2)))
        y = max(0, min(GRID_SIZE, round(y, 2)))
        return x, y


def earcut_triangulate(coords: list[tuple[float, float]]) -> list[int]:
    """Triangulate polygon using mapbox-earcut (works for concave polygons).

    Falls back to simple fan triangulation if earcut not available.
    """
    if len(coords) < 3:
        return []

    # Remove duplicate closing point if present
    if coords[0] == coords[-1]:
        coords = coords[:-1]

    if len(coords) < 3:
        return []

    # Try mapbox-earcut for proper concave polygon triangulation
    try:
        from mapbox_earcut import triangulate_float32
        import numpy as np

        # Flatten coords for earcut: [x0, y0, x1, y1, ...]
        flat = []
        for x, y in coords:
            flat.extend([x, y])

        # triangulate_float32 returns numpy array of triangle indices
        indices = triangulate_float32(flat, [len(coords)])
        return indices.tolist()
    except ImportError:
        # Fallback to simple fan triangulation (only works for convex)
        indices = []
        for i in range(1, len(coords) - 1):
            indices.extend([0, i, i + 1])
        return indices


def building_to_geometry(feature: dict, transform: CoordinateTransform, mesh_id: int) -> Optional[dict]:
    """Convert GeoJSON building to 3D mesh geometry."""
    geom = feature.get("geometry", {})
    props = feature.get("properties", {})

    geom_type = geom.get("type", "")
    coords = geom.get("coordinates", [])

    if not coords:
        return None

    # Handle Polygon vs MultiPolygon
    if geom_type == "MultiPolygon":
        ring = coords[0][0]  # First polygon, outer ring
    elif geom_type == "Polygon":
        ring = coords[0]  # Outer ring
    else:
        return None

    if len(ring) < 4:
        return None

    # Get height (minimum 0.1m to avoid degenerate meshes)
    height = props.get("height") or props.get("Total_Height") or props.get("Height") or 10
    try:
        height = max(0.1, float(height))
    except (ValueError, TypeError):
        height = 10

    # Transform coordinates
    local_coords = []
    for pt in ring[:-1]:  # Skip closing point
        x, y = transform.transform(pt[0], pt[1])
        local_coords.append((x, y))

    if len(local_coords) < 3:
        return None

    # Build 3D mesh
    vertices = []  # Flat [x, y, z, x, y, z, ...]
    indices = []

    n = len(local_coords)

    # Bottom vertices (z=0)
    for x, y in local_coords:
        vertices.extend([x, y, 0])

    # Top vertices (z=height)
    for x, y in local_coords:
        vertices.extend([x, y, height])

    # Bottom face triangles
    bottom_tris = earcut_triangulate(local_coords)
    indices.extend(bottom_tris)

    # Top face triangles (reversed winding)
    for i in range(0, len(bottom_tris), 3):
        indices.extend([bottom_tris[i] + n, bottom_tris[i + 2] + n, bottom_tris[i + 1] + n])

    # Wall triangles (n quads = 2n triangles) - winding matches reference
    for i in range(n):
        next_i = (i + 1) % n
        b1, b2 = i, next_i
        t1, t2 = i + n, next_i + n
        # Two triangles per quad (matching reference winding)
        indices.extend([b1, b2, t2])
        indices.extend([b1, t2, t1])

    return {
        "mesh_id": mesh_id,
        "coordinates": vertices,
        "indices": indices
    }


def tree_to_geometry(feature: dict, transform: CoordinateTransform, mesh_id: int) -> Optional[dict]:
    """Convert GeoJSON tree point to 3D box geometry."""
    geom = feature.get("geometry", {})
    props = feature.get("properties", {})

    if geom.get("type") != "Point":
        return None

    coords = geom.get("coordinates", [])
    if len(coords) < 2:
        return None

    lon, lat = coords[0], coords[1]
    cx, cy = transform.transform(lon, lat)

    # Get tree dimensions
    height = props.get("Height") or props.get("height") or 8
    diameter = props.get("Diameter") or props.get("diameter") or props.get("diameter_crown") or 5

    try:
        height = max(0.1, float(height))
        diameter = max(0.1, float(diameter))
    except (ValueError, TypeError):
        height = 8
        diameter = 5

    half = diameter / 2

    # 8 vertices of box
    x0, y0 = max(0, cx - half), max(0, cy - half)
    x1, y1 = min(GRID_SIZE, cx + half), min(GRID_SIZE, cy + half)

    vertices = [
        x0, y0, 0,  # 0: bottom SW
        x1, y0, 0,  # 1: bottom SE
        x1, y1, 0,  # 2: bottom NE
        x0, y1, 0,  # 3: bottom NW
        x0, y0, height,  # 4: top SW
        x1, y0, height,  # 5: top SE
        x1, y1, height,  # 6: top NE
        x0, y1, height,  # 7: top NW
    ]

    # 12 triangles (6 faces x 2) - winding matches reference implementation
    indices = [
        0, 2, 1, 0, 3, 2,  # bottom
        4, 5, 6, 4, 6, 7,  # top
        0, 1, 5, 0, 5, 4,  # front
        2, 3, 7, 2, 7, 6,  # back
        3, 0, 4, 3, 4, 7,  # left
        1, 2, 6, 1, 6, 5,  # right
    ]

    return {
        "mesh_id": mesh_id,
        "coordinates": vertices,
        "indices": indices
    }


def build_wind_payload(
    transform: CoordinateTransform,
    wind_speed: float,
    wind_direction: float,
    buildings: Optional[dict],
    trees: Optional[dict],
) -> dict:
    """Build wind-speed analysis payload."""
    geometries = {}
    mesh_id = 0

    # Add buildings
    if buildings and "features" in buildings:
        for i, f in enumerate(buildings["features"]):
            geom = building_to_geometry(f, transform, mesh_id)
            if geom:
                geometries[f"building-{i}"] = geom
                mesh_id += 1

    # Add trees (in geometries for wind)
    if trees and "features" in trees:
        for i, f in enumerate(trees["features"]):
            geom = tree_to_geometry(f, transform, mesh_id)
            if geom:
                geometries[f"tree-{i}"] = geom
                mesh_id += 1

    return {
        "analysis-type": "wind-speed",
        "geometries": geometries,
        "vegetation": {},
        "wind-speed": wind_speed,
        "wind-direction": wind_direction,
    }


def build_utci_payload(
    transform: CoordinateTransform,
    weather: dict,
    buildings: Optional[dict],
    trees: Optional[dict],
) -> dict:
    """Build thermal-comfort-index (UTCI) payload."""
    geometries = {}
    vegetation = {}
    mesh_id = 0

    # Add buildings to geometries
    if buildings and "features" in buildings:
        for i, f in enumerate(buildings["features"]):
            geom = building_to_geometry(f, transform, mesh_id)
            if geom:
                geometries[f"building-{i}"] = geom
                mesh_id += 1

    # Add trees to vegetation (different location for UTCI)
    if trees and "features" in trees:
        for i, f in enumerate(trees["features"]):
            geom = tree_to_geometry(f, transform, mesh_id)
            if geom:
                vegetation[f"tree-{i}"] = geom
                mesh_id += 1

    # Site boundary (full 512x512)
    site = [[0, GRID_SIZE], [GRID_SIZE, GRID_SIZE], [GRID_SIZE, 0], [0, 0], [0, GRID_SIZE]]

    # Weather arrays (must have >= 2 values)
    wind_speed = weather.get("windSpeed", 2.5)
    wind_dir = weather.get("windDirection", 180)
    temp = weather.get("temperature", 28)
    humidity = weather.get("humidity", 50)

    # Use TMY radiation values when provided, otherwise use defaults
    month = weather.get("month", 7)
    hour = weather.get("hour", 14)
    dnr = weather.get("directNormalRadiation", 800)
    dhr = weather.get("diffuseHorizontalRadiation", 150)
    ghr = weather.get("globalHorizontalRadiation", 950)
    hir = weather.get("horizontalInfraredRadiation", 400)

    return {
        "analysis-type": "thermal-comfort-index",
        "geometries": geometries,
        "vegetation": vegetation,
        "site": site,
        "latitude": (transform.south + transform.north) / 2,
        "longitude": (transform.west + transform.east) / 2,
        "month-stamp": [month, month],
        "hour-stamp": [hour, hour + 1],
        "dry-bulb-temperature": [temp, temp],
        "relative-humidity": [humidity, humidity],
        "wind-speed": [wind_speed, wind_speed],
        "wind-direction": [wind_dir, wind_dir],
        "horizontal-infrared-radiation-intensity": [hir, hir],
        "diffuse-horizontal-radiation": [dhr, dhr],
        "direct-normal-radiation": [dnr, dnr],
        "global-horizontal-radiation": [ghr, ghr],
    }


def decode_response(response_text: str) -> list:
    """Decode response: Try JSON, then Base64 -> GZIP -> ZIP -> JSON."""
    # Try direct JSON first
    try:
        parsed = json.loads(response_text)
        if "result" in parsed:
            return decode_result_field(parsed["result"])
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict) and "0" in parsed:
            return dict_to_grid(parsed)
        if "grid" in parsed:
            return parsed["grid"]
    except json.JSONDecodeError:
        pass

    # Try Base64 -> GZIP -> ZIP/JSON
    try:
        decoded = base64.b64decode(response_text)
        decompressed = gzip.decompress(decoded)

        try:
            with zipfile.ZipFile(io.BytesIO(decompressed)) as z:
                if "data.json" in z.namelist():
                    data_json = z.read("data.json").decode("utf-8")
                    parsed = json.loads(data_json)
                    if isinstance(parsed, list):
                        return parsed
                    if isinstance(parsed, dict) and "0" in parsed:
                        return dict_to_grid(parsed)
                    if "grid" in parsed:
                        return parsed["grid"]
        except zipfile.BadZipFile:
            pass

        parsed = json.loads(decompressed.decode("utf-8"))
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict) and "0" in parsed:
            return dict_to_grid(parsed)
        if "grid" in parsed:
            return parsed["grid"]
    except Exception as e:
        raise ValueError(f"Failed to decode response: {e}")

    raise ValueError("Unrecognized response format")


def decode_result_field(result_base64: str) -> list:
    """Decode the 'result' wrapper field."""
    decoded = base64.b64decode(result_base64)

    if decoded[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(decoded)) as z:
            if "data.json" in z.namelist():
                data_json = z.read("data.json").decode("utf-8")
                parsed = json.loads(data_json)
                if isinstance(parsed, list):
                    return parsed
                if isinstance(parsed, dict) and "0" in parsed:
                    return dict_to_grid(parsed)
                return parsed.get("grid", [])

    try:
        decompressed = gzip.decompress(decoded)
        parsed = json.loads(decompressed.decode("utf-8"))
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict) and "0" in parsed:
            return dict_to_grid(parsed)
        return parsed.get("grid", [])
    except Exception:
        pass

    parsed = json.loads(decoded.decode("utf-8"))
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict) and "0" in parsed:
        return dict_to_grid(parsed)
    return parsed.get("grid", [])


def dict_to_grid(row_dict: dict) -> list:
    """Convert row dictionary format to grid array."""
    indices = sorted([int(k) for k in row_dict.keys()])
    return [row_dict[str(i)] for i in indices]


def analyze_grid(grid: list) -> dict:
    """Compute grid statistics."""
    values = []
    null_count = 0

    for row in grid:
        for val in row:
            if val is None or (isinstance(val, float) and val != val):
                null_count += 1
            else:
                values.append(val)

    if not values:
        return {
            "min": 0, "max": 0, "mean": 0,
            "nullCount": null_count,
            "totalCells": len(grid) * len(grid[0]) if grid else 0,
        }

    return {
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "mean": round(sum(values) / len(values), 2),
        "nullCount": null_count,
        "totalCells": len(grid) * len(grid[0]) if grid else 0,
    }


def main(
    west: float,
    south: float,
    east: float,
    north: float,
    analysis_type: Literal["utci", "wind"],
    wind_direction: float = 180,
    wind_speed: float = 2.5,
    temperature: float = 28,
    humidity: float = 50,
    month: int = 7,
    hour: int = 14,
    buildings: Optional[dict] = None,
    trees: Optional[dict] = None,
    direct_normal_radiation: Optional[float] = None,
    diffuse_horizontal_radiation: Optional[float] = None,
    global_horizontal_radiation: Optional[float] = None,
    horizontal_infrared_radiation: Optional[float] = None,
) -> dict:
    """
    Run Infrared environmental simulation.

    Args:
        west, south, east, north: Bounding box coordinates
        analysis_type: "utci" for thermal comfort, "wind" for wind speed
        wind_direction: Wind direction in degrees (0=N, 90=E, 180=S, 270=W)
        wind_speed: Wind speed in m/s
        temperature: Air temperature in Celsius (UTCI only)
        humidity: Relative humidity percentage (UTCI only)
        month: Month (1-12) for UTCI analysis
        hour: Hour (0-23) for UTCI analysis
        buildings: GeoJSON FeatureCollection from f/geo/flows/fetch_buildings
        trees: GeoJSON FeatureCollection from f/geo/trees/fetch
        direct_normal_radiation: DNI W/m2 (from TMY, default 800 if None)
        diffuse_horizontal_radiation: DHI W/m2 (from TMY, default 150 if None)
        global_horizontal_radiation: GHI W/m2 (from TMY, default 950 if None)
        horizontal_infrared_radiation: HIR W/m2 (from TMY, default 400 if None)

    Returns:
        Grid results with statistics
    """
    api_key = get_api_key()
    if not api_key:
        return {"error": "Missing Infrared API key", "grid": [], "status": "error"}

    # Create coordinate transformer
    transform = CoordinateTransform(west, south, east, north)

    # Build payload
    weather = {
        "windDirection": wind_direction,
        "windSpeed": wind_speed,
        "temperature": temperature,
        "humidity": humidity,
        "month": month,
        "hour": hour,
    }
    # Pass radiation values through weather dict if provided (used by build_utci_payload)
    if direct_normal_radiation is not None:
        weather["directNormalRadiation"] = direct_normal_radiation
    if diffuse_horizontal_radiation is not None:
        weather["diffuseHorizontalRadiation"] = diffuse_horizontal_radiation
    if global_horizontal_radiation is not None:
        weather["globalHorizontalRadiation"] = global_horizontal_radiation
    if horizontal_infrared_radiation is not None:
        weather["horizontalInfraredRadiation"] = horizontal_infrared_radiation

    if analysis_type == "wind":
        payload = build_wind_payload(transform, wind_speed, wind_direction, buildings, trees)
    else:
        payload = build_utci_payload(transform, weather, buildings, trees)

    encoded_body = encode_payload(payload)

    try:
        response = requests.post(
            INFRARED_API_URL,
            headers={
                "x-api-key": api_key,
                "Content-Type": "text/plain",
                "X-Infrared-Encoding": "gzip",
            },
            data=encoded_body,
            timeout=TIMEOUT_SECONDS,
        )
        response.raise_for_status()

        grid = decode_response(response.text)
        # Infrared grid is south-first (row 0 = south). Reverse for north-up rendering.
        if grid:
            grid = list(reversed(grid))
        stats = analyze_grid(grid)

        return {
            "grid": grid,
            "bbox": {"west": west, "south": south, "east": east, "north": north},
            "analysisType": analysis_type,
            "weather": weather,
            **stats,
            "status": "ok",
        }

    except requests.Timeout:
        return {"error": "API request timed out", "grid": [], "status": "timeout"}
    except requests.HTTPError as e:
        return {"error": f"API error: {e.response.status_code}", "grid": [], "status": "api_error"}
    except Exception as e:
        return {"error": str(e), "grid": [], "status": "error"}
