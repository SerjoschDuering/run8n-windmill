"""
Merge building data from multiple sources with height priority.

Uses IoU (Intersection over Union) matching for accurate building alignment.
Falls back to centroid matching when IoU fails.

IMPORTANT: Merges ALL buildings from ALL sources (OSM, TUM, Overture).
Buildings not in OSM but present in Overture/TUM are still included.

Height Priority:
1. Overture height (direct measurement)
2. TUM measured height
3. OSM height tag
4. Overture num_floors * 3
5. OSM building:levels * 3
6. Estimate by building type/area
"""

from shapely.geometry import shape, Polygon, MultiPolygon
from shapely.ops import unary_union
from shapely.strtree import STRtree
from typing import TypedDict, Any
import math
import hashlib

# Height priority (lower = higher priority)
HEIGHT_PRIORITY = {
    "overture:height": 1,
    "tum:measured": 2,
    "osm:height": 3,
    "overture:floors": 4,
    "osm:levels": 5,
    "estimated": 6,
}

DEFAULT_STOREY_HEIGHT = 3.0

# IoU threshold for matching (0.3 = 30% overlap)
IOU_THRESHOLD = 0.3

# Slightly higher threshold for Overture-TUM matching (no OSM anchor)
OVERTURE_TUM_IOU_THRESHOLD = 0.35

# Fallback centroid threshold in meters
CENTROID_THRESHOLD_METERS = 30

# Minimum valid height (buildings below this are treated as unknown)
MIN_VALID_HEIGHT = 2.0

# Default heights by building type
HEIGHT_DEFAULTS = {
    "house": 7,
    "detached": 8,
    "apartments": 18,
    "residential": 10,
    "commercial": 15,
    "office": 18,
    "industrial": 12,
    "retail": 6,
    "warehouse": 10,
    "church": 20,
    "school": 12,
    "hospital": 20,
    "hotel": 25,
    "default": 10,
}

# Confidence levels - OSM-anchored vs unmatched
CONFIDENCE_OSM_ANCHORED = {
    "overture:height": "high",
    "tum:measured": "high",
    "osm:height": "high",
    "overture:floors": "medium",
    "osm:levels": "medium",
    "estimated": "low",
}

# Lower confidence for buildings not matched to OSM
CONFIDENCE_UNMATCHED = {
    "overture:height": "medium",
    "tum:measured": "medium",
    "overture:floors": "low",
    "estimated": "low",
}


class MergedProperties(TypedDict, total=False):
    id: str
    height: float
    height_source: str
    height_confidence: str
    height_osm: float | None
    height_osm_source: str | None
    height_tum: float | None
    height_overture: float | None
    height_overture_source: str | None
    match_method: str
    match_iou: float | None
    osm_id: int | None
    tum_id: str | None
    overture_id: str | None
    building_type: str | None
    name: str | None
    levels: int | None


def feature_to_shapely(feature: dict) -> tuple[Polygon | None, bool]:
    """
    Convert GeoJSON feature to Shapely polygon.

    Returns:
        Tuple of (polygon, was_multipolygon)
        - polygon: Shapely Polygon or None if invalid
        - was_multipolygon: True if input was MultiPolygon (converted to single)
    """
    try:
        geom = feature.get("geometry")
        if not geom:
            return None, False

        geom_type = geom.get("type")
        was_multipolygon = False

        if geom_type == "Polygon":
            poly = shape(geom)
        elif geom_type == "MultiPolygon":
            # Convert MultiPolygon to single Polygon via unary_union
            multi = shape(geom)
            poly = unary_union(multi)
            was_multipolygon = True
            # If result is still MultiPolygon (disjoint parts), take largest
            if isinstance(poly, MultiPolygon):
                poly = max(poly.geoms, key=lambda g: g.area)
        else:
            return None, False

        if not poly.is_valid:
            poly = poly.buffer(0)

        # Ensure we have a Polygon (not Point/Line from degenerate input)
        if not isinstance(poly, Polygon) or poly.is_empty:
            return None, was_multipolygon

        return poly, was_multipolygon
    except Exception:
        return None, False


def generate_synthetic_id(poly: Polygon, source: str, index: int) -> str:
    """
    Generate stable synthetic ID from polygon geometry.

    Uses centroid + area hash for stability across runs.
    """
    centroid = poly.centroid
    # Round to 6 decimals (~10cm precision) for stability
    key = f"{centroid.x:.6f},{centroid.y:.6f},{poly.area:.8f}"
    hash_hex = hashlib.md5(key.encode()).hexdigest()[:8]
    return f"{source}-noid-{hash_hex}-{index}"


def calculate_iou(poly1: Polygon, poly2: Polygon) -> float:
    """Calculate Intersection over Union for two polygons."""
    try:
        if not poly1.intersects(poly2):
            return 0.0
        intersection = poly1.intersection(poly2).area
        union = poly1.union(poly2).area
        return intersection / union if union > 0 else 0.0
    except Exception:
        return 0.0


def haversine_distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Calculate distance between two points in meters."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class SpatialIndex:
    """Simple spatial index using Shapely STRtree."""

    def __init__(self, features: list[dict]):
        self.features = []
        self.polygons = []
        self.centroids = []
        self.multipolygon_count = 0

        for f in features:
            poly, was_multi = feature_to_shapely(f)
            if poly:
                self.features.append(f)
                self.polygons.append(poly)
                self.centroids.append(poly.centroid)
                if was_multi:
                    self.multipolygon_count += 1

        self.tree = STRtree(self.polygons) if self.polygons else None
        self.centroid_tree = STRtree(self.centroids) if self.centroids else None

    def find_matches(self, query_poly: Polygon, iou_threshold: float = IOU_THRESHOLD) -> list[tuple[dict, float, str]]:
        """
        Find matching features by IoU, returns list of (feature, iou, method).

        Results are SORTED by IoU descending (best match first).
        """
        if not self.tree or not query_poly:
            return []

        matches = []
        candidates_idx = self.tree.query(query_poly)

        for idx in candidates_idx:
            iou = calculate_iou(query_poly, self.polygons[idx])
            if iou >= iou_threshold:
                matches.append((self.features[idx], iou, "iou"))

        # CRITICAL: Sort by IoU descending so best match is first
        matches.sort(key=lambda x: x[1], reverse=True)

        # Fallback to centroid if no IoU matches
        if not matches and self.centroid_tree:
            query_centroid = query_poly.centroid
            search_buffer = query_centroid.buffer(0.0004)
            nearby_idx = self.centroid_tree.query(search_buffer)

            best_dist = float('inf')
            best_match = None
            for idx in nearby_idx:
                centroid = self.centroids[idx]
                dist = haversine_distance(
                    query_centroid.x, query_centroid.y,
                    centroid.x, centroid.y
                )
                if dist <= CENTROID_THRESHOLD_METERS and dist < best_dist:
                    best_dist = dist
                    best_match = (self.features[idx], 0.0, "centroid")

            if best_match:
                matches.append(best_match)

        return matches


def get_height_info(feature: dict) -> tuple[float | None, str | None, int | None]:
    """
    Extract height, height_source, and levels from a feature.

    Heights below MIN_VALID_HEIGHT are treated as None (invalid/unknown).
    """
    props = feature.get("properties", {})
    height = props.get("height")
    height_source = props.get("height_source")
    levels = props.get("levels") or props.get("num_floors")

    # Validate height - reject suspiciously low values
    if height is not None and height < MIN_VALID_HEIGHT:
        height = None
        height_source = None

    return height, height_source, levels


def estimate_height(building_type: str | None, area: float | None = None) -> float:
    """Estimate building height from type or area."""
    if building_type:
        bt = building_type.lower()
        for key, default_height in HEIGHT_DEFAULTS.items():
            if key in bt:
                return default_height

    if area and area > 0:
        if area > 5000:
            return 8
        elif area > 1000:
            return 12
        elif area > 200:
            return 15
        else:
            return 10

    return HEIGHT_DEFAULTS["default"]


def merge_feature_heights(
    base_feature: dict,
    base_poly: Polygon | None,
    tum_matches: list[tuple[dict, float, str]],
    overture_matches: list[tuple[dict, float, str]],
) -> dict:
    """Merge heights from multiple sources into base feature (OSM-anchored)."""
    props = base_feature.get("properties", {})
    geometry = base_feature.get("geometry", {})

    candidates: list[tuple[float, str, int]] = []

    height_osm: float | None = None
    height_osm_source: str | None = None
    height_tum: float | None = None
    height_overture: float | None = None
    height_overture_source: str | None = None

    # OSM heights
    osm_height, osm_source, osm_levels = get_height_info(base_feature)
    if osm_height is not None and osm_source:
        height_osm = osm_height
        height_osm_source = osm_source
        candidates.append((osm_height, osm_source, HEIGHT_PRIORITY.get(osm_source, 99)))
    if osm_levels:
        estimated = osm_levels * DEFAULT_STOREY_HEIGHT
        if height_osm is None:
            height_osm = estimated
            height_osm_source = "osm:levels"
        candidates.append((estimated, "osm:levels", HEIGHT_PRIORITY["osm:levels"]))

    # TUM heights (from best IoU match)
    for tum_feature, iou, method in tum_matches:
        tum_height, _, _ = get_height_info(tum_feature)
        if tum_height is not None:
            height_tum = tum_height
            candidates.append((tum_height, "tum:measured", HEIGHT_PRIORITY["tum:measured"]))
            break

    # Overture heights (from best IoU match)
    for ov_feature, iou, method in overture_matches:
        ov_height, ov_source, ov_floors = get_height_info(ov_feature)
        if ov_height is not None:
            height_overture = ov_height
            height_overture_source = "overture:height"
            candidates.append((ov_height, "overture:height", HEIGHT_PRIORITY["overture:height"]))
        elif ov_floors:
            estimated = ov_floors * DEFAULT_STOREY_HEIGHT
            height_overture = estimated
            height_overture_source = "overture:floors"
            candidates.append((estimated, "overture:floors", HEIGHT_PRIORITY["overture:floors"]))
        break

    # Pick best height
    if candidates:
        candidates.sort(key=lambda x: x[2])
        best_height, best_source, _ = candidates[0]
    else:
        building_type = props.get("building_type") or props.get("building")
        area = base_poly.area * 111320 * 111320 if base_poly else None
        best_height = estimate_height(building_type, area)
        best_source = "estimated"

    # Determine match method and IoU
    match_method = "osm_base"
    match_iou = None
    if overture_matches:
        match_method = f"osm+overture:{overture_matches[0][2]}"
        match_iou = overture_matches[0][1] if overture_matches[0][1] > 0 else None
    elif tum_matches:
        match_method = f"osm+tum:{tum_matches[0][2]}"
        match_iou = tum_matches[0][1] if tum_matches[0][1] > 0 else None

    merged_props: MergedProperties = {
        "id": props.get("id", f"osm-{id(base_feature)}"),
        "height": best_height,
        "height_source": best_source,
        "height_confidence": CONFIDENCE_OSM_ANCHORED.get(best_source, "low"),
        "height_osm": height_osm,
        "height_osm_source": height_osm_source,
        "height_tum": height_tum,
        "height_overture": height_overture,
        "height_overture_source": height_overture_source,
        "match_method": match_method,
        "match_iou": round(match_iou, 3) if match_iou else None,
        "osm_id": props.get("osm_id"),
        "building_type": props.get("building_type") or props.get("building"),
        "name": props.get("name"),
        "levels": osm_levels,
    }

    if tum_matches:
        merged_props["tum_id"] = str(tum_matches[0][0].get("properties", {}).get("tum_id"))
    if overture_matches:
        merged_props["overture_id"] = str(overture_matches[0][0].get("properties", {}).get("overture_id"))

    return {
        "type": "Feature",
        "id": merged_props["id"],
        "geometry": geometry,
        "properties": merged_props,
    }


def create_unmatched_feature(
    primary: dict,
    primary_source: str,
    tum_match: tuple[dict, float, str] | None = None,
    index: int = 0,
    synthetic_id: str | None = None,
) -> dict:
    """Create feature for building not matched to OSM."""
    props = primary.get("properties", {})
    geometry = primary.get("geometry")
    poly, _ = feature_to_shapely(primary)

    height: float | None = None
    height_source: str | None = None
    height_tum: float | None = None
    height_overture: float | None = None
    height_overture_source: str | None = None

    if primary_source == "overture":
        # Try Overture height first
        ov_height = props.get("height")
        ov_floors = props.get("num_floors") or props.get("levels")

        if ov_height is not None:
            height = ov_height
            height_source = "overture:height"
            height_overture = ov_height
            height_overture_source = "overture:height"
        elif ov_floors:
            height = ov_floors * DEFAULT_STOREY_HEIGHT
            height_source = "overture:floors"
            height_overture = height
            height_overture_source = "overture:floors"

        # Enrich from TUM if matched and no Overture height
        if tum_match and height is None:
            tum_props = tum_match[0].get("properties", {})
            tum_h = tum_props.get("height")
            if tum_h is not None:
                height = tum_h
                height_source = "tum:measured"
                height_tum = tum_h

    elif primary_source == "tum":
        tum_h = props.get("height")
        if tum_h is not None:
            height = tum_h
            height_source = "tum:measured"
            height_tum = tum_h

    # Fallback to estimation
    if height is None:
        building_type = props.get("building_type") or props.get("subtype") or props.get("class_")
        area = poly.area * 111320 * 111320 if poly else None
        height = estimate_height(building_type, area)
        height_source = "estimated"

    # Determine match method
    if primary_source == "overture" and tum_match:
        match_method = "overture+tum_no_osm"
        match_iou = tum_match[1] if tum_match[1] > 0 else None
    else:
        match_method = f"{primary_source}_only"
        match_iou = None

    # Build ID - use synthetic_id if provided (for None ID buildings)
    if synthetic_id:
        feat_id = synthetic_id
    elif primary_source == "overture":
        feat_id = f"overture-{props.get('overture_id', index)}"
    else:
        feat_id = f"tum-{props.get('tum_id', index)}"

    merged_props: MergedProperties = {
        "id": feat_id,
        "height": height,
        "height_source": height_source,
        "height_confidence": CONFIDENCE_UNMATCHED.get(height_source, "low"),
        "height_osm": None,
        "height_osm_source": None,
        "height_tum": height_tum,
        "height_overture": height_overture,
        "height_overture_source": height_overture_source,
        "match_method": match_method,
        "match_iou": round(match_iou, 3) if match_iou else None,
        "osm_id": None,
        "building_type": props.get("building_type") or props.get("subtype") or props.get("class_"),
        "name": props.get("name"),
        "levels": props.get("num_floors") or props.get("levels"),
    }

    if primary_source == "overture":
        merged_props["overture_id"] = str(props.get("overture_id"))
        if tum_match:
            merged_props["tum_id"] = str(tum_match[0].get("properties", {}).get("tum_id"))
    elif primary_source == "tum":
        merged_props["tum_id"] = str(props.get("tum_id"))

    return {
        "type": "Feature",
        "id": feat_id,
        "geometry": geometry,
        "properties": merged_props,
    }


def main(
    osm_geojson: dict,
    tum_geojson: dict | None = None,
    overture_geojson: dict | None = None,
) -> dict:
    """
    Merge building data from ALL sources with height priority.

    Includes buildings from OSM, TUM, and Overture - no data is lost.
    Buildings not in OSM but present in other sources are still included.

    Args:
        osm_geojson: OSM buildings (primary geometry source)
        tum_geojson: TUM 3D buildings (optional)
        overture_geojson: Overture buildings (optional)

    Returns:
        GeoJSON FeatureCollection with merged heights from all sources
    """
    osm_features = osm_geojson.get("features", [])
    tum_features = (tum_geojson or {}).get("features", [])
    overture_features = (overture_geojson or {}).get("features", [])

    # Build spatial indices
    tum_index = SpatialIndex(tum_features) if tum_features else None
    overture_index = SpatialIndex(overture_features) if overture_features else None

    merged_features = []

    # Track matched IDs to find unmatched buildings later
    matched_tum_ids: set[str] = set()
    matched_overture_ids: set[str] = set()

    stats = {
        "input_osm": len(osm_features),
        "input_tum": len(tum_features),
        "input_overture": len(overture_features),
        "osm_processed": 0,
        "osm_invalid_geom": 0,
        "osm_multipolygon_converted": 0,
        "overture_only": 0,
        "overture_invalid_geom": 0,
        "overture_multipolygon_converted": 0,
        "overture_none_id": 0,
        "tum_only": 0,
        "tum_invalid_geom": 0,
        "tum_multipolygon_converted": 0,
        "tum_none_id": 0,
        "overture_tum_merged": 0,
        "with_osm_height": 0,
        "with_tum_height": 0,
        "with_overture_height": 0,
        "estimated": 0,
        "matched_by_iou": 0,
        "matched_by_centroid": 0,
        "heights_rejected_too_low": 0,
    }

    # =========================================================================
    # Phase 1: Process OSM buildings (primary source)
    # =========================================================================
    for osm_feature in osm_features:
        osm_poly, was_multi = feature_to_shapely(osm_feature)
        if not osm_poly:
            stats["osm_invalid_geom"] += 1
            continue
        if was_multi:
            stats["osm_multipolygon_converted"] += 1

        # Find matches using IoU
        tum_matches = tum_index.find_matches(osm_poly) if tum_index else []
        overture_matches = overture_index.find_matches(osm_poly) if overture_index else []

        # Track matched IDs - including synthetic IDs for None-ID buildings
        for idx, (tum_feat, _, _) in enumerate(tum_matches):
            tum_id = tum_feat.get("properties", {}).get("tum_id")
            if tum_id is not None:
                matched_tum_ids.add(str(tum_id))
            else:
                # Generate synthetic ID for None-ID building
                tum_poly_matched, _ = feature_to_shapely(tum_feat)
                if tum_poly_matched:
                    synthetic = generate_synthetic_id(tum_poly_matched, "tum", idx)
                    matched_tum_ids.add(synthetic)

        for idx, (ov_feat, _, _) in enumerate(overture_matches):
            ov_id = ov_feat.get("properties", {}).get("overture_id")
            if ov_id is not None:
                matched_overture_ids.add(str(ov_id))
            else:
                # Generate synthetic ID for None-ID building
                ov_poly_matched, _ = feature_to_shapely(ov_feat)
                if ov_poly_matched:
                    synthetic = generate_synthetic_id(ov_poly_matched, "overture", idx)
                    matched_overture_ids.add(synthetic)

        # Merge heights
        merged = merge_feature_heights(osm_feature, osm_poly, tum_matches, overture_matches)
        merged_features.append(merged)

        # Update stats
        stats["osm_processed"] += 1
        source = merged["properties"].get("height_source", "")
        if source.startswith("osm"):
            stats["with_osm_height"] += 1
        elif source.startswith("tum"):
            stats["with_tum_height"] += 1
        elif source.startswith("overture"):
            stats["with_overture_height"] += 1
        elif source == "estimated":
            stats["estimated"] += 1

        method = merged["properties"].get("match_method", "")
        if "iou" in method:
            stats["matched_by_iou"] += 1
        elif "centroid" in method:
            stats["matched_by_centroid"] += 1

    # =========================================================================
    # Phase 2: Collect unmatched Overture buildings
    # Include buildings with None IDs (they weren't tracked in Phase 1)
    # =========================================================================
    unmatched_overture = []
    for idx, ov_feat in enumerate(overture_features):
        ov_id = ov_feat.get("properties", {}).get("overture_id")
        if ov_id is None:
            # None ID - generate synthetic ID for tracking
            ov_poly, _ = feature_to_shapely(ov_feat)
            if ov_poly:
                synthetic_id = generate_synthetic_id(ov_poly, "overture", idx)
                # Check if matched by geometry (could have been matched in Phase 1)
                if synthetic_id not in matched_overture_ids:
                    unmatched_overture.append((ov_feat, synthetic_id))
                    stats["overture_none_id"] += 1
        elif str(ov_id) not in matched_overture_ids:
            unmatched_overture.append((ov_feat, str(ov_id)))

    # =========================================================================
    # Phase 3: Collect unmatched TUM buildings
    # Include buildings with None IDs (they weren't tracked in Phase 1)
    # =========================================================================
    unmatched_tum = []
    for idx, tum_feat in enumerate(tum_features):
        tum_id = tum_feat.get("properties", {}).get("tum_id")
        if tum_id is None:
            # None ID - generate synthetic ID for tracking
            tum_poly, _ = feature_to_shapely(tum_feat)
            if tum_poly:
                synthetic_id = generate_synthetic_id(tum_poly, "tum", idx)
                # Check if matched by geometry (could have been matched in Phase 1)
                if synthetic_id not in matched_tum_ids:
                    unmatched_tum.append((tum_feat, synthetic_id))
                    stats["tum_none_id"] += 1
        elif str(tum_id) not in matched_tum_ids:
            unmatched_tum.append((tum_feat, str(tum_id)))

    # =========================================================================
    # Phase 4: Process unmatched Overture buildings
    # Try to match with unmatched TUM to avoid duplicates
    # CRITICAL: Track ALL TUM matches, not just first, to prevent duplicates
    # =========================================================================
    # Build index from just the features (not tuples)
    unmatched_tum_features = [t[0] for t in unmatched_tum]
    unmatched_tum_ids = {id(t[0]): t[1] for t in unmatched_tum}  # Map feature to its ID
    unmatched_tum_index = SpatialIndex(unmatched_tum_features) if unmatched_tum_features else None
    tum_ids_matched_by_overture: set[str] = set()

    for i, (ov_feat, ov_id) in enumerate(unmatched_overture):
        ov_poly, was_multi = feature_to_shapely(ov_feat)
        if not ov_poly:
            stats["overture_invalid_geom"] += 1
            continue
        if was_multi:
            stats["overture_multipolygon_converted"] += 1

        # Try to match with unmatched TUM (higher threshold for non-OSM matching)
        tum_matches = []
        if unmatched_tum_index:
            tum_matches = unmatched_tum_index.find_matches(ov_poly, iou_threshold=OVERTURE_TUM_IOU_THRESHOLD)

        # Create merged feature (Overture geometry, best height from first match)
        merged = create_unmatched_feature(
            primary=ov_feat,
            primary_source="overture",
            tum_match=tum_matches[0] if tum_matches else None,
            index=i,
            synthetic_id=ov_id if ov_id.startswith("overture-noid-") else None,
        )
        merged_features.append(merged)

        # CRITICAL FIX: Track ALL TUM IDs matched by this Overture building
        # This prevents duplicates when one Overture matches multiple TUM
        if tum_matches:
            for tum_match in tum_matches:
                tum_feat = tum_match[0]
                # Get the tracked ID (may be synthetic)
                tracked_id = unmatched_tum_ids.get(id(tum_feat))
                if tracked_id:
                    tum_ids_matched_by_overture.add(tracked_id)
            stats["overture_tum_merged"] += 1
        else:
            stats["overture_only"] += 1

        # Update height stats
        source = merged["properties"].get("height_source", "")
        if source.startswith("overture"):
            stats["with_overture_height"] += 1
        elif source.startswith("tum"):
            stats["with_tum_height"] += 1
        elif source == "estimated":
            stats["estimated"] += 1

    # =========================================================================
    # Phase 5: Add truly orphan TUM buildings
    # (not matched to OSM AND not matched to Overture)
    # =========================================================================
    for i, (tum_feat, tracked_id) in enumerate(unmatched_tum):
        # Skip if already matched by Overture (using tracked ID which may be synthetic)
        if tracked_id in tum_ids_matched_by_overture:
            continue

        tum_poly, was_multi = feature_to_shapely(tum_feat)
        if not tum_poly:
            stats["tum_invalid_geom"] += 1
            continue
        if was_multi:
            stats["tum_multipolygon_converted"] += 1

        # Create standalone TUM feature
        merged = create_unmatched_feature(
            primary=tum_feat,
            primary_source="tum",
            index=i,
            synthetic_id=tracked_id if tracked_id.startswith("tum-noid-") else None,
        )
        merged_features.append(merged)
        stats["tum_only"] += 1

        # Update height stats
        source = merged["properties"].get("height_source", "")
        if source.startswith("tum"):
            stats["with_tum_height"] += 1
        elif source == "estimated":
            stats["estimated"] += 1

    # =========================================================================
    # Build metadata
    # =========================================================================
    stats["total"] = len(merged_features)

    source_metadata = {"osm": osm_geojson.get("metadata", {})}
    if tum_geojson:
        source_metadata["tum"] = tum_geojson.get("metadata", {})
    if overture_geojson:
        source_metadata["overture"] = overture_geojson.get("metadata", {})

    return {
        "type": "FeatureCollection",
        "features": merged_features,
        "metadata": {
            "source": "merged",
            "stats": stats,
            "sources": source_metadata,
            "height_priority": list(HEIGHT_PRIORITY.keys()),
            "matching": {
                "osm_iou_threshold": IOU_THRESHOLD,
                "overture_tum_iou_threshold": OVERTURE_TUM_IOU_THRESHOLD,
                "centroid_fallback_meters": CENTROID_THRESHOLD_METERS,
            },
        },
    }
