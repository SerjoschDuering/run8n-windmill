"""
Merge tree data from multiple sources with deduplication.

Uses point-distance matching to identify duplicate trees.
Prefers municipal data (OpenTrees) over OSM when both exist.

Height Priority:
1. OpenTrees measured height (municipal surveys)
2. OSM height tag (direct measurement)
3. Genus-based estimate
4. Default urban average

Usage:
    result = main(osm_geojson, opentrees_geojson)
"""

import math
from shapely.geometry import Point
from shapely.strtree import STRtree
from typing import TypedDict

# Distance threshold for duplicate detection (meters)
DUPLICATE_DISTANCE_METERS = 3.0

# Convert degrees to approximate meters at mid-latitudes
# 1 degree ≈ 111,320 meters at equator, varies by latitude
DEG_TO_METERS_APPROX = 111320.0

# Height priority (lower = higher priority)
HEIGHT_PRIORITY = {
    "opentrees:measured": 1,
    "osm:height": 2,
    "osm:diameter_crown": 3,
    "osm:circumference": 4,
    "genus": 5,  # Prefix match for genus:*
    "default": 6,
}


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
    # Merge metadata
    merged_from: list[str]


def haversine_distance(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Calculate distance between two points in meters using Haversine formula."""
    R = 6371000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = (math.sin(delta_phi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def get_priority(source: str) -> int:
    """Get priority for a height source (lower = better)."""
    if source in HEIGHT_PRIORITY:
        return HEIGHT_PRIORITY[source]
    # Handle genus:* prefix
    if source.startswith("genus:"):
        return HEIGHT_PRIORITY["genus"]
    return 99


class TreeSpatialIndex:
    """Spatial index for tree points using Shapely STRtree."""

    def __init__(self, features: list[dict]):
        self.features = []
        self.points = []

        for f in features:
            geom = f.get("geometry", {})
            if geom.get("type") != "Point":
                continue
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                continue

            lon, lat = coords[0], coords[1]
            point = Point(lon, lat)
            self.features.append(f)
            self.points.append(point)

        self.tree = STRtree(self.points) if self.points else None

    def find_nearby(self, lon: float, lat: float, threshold_meters: float = DUPLICATE_DISTANCE_METERS) -> list[tuple[dict, float]]:
        """
        Find features within threshold distance of a point.

        Returns: list of (feature, distance_meters) tuples
        """
        if not self.tree:
            return []

        query_point = Point(lon, lat)

        # Convert threshold to approximate degrees for spatial query
        # This is a rough approximation; actual distance is calculated with Haversine
        threshold_deg = threshold_meters / DEG_TO_METERS_APPROX
        search_buffer = query_point.buffer(threshold_deg * 1.5)  # Buffer slightly larger for safety

        # Query spatial index
        candidates_idx = self.tree.query(search_buffer)

        # Calculate actual distances
        nearby = []
        for idx in candidates_idx:
            feat = self.features[idx]
            feat_coords = feat.get("geometry", {}).get("coordinates", [])
            if len(feat_coords) >= 2:
                dist = haversine_distance(lon, lat, feat_coords[0], feat_coords[1])
                if dist <= threshold_meters:
                    nearby.append((feat, dist))

        # Sort by distance
        nearby.sort(key=lambda x: x[1])
        return nearby


def merge_tree_properties(primary: dict, secondary: dict | None) -> TreeProperties:
    """
    Merge properties from two tree features, preferring primary.

    Takes height from source with highest priority.
    Combines metadata from both sources.
    """
    primary_props = primary.get("properties", {})
    secondary_props = secondary.get("properties", {}) if secondary else {}

    # Collect height candidates
    candidates = []

    # Primary heights
    p_height = primary_props.get("Height")
    p_source = primary_props.get("height_source")
    if p_height and p_source:
        candidates.append((p_height, p_source, primary_props.get("height_confidence", "low")))

    # Secondary heights
    if secondary_props:
        s_height = secondary_props.get("Height")
        s_source = secondary_props.get("height_source")
        if s_height and s_source:
            candidates.append((s_height, s_source, secondary_props.get("height_confidence", "low")))

    # Select best height by priority
    if candidates:
        candidates.sort(key=lambda x: get_priority(x[1]))
        best_height, best_source, best_confidence = candidates[0]
    else:
        best_height = 8.0
        best_source = "default"
        best_confidence = "low"

    # Determine best diameter (prefer measured, then primary)
    diameter = primary_props.get("Diameter")
    if not diameter and secondary_props:
        diameter = secondary_props.get("Diameter")
    if not diameter:
        diameter = best_height * 0.7

    # Combine metadata (prefer non-null)
    species = primary_props.get("species") or (secondary_props.get("species") if secondary_props else None)
    genus = primary_props.get("genus") or (secondary_props.get("genus") if secondary_props else None)
    leaf_type = primary_props.get("leaf_type") or (secondary_props.get("leaf_type") if secondary_props else None)

    # Track sources
    merged_from = [primary_props.get("data_source", "unknown")]
    if secondary_props:
        merged_from.append(secondary_props.get("data_source", "unknown"))

    return TreeProperties(
        GlobalID=primary_props.get("GlobalID", "merged"),
        Height=round(best_height, 1),
        Diameter=round(diameter, 1),
        height_source=best_source,
        height_confidence=best_confidence,
        species=species,
        genus=genus,
        leaf_type=leaf_type,
        data_source="merged" if secondary_props else primary_props.get("data_source", "unknown"),
        merged_from=merged_from,
    )


def main(
    osm_geojson: dict,
    opentrees_geojson: dict | None = None,
) -> dict:
    """
    Merge tree data from OSM and OpenTrees with deduplication.

    Uses point-distance matching. Prefers OpenTrees municipal data.

    Args:
        osm_geojson: OSM trees (from Overpass API)
        opentrees_geojson: OpenTrees municipal data (optional)

    Returns:
        GeoJSON FeatureCollection with merged, deduplicated trees
    """
    osm_features = osm_geojson.get("features", [])
    opentrees_features = (opentrees_geojson or {}).get("features", [])

    # Stats
    stats = {
        "osm_input": len(osm_features),
        "opentrees_input": len(opentrees_features),
        "duplicates_removed": 0,
        "opentrees_only": 0,
        "osm_only": 0,
        "merged": 0,
        "height_sources": {},
    }

    merged_features = []

    # Start with OpenTrees features (they're usually higher quality)
    # Build index of OpenTrees for lookup
    opentrees_index = TreeSpatialIndex(opentrees_features)
    osm_index = TreeSpatialIndex(osm_features)

    # Track which OSM features have been matched
    osm_matched = set()

    # Process OpenTrees features first
    for ot_feature in opentrees_features:
        ot_geom = ot_feature.get("geometry", {})
        ot_coords = ot_geom.get("coordinates", [])
        if len(ot_coords) < 2:
            continue

        # Find nearby OSM trees
        nearby_osm = osm_index.find_nearby(ot_coords[0], ot_coords[1])

        secondary = None
        if nearby_osm:
            # Use closest OSM tree as secondary
            secondary = nearby_osm[0][0]
            osm_matched.add(secondary.get("id"))
            stats["merged"] += 1
        else:
            stats["opentrees_only"] += 1

        # Merge properties (OpenTrees is primary)
        merged_props = merge_tree_properties(ot_feature, secondary)

        merged_features.append({
            "type": "Feature",
            "id": merged_props["GlobalID"],
            "geometry": ot_geom,
            "properties": dict(merged_props),
        })

        # Track height sources
        src = merged_props.get("height_source", "unknown")
        stats["height_sources"][src] = stats["height_sources"].get(src, 0) + 1

    # Add unmatched OSM features
    for osm_feature in osm_features:
        if osm_feature.get("id") in osm_matched:
            stats["duplicates_removed"] += 1
            continue

        osm_props = osm_feature.get("properties", {})
        merged_props = merge_tree_properties(osm_feature, None)

        merged_features.append({
            "type": "Feature",
            "id": merged_props["GlobalID"],
            "geometry": osm_feature.get("geometry"),
            "properties": dict(merged_props),
        })

        stats["osm_only"] += 1

        # Track height sources
        src = merged_props.get("height_source", "unknown")
        stats["height_sources"][src] = stats["height_sources"].get(src, 0) + 1

    # Confidence breakdown
    confidence_counts = {"high": 0, "medium": 0, "low": 0}
    for f in merged_features:
        conf = f["properties"].get("height_confidence", "low")
        confidence_counts[conf] = confidence_counts.get(conf, 0) + 1

    # Source metadata
    source_metadata = {"osm": osm_geojson.get("metadata", {})}
    if opentrees_geojson:
        source_metadata["opentrees"] = opentrees_geojson.get("metadata", {})

    return {
        "type": "FeatureCollection",
        "features": merged_features,
        "metadata": {
            "source": "merged",
            "total_features": len(merged_features),
            "stats": stats,
            "height_confidence": confidence_counts,
            "sources": source_metadata,
            "dedup_threshold_meters": DUPLICATE_DISTANCE_METERS,
        },
    }
