#!/usr/bin/env python3
"""
Standalone BigQuery Overture benchmark (no psycopg2/cache dependencies).
"""

import json
import time
import math
import hashlib
from pathlib import Path
from typing import Optional
from google.cloud import bigquery
from google.oauth2 import service_account
import shapely


VALID_TYPES = {"building", "land", "water", "land_use", "place"}
MAX_BBOX_AREA_SQ_DEG = 0.01  # ~10 km² at equator

COLUMNS_BY_TYPE = {
    "building": [
        "id", "names", "sources", "class",
        "height", "num_floors", "has_parts"
    ],
    "land": ["id", "names", "sources"],
    "water": ["id", "names", "sources"],
    "land_use": ["id", "names", "sources"],
    "place": ["id", "names", "sources", "categories", "confidence"],
}


def validate_bbox(west, south, east, north) -> Optional[str]:
    """Validate bbox coordinates."""
    if not (-180 <= west <= 180 and -180 <= east <= 180):
        return f"Invalid longitude: west={west}, east={east}"
    if not (-90 <= south <= 90 and -90 <= north <= 90):
        return f"Invalid latitude: south={south}, north={north}"
    if west >= east:
        return f"Invalid bbox: west ({west}) >= east ({east})"
    if south >= north:
        return f"Invalid bbox: south ({south}) >= north ({north})"
    for coord in [west, south, east, north]:
        if not math.isfinite(coord):
            return f"Invalid coordinate (NaN/Inf): {coord}"
    area = (east - west) * (north - south)
    if area > MAX_BBOX_AREA_SQ_DEG:
        return f"Bbox too large: {area:.4f}° (max {MAX_BBOX_AREA_SQ_DEG}°)"
    return None


def build_query(west, south, east, north, overture_type: str) -> str:
    """Build BigQuery SQL."""
    polygon_wkt = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    cols = COLUMNS_BY_TYPE.get(overture_type, ["id", "names", "sources"])
    col_list = ", ".join(cols)
    return f"""
    SELECT
        ST_AsText(geometry) AS geometry_wkt,
        {col_list}
    FROM `bigquery-public-data.overture_maps.{overture_type}`
    WHERE ST_Within(geometry, ST_GeogFromText('{polygon_wkt}'))
    LIMIT 50000
    """


def parse_results(query_job, overture_type: str) -> tuple[list[dict], dict]:
    """Parse BigQuery results."""
    rows = list(query_job)

    if not rows:
        bytes_billed = getattr(query_job, 'total_bytes_billed', None) or 0
        cost_usd = (bytes_billed / 1099511627776) * 6.25
        return [], {
            "features_returned": 0,
            "bytes_billed": bytes_billed,
            "cost_usd": round(cost_usd, 6),
        }

    # Vectorized geometry conversion
    wkt_array = [row.geometry_wkt for row in rows]
    geoms = shapely.from_wkt(wkt_array)
    geojson_strings = shapely.to_geojson(geoms)

    features = []
    for i, row in enumerate(rows):
        try:
            if geoms[i] is None or shapely.is_empty(geoms[i]):
                continue
            if shapely.get_type_id(geoms[i]) == 7:  # Skip GeometryCollection
                continue

            geojson_geom = json.loads(geojson_strings[i])
            props = {"overture_type": overture_type, "id": row.id}

            for field in ["names", "sources", "height", "num_floors", "class",
                          "has_parts", "categories", "confidence"]:
                val = getattr(row, field, None)
                if val is not None:
                    props[field] = val

            # Extract name
            names_raw = props.get("names")
            if names_raw:
                if isinstance(names_raw, dict):
                    props["name"] = names_raw.get("primary")
                elif isinstance(names_raw, list) and names_raw:
                    first = names_raw[0]
                    if isinstance(first, dict):
                        props["name"] = first.get("value") or first.get("primary")

            features.append({
                "type": "Feature",
                "id": row.id,
                "geometry": geojson_geom,
                "properties": props
            })
        except Exception as e:
            print(f"Warning: Failed to parse feature {getattr(row, 'id', '?')}: {e}")
            continue

    bytes_billed = getattr(query_job, 'total_bytes_billed', None) or 0
    cost_usd = (bytes_billed / 1099511627776) * 6.25

    return features, {
        "features_returned": len(features),
        "bytes_billed": bytes_billed,
        "cost_usd": round(cost_usd, 6),
    }


def fetch_overture(west, south, east, north, types, gcp_creds):
    """Fetch Overture Maps features via BigQuery."""
    bbox = {"west": west, "south": south, "east": east, "north": north}

    # Validate
    bbox_error = validate_bbox(west, south, east, north)
    if bbox_error:
        return {"error": bbox_error, "bbox": bbox}

    invalid = [t for t in types if t not in VALID_TYPES]
    if invalid:
        return {"error": f"Invalid types: {invalid}", "bbox": bbox}

    # Initialize BigQuery client
    if isinstance(gcp_creds.get("credentials_json"), dict):
        credentials = service_account.Credentials.from_service_account_info(
            gcp_creds["credentials_json"]
        )
    else:
        credentials = gcp_creds.get("credentials_json")

    client = bigquery.Client(
        credentials=credentials,
        project=gcp_creds["project_id"]
    )

    # Query each type
    all_features = []
    total_bytes = 0
    total_cost = 0.0

    for overture_type in types:
        print(f"  [{overture_type}] querying BigQuery...")
        query = build_query(west, south, east, north, overture_type)
        query_job = client.query(query)

        features, stats = parse_results(query_job, overture_type)
        all_features.extend(features)
        total_bytes += stats["bytes_billed"]
        total_cost += stats["cost_usd"]

        print(f"  [{overture_type}] -> {stats['features_returned']} features, ${stats['cost_usd']:.6f}")

    return {
        "features": all_features,
        "metadata": {
            "source": "bigquery",
            "bbox": bbox,
            "types_requested": types,
            "features_returned": len(all_features),
            "bytes_billed": total_bytes,
            "cost_usd": total_cost,
        }
    }


# Test areas
AREAS = {
    "Hamburg 5km²": {
        "west": 9.985,
        "south": 53.540,
        "east": 10.018,
        "north": 53.560,
    },
    "Berlin 10km²": {
        "west": 13.377,
        "south": 52.506,
        "east": 13.423,
        "north": 52.534,
    },
}

ALL_TYPES = ["building", "land", "water", "land_use", "place"]


def load_gcp_credentials():
    """Load GCP credentials."""
    creds_path = Path.home() / "Downloads" / "n8ntest-425022-988a3c740b23.json"
    if not creds_path.exists():
        raise FileNotFoundError(f"GCP credentials not found at {creds_path}")
    with open(creds_path) as f:
        creds_json = json.load(f)
    return {
        "project_id": creds_json["project_id"],
        "credentials_json": creds_json
    }


def run_benchmark(area_name, bbox, types, gcp_creds):
    """Run a single benchmark."""
    print(f"\n{'='*70}")
    print(f"Testing: {area_name}")
    print(f"Bbox: {bbox}")
    print(f"Types: {types}")
    print(f"{'='*70}")

    start_time = time.time()
    result = fetch_overture(**bbox, types=types, gcp_creds=gcp_creds)
    end_time = time.time()

    duration = end_time - start_time

    if "error" in result:
        print(f"\n✗ Error: {result['error']}")
        return None

    metadata = result.get("metadata", {})
    features = metadata.get("features_returned", 0)
    cost = metadata.get("cost_usd", 0)

    # Calculate GeoJSON size
    geojson = {
        "type": "FeatureCollection",
        "features": result.get("features", [])
    }
    size_bytes = len(json.dumps(geojson).encode())
    size_mb = round(size_bytes / 1_048_576, 2)

    print(f"\n✓ Completed in {duration:.1f}s")
    print(f"  Features: {features:,}")
    print(f"  Cost: ${cost:.6f}")
    print(f"  Size: {size_mb} MB")

    return {
        "area": area_name,
        "duration_s": round(duration, 1),
        "features": features,
        "cost_usd": cost,
        "size_mb": size_mb,
        "types_count": len(types)
    }


def print_results_table(results):
    """Print formatted results table."""
    print("\n" + "="*80)
    print("BENCHMARK RESULTS")
    print("="*80)
    print()
    print("┌──────────────┬──────────┬────────┬──────────┬─────────┬─────────┐")
    print("│     Area     │   Time   │  Cost  │ Features │  Size   │  Types  │")
    print("├──────────────┼──────────┼────────┼──────────┼─────────┼─────────┤")

    for r in results:
        print(f"│ {r['area']:<12} │ {r['duration_s']:>6}s │ ${r['cost_usd']:>5.4f} │ {r['features']:>8,} │ {r['size_mb']:>6.2f}M │   {r['types_count']:>2}    │")

    print("└──────────────┴──────────┴────────┴──────────┴─────────┴─────────┘")
    print()

    total_cost = sum(r['cost_usd'] for r in results)
    total_features = sum(r['features'] for r in results)
    total_time = sum(r['duration_s'] for r in results)

    print(f"Total Time: {total_time:.1f}s")
    print(f"Total Cost: ${total_cost:.6f}")
    print(f"Total Features: {total_features:,}")
    print()


def main():
    """Run all benchmarks."""
    print("BigQuery Overture Maps Benchmark")
    print("=================================")

    print("\nLoading GCP credentials...")
    try:
        gcp_creds = load_gcp_credentials()
        print(f"✓ Loaded credentials for project: {gcp_creds['project_id']}")
    except Exception as e:
        print(f"✗ Failed to load credentials: {e}")
        return 1

    results = []
    for area_name, bbox in AREAS.items():
        try:
            result = run_benchmark(area_name, bbox, ALL_TYPES, gcp_creds)
            if result:
                results.append(result)
        except Exception as e:
            print(f"\n✗ Benchmark failed for {area_name}: {e}")
            import traceback
            traceback.print_exc()

    if results:
        print_results_table(results)

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
