"""
Fetch features from Overture Maps (free, global coverage).

Supports all 15 Overture feature types across 6 themes.
Returns GeoJSON FeatureCollection. No authentication needed.
Uses PyArrow + shapely only (no geopandas/GDAL required).

Usage:
    result = main(west=11.5, south=48.1, east=11.6, north=48.2,
                  types=["building", "place", "infrastructure"])
"""

#extra_requirements:
#overturemaps==0.19.0
#psycopg2-binary==2.9.10
#wmill>=1.0.0

from typing import Optional
import json
import time
import uuid

import wmill
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds
import pyarrow.fs as fs
import shapely


VALID_TYPES = {
    "building", "building_part", "land", "water", "land_use",
    "land_cover", "infrastructure", "bathymetry", "place",
    "segment", "connector", "address",
    "division", "division_area", "division_boundary",
}
SKIP_COLUMNS = {"geometry", "bbox"}

# Column projection per type — only read what we need from S3
_BASE_COLS = ["id", "geometry", "bbox", "names", "sources", "class", "subtype"]
COLUMNS_BY_TYPE = {
    "building": _BASE_COLS + [
        "height", "num_floors", "has_parts",
        "facade_material", "facade_color",
        "roof_shape", "roof_material", "roof_color",
    ],
    "building_part": _BASE_COLS + [
        "height", "num_floors", "building_id",
        "facade_material", "roof_shape", "roof_material",
    ],
    "place": _BASE_COLS + ["categories", "confidence", "addresses", "websites"],
    "land": _BASE_COLS + ["elevation", "surface", "source_tags", "wikidata"],
    "water": _BASE_COLS + ["is_salt", "is_intermittent"],
    "land_use": _BASE_COLS,
    "land_cover": ["id", "geometry", "bbox", "sources", "subtype", "cartography"],
    "infrastructure": _BASE_COLS + [
        "height", "surface", "source_tags", "wikidata",
    ],
    "bathymetry": ["id", "geometry", "bbox", "sources", "depth"],
    "segment": [
        "id", "geometry", "bbox", "names", "sources",
        "class", "subtype", "subclass", "road_surface",
        "road_flags", "speed_limits", "access_restrictions",
        "routes", "connectors",
    ],
    "connector": ["id", "geometry", "bbox", "sources"],
    "address": [
        "id", "geometry", "bbox", "sources",
        "country", "street", "number", "unit", "postcode",
        "address_levels",
    ],
    "division": _BASE_COLS + ["population", "admin_level", "wikidata"],
    "division_area": _BASE_COLS + ["division_id", "admin_level"],
    "division_boundary": ["id", "geometry", "bbox", "sources"],
}


def _safe_columns(schema_names: set, columns: list[str]) -> list[str]:
    """Filter requested columns to only those present in the schema."""
    return [c for c in columns if c in schema_names]


def _get_intersecting_files(
    west: float, south: float, east: float, north: float,
    cur, collection: str,
) -> list[str]:
    """Query PostgreSQL cache for S3 paths of files intersecting bbox."""
    cur.execute(
        """
        SELECT s3_path FROM overture_file_metadata
        WHERE collection = %s
          AND ST_Intersects(bbox, ST_MakeEnvelope(%s, %s, %s, %s, 4326))
        ORDER BY s3_path
        """,
        (collection, west, south, east, north),
    )
    return [row[0] for row in cur.fetchall()]


def _read_from_cache(
    west: float, south: float, east: float, north: float,
    cur, collection: str,
) -> tuple[Optional[pa.Table], int]:
    """Read features using PostgreSQL cache + direct PyArrow S3."""
    files = _get_intersecting_files(west, south, east, north, cur, collection)
    if not files:
        return None, 0

    s3_paths = [f[5:] if f.startswith("s3://") else f for f in files]
    s3 = fs.S3FileSystem(anonymous=True, region="us-west-2")
    filter_expr = (
        (pc.field("bbox", "xmin") < east)
        & (pc.field("bbox", "xmax") > west)
        & (pc.field("bbox", "ymin") < north)
        & (pc.field("bbox", "ymax") > south)
    )

    dataset = pds.dataset(s3_paths, filesystem=s3)
    available = set(dataset.schema.names)
    cols = _safe_columns(available, COLUMNS_BY_TYPE.get(collection, []))
    table = dataset.to_table(filter=filter_expr, columns=cols or None)
    return table, len(files)


def _read_from_stac(
    west: float, south: float, east: float, north: float,
    overture_type: str,
) -> Optional[pa.Table]:
    """Read features using overturemaps STAC catalog (no cache needed)."""
    from overturemaps.core import record_batch_reader

    reader = record_batch_reader(
        overture_type=overture_type,
        bbox=(west, south, east, north),
        stac=True,
    )
    if reader is None:
        print(f"  [{overture_type}] STAC returned no reader")
        return None

    table = reader.read_all()

    # Project down to expected columns (Bug 2 fix: STAC reads all cols)
    wanted = COLUMNS_BY_TYPE.get(overture_type, [])
    if wanted and table.num_rows > 0:
        available = set(table.schema.names)
        cols = _safe_columns(available, wanted)
        if cols:
            table = table.select(cols)

    return table


def _table_to_geojson(
    table: pa.Table, overture_type: str,
    clip_box: tuple[float, float, float, float] | None = None,
) -> list[dict]:
    """Convert PyArrow table to GeoJSON features using vectorized ops."""
    if table is None or table.num_rows == 0:
        return []

    geom_col = table.column("geometry").to_pylist()
    geom_array = shapely.from_wkb(geom_col)

    # Clip to bbox — removes global features like Aleutian Islands
    if clip_box is not None:
        bbox_geom = shapely.box(*clip_box)
        geom_array = shapely.intersection(geom_array, bbox_geom)

    geojson_strings = shapely.to_geojson(geom_array)

    # Build property columns (skip geometry/bbox, already handled)
    prop_cols = {}
    for name in table.schema.names:
        if name not in SKIP_COLUMNS:
            prop_cols[name] = table.column(name).to_pylist()

    features = []
    for i in range(table.num_rows):
        if geom_array[i] is None or shapely.is_empty(geom_array[i]):
            continue
        # Bug 1 fix: skip GeometryCollection from intersection artifacts
        if shapely.get_type_id(geom_array[i]) == 7:
            continue

        geojson_geom = json.loads(geojson_strings[i])

        props = {"overture_type": overture_type}
        for col, vals in prop_cols.items():
            props[col] = vals[i]

        # Convenience: extract primary name
        names_raw = props.get("names")
        if isinstance(names_raw, dict):
            props["name"] = names_raw.get("primary")

        # Convenience: extract first source dataset
        sources_raw = props.get("sources")
        if isinstance(sources_raw, list) and sources_raw:
            first = sources_raw[0]
            if isinstance(first, dict):
                props["source"] = first.get("dataset", "unknown")

        features.append({
            "type": "Feature",
            "id": props.get("id", ""),
            "geometry": geojson_geom,
            "properties": props,
        })

    return features


def _fetch_type(
    west: float, south: float, east: float, north: float,
    overture_type: str, cur=None,
) -> tuple[list[dict], dict]:
    """Fetch a single Overture type. Returns (features, type_metadata)."""
    table = None
    cache_files = None

    if cur is not None:
        try:
            table, cache_files = _read_from_cache(
                west, south, east, north, cur, overture_type,
            )
            print(f"  [{overture_type}] cache: {cache_files} files")
        except Exception as e:
            print(f"  [{overture_type}] cache fail ({e}), STAC fallback")

    if table is None:
        print(f"  [{overture_type}] using STAC catalog")
        table = _read_from_stac(west, south, east, north, overture_type)

    features = _table_to_geojson(
        table, overture_type, clip_box=(west, south, east, north),
    )
    meta = {"type": overture_type, "count": len(features)}
    if cache_files is not None:
        meta["cache_files"] = cache_files
    return features, meta


CACHE_DB_RESOURCE = "f/geo/overture/postgres_db"


def main(
    west: float,
    south: float,
    east: float,
    north: float,
    types: list[str] = ["building"],
    use_cache: bool = True,
    db: Optional[dict] = None,
    store_s3: bool = True,
) -> dict:
    bbox = {"west": west, "south": south, "east": east, "north": north}

    invalid = [t for t in types if t not in VALID_TYPES]
    if invalid:
        return {
            "type": "FeatureCollection", "features": [],
            "metadata": {
                "error": f"Invalid: {invalid}. Valid: {sorted(VALID_TYPES)}",
                "bbox": bbox, "total_features": 0,
            },
        }

    if use_cache and not db:
        try:
            db = wmill.get_resource(CACHE_DB_RESOURCE)
        except Exception as e:
            print(f"Failed to resolve DB resource {CACHE_DB_RESOURCE}: {e}")

    if use_cache and not db:
        return {
            "type": "FeatureCollection", "features": [],
            "metadata": {
                "source": "overture", "bbox": bbox, "total_features": 0,
                "error": "db parameter required when use_cache=True",
            },
        }

    # Bug 6 fix: use try/finally for DB connection cleanup
    cur = None
    conn = None
    try:
        if use_cache and db:
            import psycopg2
            try:
                conn = psycopg2.connect(**dict(db))
                cur = conn.cursor()
            except Exception as e:
                print(f"Cache DB failed ({e}), using STAC for all")

        all_features = []
        type_stats = []
        for t in types:
            features, meta = _fetch_type(west, south, east, north, t, cur)
            all_features.extend(features)
            type_stats.append(meta)
            print(f"  [{t}] -> {meta['count']} features")

        metadata = {
            "source": "overture", "bbox": bbox,
            "total_features": len(all_features),
            "types_requested": types,
            "type_breakdown": type_stats,
            "cache_enabled": use_cache and db is not None,
        }

        result = {
            "type": "FeatureCollection",
            "features": all_features,
            "metadata": metadata,
        }

        if store_s3 and all_features:
            try:
                ts = int(time.time())
                types_slug = "_".join(sorted(types))
                uid = uuid.uuid4().hex[:8]
                s3_key = f"geo/overture/{types_slug}_{ts}_{uid}.geojson"
                s3_ref = {"s3": s3_key}
                data = json.dumps(result).encode()
                size_mb = round(len(data) / 1_048_576, 1)
                print(f"  Writing {size_mb} MB to s3://{s3_key} ...")
                wmill.write_s3_file(s3_ref, data)
                print(f"  S3 write OK")
                return {
                    "s3": s3_key, "metadata": metadata,
                    "size_mb": size_mb,
                }
            except Exception as s3_err:
                print(f"  S3 write failed: {s3_err}, returning inline")

        return result

    except Exception as e:
        print(f"  ERROR: {e}")
        return {
            "type": "FeatureCollection", "features": [],
            "metadata": {
                "source": "overture", "error": str(e),
                "bbox": bbox, "total_features": 0,
            },
        }
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
