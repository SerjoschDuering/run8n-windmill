"""
Build Overture Maps spatial metadata cache from STAC catalog.

Downloads STAC catalog (collections.parquet), extracts file metadata
(bbox, S3 paths, row counts), and populates PostgreSQL cache for fast
spatial queries. Caches all Overture types by default.
"""

import pyarrow.parquet as pq
import pyarrow.compute as pc
import psycopg2
from typing import Optional
import requests
import io
import wmill

CACHE_DB_RESOURCE = "f/geo/overture/postgres_db"


ALL_COLLECTIONS = [
    "building", "building_part", "land", "water", "land_use",
    "land_cover", "infrastructure", "bathymetry", "place",
    "segment", "connector", "address",
    "division", "division_area", "division_boundary",
]

INSERT_SQL = """
    INSERT INTO overture_file_metadata
    (s3_path, s3_bucket, s3_key, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
     bbox, release, collection, num_rows, num_row_groups)
    VALUES (%s, %s, %s, %s, %s, %s, %s,
            ST_MakeEnvelope(%s, %s, %s, %s, 4326), %s, %s, %s, %s)
    ON CONFLICT (s3_path) DO NOTHING
"""


def _extract_s3_path(assets: dict) -> str:
    """Extract S3 path from STAC asset metadata."""
    if not isinstance(assets, dict):
        return ""
    aws_asset = assets.get("aws", {})
    if not isinstance(aws_asset, dict):
        return ""
    alternate = aws_asset.get("alternate", {})
    if not isinstance(alternate, dict):
        return ""
    s3_info = alternate.get("s3", {})
    if not isinstance(s3_info, dict):
        return ""
    return s3_info.get("href", "")


def _build_rows_from_table(table, release: str, collection: str) -> list[tuple]:
    """Build insert tuples from a PyArrow table. Skips rows with invalid data."""
    cols = table.to_pydict()
    bbox_list = cols.get("bbox", [])
    assets_list = cols.get("assets", [])
    num_rows_list = cols.get("num_rows", [])
    num_row_groups_list = cols.get("num_row_groups", [])
    n = table.num_rows
    rows = []

    for i in range(n):
        bbox = bbox_list[i] if i < len(bbox_list) else {}
        if not isinstance(bbox, dict) or not all(
            k in bbox for k in ["xmin", "ymin", "xmax", "ymax"]
        ):
            continue

        xmin = float(bbox["xmin"])
        ymin = float(bbox["ymin"])
        xmax = float(bbox["xmax"])
        ymax = float(bbox["ymax"])

        assets = assets_list[i] if i < len(assets_list) else {}
        s3_path = _extract_s3_path(assets)
        if not s3_path:
            continue

        if s3_path.startswith("s3://"):
            parts = s3_path[5:].split("/", 1)
            s3_bucket = parts[0] if len(parts) > 0 else ""
            s3_key = parts[1] if len(parts) > 1 else ""
        else:
            s3_bucket = ""
            s3_key = s3_path

        nr = int(num_rows_list[i]) if i < len(num_rows_list) else 0
        nrg = int(num_row_groups_list[i]) if i < len(num_row_groups_list) else 0

        rows.append((
            s3_path, s3_bucket, s3_key,
            xmin, ymin, xmax, ymax,
            xmin, ymin, xmax, ymax,
            release, collection, nr, nrg,
        ))

    return rows


def main(
    db: Optional[dict] = None,
    release: Optional[str] = None,
    collections: Optional[list[str]] = None,
    force_rebuild: bool = False,
) -> dict:
    """
    Build spatial metadata cache from Overture Maps STAC catalog.

    Args:
        db: PostgreSQL connection (auto-resolved from Windmill resource if omitted)
        release: Overture version (default: latest from STAC catalog)
        collections: Which collections to cache (default: all 15 types)
        force_rebuild: Rebuild even if cache exists for this release

    Returns:
        dict with status, release, and per-collection counts
    """
    if not db:
        db = wmill.get_resource(CACHE_DB_RESOURCE)

    if collections is None:
        collections = list(ALL_COLLECTIONS)

    # Get latest release if not specified
    if not release:
        catalog_url = "https://stac.overturemaps.org/catalog.json"
        catalog = requests.get(catalog_url, timeout=30).json()
        release = catalog.get("latest")
        if not release:
            release_links = [
                link for link in catalog.get("links", [])
                if link.get("rel") == "child" and link.get("latest") == True
            ]
            if release_links:
                latest_link = release_links[0]["href"]
                release = latest_link.replace("./", "").split("/")[0]
            else:
                return {"error": "No releases found in STAC catalog"}

    print(f"Release: {release}, collections: {collections}")

    conn = psycopg2.connect(**db)
    cur = conn.cursor()

    # Check existing cache per collection
    if not force_rebuild:
        cur.execute(
            "SELECT collection, COUNT(*) FROM overture_file_metadata "
            "WHERE release = %s GROUP BY collection",
            (release,),
        )
        existing = dict(cur.fetchall())
        already_cached = [c for c in collections if c in existing]
        if already_cached:
            missing = [c for c in collections if c not in existing]
            if not missing:
                cur.close()
                conn.close()
                return {
                    "status": "skipped",
                    "release": release,
                    "cached": existing,
                    "message": "All requested collections already cached. "
                    "Use force_rebuild=True to rebuild.",
                }
            print(f"Already cached: {already_cached}. Building: {missing}")
            collections = missing

    # Download STAC catalog
    stac_url = f"https://stac.overturemaps.org/{release}/collections.parquet"
    print(f"Downloading: {stac_url}")

    try:
        response = requests.get(stac_url, timeout=60)
        response.raise_for_status()
    except Exception as e:
        cur.close()
        conn.close()
        return {"error": f"Failed to download STAC catalog: {str(e)}"}

    parquet_bytes = io.BytesIO(response.content)
    catalog_table = pq.read_table(parquet_bytes)

    if force_rebuild:
        for coll in collections:
            cur.execute(
                "DELETE FROM overture_file_metadata "
                "WHERE release = %s AND collection = %s",
                (release, coll),
            )
            if cur.rowcount:
                print(f"Deleted {cur.rowcount} existing {coll} entries")

    # Process each collection using PyArrow filtering (no pandas)
    results = {}
    total_inserted = 0
    batch_size = 50

    for coll in collections:
        mask = pc.equal(catalog_table.column("collection"), coll)
        coll_table = catalog_table.filter(mask)
        print(f"[{coll}] {coll_table.num_rows} files in catalog")

        if coll_table.num_rows == 0:
            results[coll] = {"found": 0, "inserted": 0}
            continue

        all_rows = _build_rows_from_table(coll_table, release, coll)
        inserted = 0

        for start in range(0, len(all_rows), batch_size):
            batch = all_rows[start : start + batch_size]
            cur.executemany(INSERT_SQL, batch)
            inserted += len(batch)

        results[coll] = {"found": coll_table.num_rows, "inserted": inserted}
        total_inserted += inserted
        print(f"[{coll}] inserted {inserted} files")

    conn.commit()
    cur.close()
    conn.close()

    print(f"Cache build complete: {total_inserted} files inserted")

    return {
        "status": "success",
        "release": release,
        "total_inserted": total_inserted,
        "collections": results,
    }
