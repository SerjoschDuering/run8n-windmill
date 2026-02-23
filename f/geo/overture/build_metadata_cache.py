"""
Build Overture Maps spatial metadata cache from STAC catalog.

Downloads STAC catalog (collections.parquet), extracts file metadata
(bbox, S3 paths, row counts), and populates PostgreSQL cache for fast
spatial queries. Reduces query time from 6min to 30-60s.
"""

import pyarrow.parquet as pq
import psycopg2
from typing import Optional
import requests
import io


def main(
    db: dict,
    release: Optional[str] = None,
    force_rebuild: bool = False
) -> dict:
    """
    Build spatial metadata cache from Overture Maps STAC catalog.

    Args:
        db: PostgreSQL resource (from Windmill resource picker)
        release: Overture version (default: latest from STAC catalog)
        force_rebuild: Rebuild even if cache exists for this release

    Returns:
        dict with status, release, files_found, and inserted count
    """

    # Get latest release if not specified
    if not release:
        catalog_url = "https://stac.overturemaps.org/catalog.json"
        catalog = requests.get(catalog_url, timeout=30).json()
        # Get latest release from catalog
        release = catalog.get("latest")
        if not release:
            # Fallback: get from links
            release_links = [
                link for link in catalog.get("links", [])
                if link.get("rel") == "child" and link.get("latest") == True
            ]
            if release_links:
                # Extract release from URL
                latest_link = release_links[0]["href"]
                release = latest_link.replace("./", "").split("/")[0]
            else:
                return {"error": "No releases found in STAC catalog"}

    print(f"Using Overture Maps release: {release}")

    # Connect to PostgreSQL
    conn = psycopg2.connect(**db)
    cur = conn.cursor()

    # Check if release already exists
    if not force_rebuild:
        cur.execute(
            "SELECT COUNT(*) FROM overture_file_metadata WHERE release = %s",
            (release,)
        )
        existing = cur.fetchone()[0]
        if existing > 0:
            cur.close()
            conn.close()
            return {
                "status": "skipped",
                "release": release,
                "message": f"Cache already exists with {existing} files. Use force_rebuild=True to rebuild."
            }

    # Download STAC collections catalog
    stac_url = f"https://stac.overturemaps.org/{release}/collections.parquet"
    print(f"Downloading STAC catalog from: {stac_url}")

    try:
        response = requests.get(stac_url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        cur.close()
        conn.close()
        return {"error": f"Failed to download STAC catalog: {str(e)}"}

    # Read Parquet file from bytes
    parquet_bytes = io.BytesIO(response.content)
    table = pq.read_table(parquet_bytes)
    df = table.to_pandas()

    # Filter for building collection
    building_files = df[df["collection"] == "building"]
    print(f"Found {len(building_files)} building files in release {release}")

    if len(building_files) == 0:
        cur.close()
        conn.close()
        return {
            "error": "No building files found in STAC catalog",
            "release": release
        }

    # Delete existing entries for this release if force_rebuild
    if force_rebuild:
        cur.execute("DELETE FROM overture_file_metadata WHERE release = %s", (release,))
        print(f"Deleted {cur.rowcount} existing entries for release {release}")

    # Batch insert file metadata
    inserted = 0
    batch_size = 50
    batch_data = []

    for idx, row in building_files.iterrows():
        # Extract bbox coordinates
        bbox = row.get("bbox", [])
        if len(bbox) != 4:
            print(f"Warning: Invalid bbox for file {idx}, skipping")
            continue

        xmin, ymin, xmax, ymax = bbox

        # Extract S3 path from assets
        assets = row.get("assets", {})
        if not isinstance(assets, dict):
            print(f"Warning: Invalid assets for file {idx}, skipping")
            continue

        # Get S3 path from aws.alternate.s3.href
        aws_asset = assets.get("aws", {})
        if isinstance(aws_asset, dict):
            alternate = aws_asset.get("alternate", {})
            if isinstance(alternate, dict):
                s3_info = alternate.get("s3", {})
                if isinstance(s3_info, dict):
                    s3_path = s3_info.get("href", "")
                else:
                    s3_path = ""
            else:
                s3_path = ""
        else:
            s3_path = ""

        if not s3_path:
            print(f"Warning: No S3 path for file {idx}, skipping")
            continue

        # Parse S3 bucket and key from s3:// URL
        if s3_path.startswith("s3://"):
            parts = s3_path[5:].split("/", 1)
            s3_bucket = parts[0] if len(parts) > 0 else ""
            s3_key = parts[1] if len(parts) > 1 else ""
        else:
            s3_bucket = ""
            s3_key = s3_path

        # Get row counts
        num_rows = int(row.get("num_rows", 0))
        num_row_groups = int(row.get("num_row_groups", 0))

        # Add to batch
        batch_data.append((
            s3_path,
            s3_bucket,
            s3_key,
            xmin, ymin, xmax, ymax,
            release,
            "building",
            num_rows,
            num_row_groups
        ))

        # Insert batch when full
        if len(batch_data) >= batch_size:
            cur.executemany(
                """
                INSERT INTO overture_file_metadata
                (s3_path, s3_bucket, s3_key, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
                 bbox, release, collection, num_rows, num_row_groups)
                VALUES (%s, %s, %s, %s, %s, %s, %s,
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326), %s, %s, %s, %s)
                ON CONFLICT (s3_path) DO NOTHING
                """,
                batch_data
            )
            inserted += len(batch_data)
            batch_data = []
            print(f"Inserted batch, total: {inserted} files")

    # Insert remaining batch
    if batch_data:
        cur.executemany(
            """
            INSERT INTO overture_file_metadata
            (s3_path, s3_bucket, s3_key, bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
             bbox, release, collection, num_rows, num_row_groups)
            VALUES (%s, %s, %s, %s, %s, %s, %s,
                    ST_MakeEnvelope(%s, %s, %s, %s, 4326), %s, %s, %s, %s)
            ON CONFLICT (s3_path) DO NOTHING
            """,
            batch_data
        )
        inserted += len(batch_data)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✓ Cache build complete: {inserted} files inserted for release {release}")

    return {
        "status": "success",
        "release": release,
        "files_found": len(building_files),
        "inserted": inserted
    }
