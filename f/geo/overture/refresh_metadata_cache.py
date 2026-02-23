"""
Auto-refresh Overture Maps metadata cache when new releases are published.

Checks STAC catalog for latest release, compares with cached releases,
and triggers cache builder for new releases. Cleans up old releases (keeps last 2).
"""

import requests
import psycopg2
import wmill
from typing import Optional


def main(db: dict) -> dict:
    """
    Check for new Overture Maps releases and update cache if needed.

    Args:
        db: PostgreSQL resource (from Windmill resource picker)

    Returns:
        dict with update status, latest release, and cached releases
    """

    # Get latest release from STAC catalog
    try:
        catalog_url = "https://stac.overturemaps.org/catalog.json"
        catalog = requests.get(catalog_url, timeout=30).json()

        # Get releases from links
        release_links = [
            link for link in catalog.get("links", [])
            if link.get("rel") == "child" and "release" in link.get("href", "")
        ]

        if not release_links:
            return {"error": "No releases found in STAC catalog"}

        # Extract release IDs from URLs
        releases = []
        for link in release_links:
            href = link.get("href", "")
            release_id = href.rstrip("/").split("/")[-1]
            releases.append(release_id)

        latest_release = releases[-1] if releases else None
        if not latest_release:
            return {"error": "Could not determine latest release"}

        print(f"Latest release from STAC: {latest_release}")

    except Exception as e:
        return {"error": f"Failed to fetch STAC catalog: {str(e)}"}

    # Query database for existing releases
    try:
        conn = psycopg2.connect(**db)
        cur = conn.cursor()

        cur.execute(
            "SELECT DISTINCT release FROM overture_file_metadata ORDER BY release"
        )
        cached_releases = [row[0] for row in cur.fetchall()]
        print(f"Cached releases: {cached_releases}")

        cur.close()
        conn.close()

    except Exception as e:
        return {"error": f"Failed to query database: {str(e)}"}

    # Check if latest is already cached
    if latest_release in cached_releases:
        return {
            "updated": False,
            "latest_release": latest_release,
            "cached_releases": cached_releases,
            "message": f"Latest release {latest_release} is already cached"
        }

    # Trigger cache builder for new release
    print(f"New release detected: {latest_release}")
    print("Triggering cache builder...")

    try:
        # Run cache builder script with new release
        result = wmill.run_script_by_path(
            "f/geo/overture/build_metadata_cache",
            args={
                "db": db,
                "release": latest_release,
                "force_rebuild": False
            }
        )

        if "error" in result:
            return {
                "error": f"Cache builder failed: {result.get('error')}",
                "latest_release": latest_release,
                "cached_releases": cached_releases
            }

        print(f"✓ Cache built for release {latest_release}")
        cached_releases.append(latest_release)

    except Exception as e:
        return {
            "error": f"Failed to run cache builder: {str(e)}",
            "latest_release": latest_release,
            "cached_releases": cached_releases
        }

    # Clean up old releases (keep last 2 only)
    if len(cached_releases) > 2:
        old_releases = cached_releases[:-2]
        print(f"Cleaning up old releases: {old_releases}")

        try:
            conn = psycopg2.connect(**db)
            cur = conn.cursor()

            for old_release in old_releases:
                cur.execute(
                    "DELETE FROM overture_file_metadata WHERE release = %s",
                    (old_release,)
                )
                deleted = cur.rowcount
                print(f"Deleted {deleted} entries for release {old_release}")

            conn.commit()
            cur.close()
            conn.close()

            # Update cached releases list
            cached_releases = [r for r in cached_releases if r not in old_releases]

        except Exception as e:
            print(f"Warning: Failed to clean up old releases: {str(e)}")

    return {
        "updated": True,
        "latest_release": latest_release,
        "cached_releases": cached_releases,
        "message": f"Cache updated to release {latest_release}"
    }
