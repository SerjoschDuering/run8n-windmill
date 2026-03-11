"""
Setup PostgreSQL cache table for BigQuery Overture results.

Creates table with spatial indexing for bbox queries and tracking
metadata (access counts, costs, sizes).

Run once to initialize, safe to re-run (uses IF NOT EXISTS).
"""

#extra_requirements:
#psycopg2-binary==2.9.10

import psycopg2


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bq_overture_cache (
    cache_key TEXT PRIMARY KEY,
    bbox GEOMETRY(POLYGON, 4326) NOT NULL,
    types TEXT[] NOT NULL,
    geojson JSONB NOT NULL,
    feature_count INTEGER NOT NULL,
    size_bytes BIGINT NOT NULL,
    bytes_billed BIGINT NOT NULL,
    cost_usd NUMERIC(10, 6) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    accessed_at TIMESTAMP DEFAULT NOW(),
    access_count INTEGER DEFAULT 1
);

COMMENT ON TABLE bq_overture_cache IS 'BigQuery Overture Maps result cache with cost tracking';
COMMENT ON COLUMN bq_overture_cache.cache_key IS 'SHA256 hash of version + bbox + types';
COMMENT ON COLUMN bq_overture_cache.bbox IS 'Query bounding box (for spatial queries)';
COMMENT ON COLUMN bq_overture_cache.types IS 'Overture types included (building, land, etc)';
COMMENT ON COLUMN bq_overture_cache.geojson IS 'Cached GeoJSON result';
COMMENT ON COLUMN bq_overture_cache.bytes_billed IS 'BigQuery bytes billed for this query';
COMMENT ON COLUMN bq_overture_cache.cost_usd IS 'Estimated query cost in USD';
"""

CREATE_INDEXES_SQL = """
-- Spatial index for bbox queries
CREATE INDEX IF NOT EXISTS idx_bq_cache_bbox_gist
ON bq_overture_cache USING GIST(bbox);

-- Index for cleanup by access time
CREATE INDEX IF NOT EXISTS idx_bq_cache_accessed
ON bq_overture_cache(accessed_at DESC);

-- Index for filtering by types
CREATE INDEX IF NOT EXISTS idx_bq_cache_types
ON bq_overture_cache USING GIN(types);

-- Index for cost analysis
CREATE INDEX IF NOT EXISTS idx_bq_cache_cost
ON bq_overture_cache(cost_usd DESC);

-- Index for access frequency analysis
CREATE INDEX IF NOT EXISTS idx_bq_cache_access_count
ON bq_overture_cache(access_count DESC);
"""


def main(db: dict) -> dict:
    """
    Create BigQuery Overture cache table and indexes.

    Args:
        db: PostgreSQL connection dict

    Returns:
        Status dict with table/index creation results
    """
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**dict(db))
        cur = conn.cursor()

        # Create table
        cur.execute(CREATE_TABLE_SQL)
        print("✓ Table bq_overture_cache created/verified")

        # Create indexes
        cur.execute(CREATE_INDEXES_SQL)
        print("✓ Indexes created/verified")

        conn.commit()

        # Get table stats
        cur.execute(
            """
            SELECT
                COUNT(*) as total_entries,
                SUM(feature_count) as total_features,
                SUM(size_bytes) as total_size_bytes,
                SUM(bytes_billed) as total_bytes_billed,
                SUM(cost_usd) as total_cost_usd,
                MAX(accessed_at) as last_access
            FROM bq_overture_cache
            """
        )
        stats = cur.fetchone()

        total_entries, total_features, total_size_bytes, total_bytes_billed, total_cost_usd, last_access = stats

        return {
            "status": "success",
            "table": "bq_overture_cache",
            "cache_stats": {
                "total_entries": total_entries or 0,
                "total_features_cached": total_features or 0,
                "total_size_mb": round((total_size_bytes or 0) / 1024 / 1024, 2),
                "total_bytes_billed": total_bytes_billed or 0,
                "total_cost_usd": float(total_cost_usd or 0),
                "last_access": str(last_access) if last_access else None
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

    finally:
        # Proper resource cleanup
        if cur:
            cur.close()
        if conn:
            conn.close()
