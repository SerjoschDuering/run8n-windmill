"""
Clean up BigQuery Overture cache using hybrid TTL + LRU strategy.

Strategy:
- Remove entries older than max_age_days (default 30)
- Keep only top N most-accessed entries (default 1000)
- Prioritize keeping high-access, recent entries

Designed to run daily via Windmill cron schedule.
"""

#extra_requirements:
#psycopg2-binary==2.9.10

import psycopg2


def main(
    db: dict,
    max_age_days: int = 30,
    max_entries: int = 1000,
    dry_run: bool = False
) -> dict:
    """
    Clean up BigQuery Overture cache.

    Args:
        db: PostgreSQL connection dict
        max_age_days: Remove entries older than this (default 30)
        max_entries: Keep only this many entries (default 1000, by access_count)
        dry_run: If True, report what would be deleted without deleting

    Returns:
        Cleanup stats (deleted counts, sizes, cost savings)
    """
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(**dict(db))
        cur = conn.cursor()

        # Get stats before cleanup
        cur.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(size_bytes) as total_size,
                SUM(cost_usd) as total_cost
            FROM bq_overture_cache
            """
        )
        before_stats = cur.fetchone()
        before_count, before_size, before_cost = before_stats

        # CRITICAL FIX #2: Use parameterized queries (no f-strings)
        # Find entries to delete by age
        cur.execute(
            """
            SELECT cache_key, size_bytes, cost_usd, created_at
            FROM bq_overture_cache
            WHERE created_at < NOW() - make_interval(days => %s)
            ORDER BY created_at
            """,
            (max_age_days,)
        )
        age_deletes = cur.fetchall()

        # Find entries to delete by LRU (keep top max_entries by access_count)
        cur.execute(
            """
            SELECT cache_key, size_bytes, cost_usd, access_count
            FROM bq_overture_cache
            ORDER BY access_count DESC, accessed_at DESC
            OFFSET %s
            """,
            (max_entries,)
        )
        lru_deletes = cur.fetchall()

        # Combine delete lists (deduplicate by cache_key)
        delete_keys = set()
        deleted_size = 0
        deleted_cost = 0.0

        for row in age_deletes:
            cache_key, size_bytes, cost_usd, _ = row
            if cache_key not in delete_keys:
                delete_keys.add(cache_key)
                deleted_size += size_bytes or 0
                deleted_cost += float(cost_usd or 0)

        for row in lru_deletes:
            cache_key, size_bytes, cost_usd, _ = row
            if cache_key not in delete_keys:
                delete_keys.add(cache_key)
                deleted_size += size_bytes or 0
                deleted_cost += float(cost_usd or 0)

        deleted_count = len(delete_keys)

        # Execute deletions (unless dry run)
        if delete_keys and not dry_run:
            cur.execute(
                """
                DELETE FROM bq_overture_cache
                WHERE cache_key = ANY(%s)
                """,
                (list(delete_keys),)
            )
            conn.commit()
            print(f"✓ Deleted {deleted_count} cache entries")
        elif dry_run and delete_keys:
            print(f"[DRY RUN] Would delete {deleted_count} entries")
        else:
            print("No entries to delete")

        # Get stats after cleanup
        cur.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(size_bytes) as total_size,
                SUM(cost_usd) as total_cost
            FROM bq_overture_cache
            """
        )
        after_stats = cur.fetchone()
        after_count, after_size, after_cost = after_stats

        return {
            "status": "success",
            "dry_run": dry_run,
            "cleanup_params": {
                "max_age_days": max_age_days,
                "max_entries": max_entries
            },
            "before": {
                "entries": before_count or 0,
                "size_mb": round((before_size or 0) / 1024 / 1024, 2),
                "total_cost_usd": float(before_cost or 0)
            },
            "deleted": {
                "entries": deleted_count,
                "size_mb": round(deleted_size / 1024 / 1024, 2),
                "cost_represented_usd": round(deleted_cost, 4),
                "by_age": len(age_deletes),
                "by_lru": len(lru_deletes) - len([k for k in lru_deletes if k[0] in {r[0] for r in age_deletes}])
            },
            "after": {
                "entries": after_count or 0,
                "size_mb": round((after_size or 0) / 1024 / 1024, 2),
                "total_cost_usd": float(after_cost or 0)
            }
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

    finally:
        # CRITICAL FIX #3: Proper resource cleanup
        if cur:
            cur.close()
        if conn:
            conn.close()
