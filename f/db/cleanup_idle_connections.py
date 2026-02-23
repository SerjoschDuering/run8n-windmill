import wmill
import psycopg2
from psycopg2.extras import RealDictCursor


def main(
    db: dict = None,
    max_idle_minutes: int = 30,
    enable_cleanup: bool = False
) -> dict:
    """Scheduled cleanup of idle PostgreSQL connections.

    Designed to run on a schedule (e.g., every 15 minutes) to prevent
    connection pool exhaustion.

    SAFETY: Disabled by default. Set enable_cleanup=True to activate.

    Args:
        db: Database resource (uses f/resources/run8n_db if not provided)
        max_idle_minutes: Kill connections idle longer than this (default 30)
        enable_cleanup: Must be True to actually kill connections (default False)

    Returns:
        Cleanup summary with killed connections
    """
    conn = None
    cur = None

    try:
        # Get database resource
        if not db:
            db = wmill.get_resource("f/resources/run8n_db")

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db.get("host"),
            port=db.get("port", 5432),
            user=db.get("user"),
            password=db.get("password"),
            dbname=db.get("dbname", "postgres")
        )

        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Check current usage first
        cur.execute("SHOW max_connections;")
        max_connections = int(cur.fetchone()['max_connections'])

        cur.execute("SELECT COUNT(*) as count FROM pg_stat_activity;")
        current_connections = cur.fetchone()['count']

        usage_percent = (current_connections / max_connections) * 100

        # Find idle connections
        max_idle_interval = f"{max_idle_minutes} minutes"

        cur.execute(f"""
            SELECT
                pid,
                usename,
                application_name,
                state_change,
                EXTRACT(EPOCH FROM (NOW() - state_change))::int AS idle_seconds
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
            AND state = 'idle'
            AND state_change < NOW() - INTERVAL '{max_idle_interval}'
            AND usename NOT IN ('postgres', 'replication_user')
            ORDER BY state_change ASC;
        """)

        idle_connections = cur.fetchall()

        # Get current timestamp using Python datetime (avoiding SQL cursor issue)
        from datetime import datetime, timezone

        result = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "enabled": enable_cleanup,
            "max_idle_minutes": max_idle_minutes,
            "connection_usage": {
                "current": current_connections,
                "max": max_connections,
                "percent": round(usage_percent, 1)
            },
            "idle_found": len(idle_connections),
            "killed_count": 0,
            "killed_pids": []
        }

        # Kill connections if enabled
        if enable_cleanup and idle_connections:
            for conn_info in idle_connections:
                pid = conn_info['pid']
                try:
                    cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                    result["killed_pids"].append(pid)
                    result["killed_count"] += 1
                except Exception as e:
                    print(f"Failed to kill PID {pid}: {e}")

            conn.commit()
            result["message"] = f"Killed {result['killed_count']} idle connections"
        elif idle_connections:
            result["message"] = f"Found {len(idle_connections)} idle connections but cleanup disabled"
        else:
            result["message"] = "No idle connections to clean up"

        return result

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
