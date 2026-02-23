import wmill
import psycopg2
from psycopg2.extras import RealDictCursor


def main(db: dict = None) -> dict:
    """Check PostgreSQL connection usage vs limits.

    Shows current connection count, maximum allowed, and breakdown by
    database and user. Helps identify when approaching connection limits.

    Args:
        db: Database resource (uses f/resources/run8n_db if not provided)

    Returns:
        Health report with connection limits and usage breakdown
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

        # Get max_connections
        cur.execute("SHOW max_connections;")
        max_connections = int(cur.fetchone()['max_connections'])

        # Get current connection count
        cur.execute("SELECT COUNT(*) as count FROM pg_stat_activity;")
        current_connections = cur.fetchone()['count']

        # Get connections by database
        cur.execute("""
            SELECT datname, COUNT(*) as count
            FROM pg_stat_activity
            GROUP BY datname
            ORDER BY count DESC;
        """)
        by_database = cur.fetchall()

        # Get connections by user
        cur.execute("""
            SELECT usename, COUNT(*) as count
            FROM pg_stat_activity
            GROUP BY usename
            ORDER BY count DESC;
        """)
        by_user = cur.fetchall()

        # Get connections by state
        cur.execute("""
            SELECT state, COUNT(*) as count
            FROM pg_stat_activity
            WHERE state IS NOT NULL
            GROUP BY state
            ORDER BY count DESC;
        """)
        by_state = cur.fetchall()

        # Get connections by application
        cur.execute("""
            SELECT application_name, COUNT(*) as count
            FROM pg_stat_activity
            WHERE application_name IS NOT NULL AND application_name != ''
            GROUP BY application_name
            ORDER BY count DESC;
        """)
        by_application = cur.fetchall()

        # Calculate health metrics
        usage_percent = (current_connections / max_connections) * 100
        available = max_connections - current_connections

        # Determine health status
        if usage_percent >= 90:
            health_status = "CRITICAL"
            message = "Connection pool nearly exhausted! Immediate action needed."
        elif usage_percent >= 80:
            health_status = "WARNING"
            message = "Connection pool usage high. Monitor closely."
        elif usage_percent >= 60:
            health_status = "CAUTION"
            message = "Connection usage elevated. Review connection patterns."
        else:
            health_status = "HEALTHY"
            message = "Connection usage within normal limits."

        return {
            "health_status": health_status,
            "message": message,
            "limits": {
                "max_connections": max_connections,
                "current_connections": current_connections,
                "available_connections": available,
                "usage_percent": round(usage_percent, 1)
            },
            "breakdown": {
                "by_database": by_database,
                "by_user": by_user,
                "by_state": by_state,
                "by_application": by_application
            }
        }

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
