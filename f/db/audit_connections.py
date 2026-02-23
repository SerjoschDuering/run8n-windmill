import wmill
import psycopg2
from psycopg2.extras import RealDictCursor


def main(
    db: dict = None,
    min_idle_seconds: int = 0
) -> dict:
    """Audit active PostgreSQL connections.

    Shows all active connections, what they're doing, how long they've been
    idle, and what they're waiting on. Helps identify stuck or long-running
    connections that may be exhausting the connection pool.

    Args:
        db: Database resource (host, port, user, password, dbname).
            If not provided, uses f/resources/run8n_db
        min_idle_seconds: Only show connections idle longer than this
            (default 0 = show all)

    Returns:
        Dictionary with connection stats and list of active connections
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

        # Use RealDictCursor for dict results
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Query active connections
        query = """
        SELECT
            pid,
            usename,
            application_name,
            client_addr,
            backend_start,
            state,
            state_change,
            EXTRACT(EPOCH FROM (NOW() - state_change))::int AS idle_seconds,
            query,
            wait_event_type,
            wait_event
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()  -- Exclude this connection
        AND state IS NOT NULL
        ORDER BY state_change ASC;
        """

        cur.execute(query)
        all_connections = cur.fetchall()

        # Filter by min_idle_seconds
        connections = [
            c for c in all_connections
            if c['idle_seconds'] >= min_idle_seconds
        ]

        # Calculate summary stats
        total = len(all_connections)
        idle_count = len([c for c in all_connections if c['state'] == 'idle'])
        active_count = len([c for c in all_connections if c['state'] == 'active'])
        idle_in_transaction = len([c for c in all_connections if c['state'] == 'idle in transaction'])

        # Get max_connections setting
        cur.execute("SHOW max_connections;")
        max_connections = int(cur.fetchone()['max_connections'])

        # Convert timedelta to string for JSON serialization
        for conn_info in connections:
            if conn_info.get('backend_start'):
                conn_info['backend_start'] = str(conn_info['backend_start'])
            if conn_info.get('state_change'):
                conn_info['state_change'] = str(conn_info['state_change'])
            if conn_info.get('client_addr'):
                conn_info['client_addr'] = str(conn_info['client_addr'])

        return {
            "summary": {
                "total_connections": total,
                "max_connections": max_connections,
                "usage_percent": round((total / max_connections) * 100, 1),
                "by_state": {
                    "active": active_count,
                    "idle": idle_count,
                    "idle_in_transaction": idle_in_transaction
                }
            },
            "connections": connections,
            "filtered_count": len(connections),
            "filter_applied": f"idle >= {min_idle_seconds}s" if min_idle_seconds > 0 else "none"
        }

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
