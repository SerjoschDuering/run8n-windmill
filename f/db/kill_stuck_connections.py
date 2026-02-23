import wmill
import psycopg2
from psycopg2.extras import RealDictCursor


def main(
    db: dict = None,
    min_idle_minutes: int = 30,
    dry_run: bool = True,
    exclude_users: list = None
) -> dict:
    """Terminate stuck/idle PostgreSQL connections.

    Safely kills connections that have been idle for too long. Includes
    safety checks to prevent killing important connections.

    IMPORTANT: Runs in dry-run mode by default. Set dry_run=False to
    actually kill connections.

    Args:
        db: Database resource (uses f/resources/run8n_db if not provided)
        min_idle_minutes: Kill connections idle longer than this (default 30)
        dry_run: If True, only show what would be killed (default True)
        exclude_users: List of usernames to never kill (default: ['postgres'])

    Returns:
        Dictionary with killed connections and summary
    """
    conn = None
    cur = None

    try:
        # Get database resource
        if not db:
            db = wmill.get_resource("f/resources/run8n_db")

        # Default excluded users
        if exclude_users is None:
            exclude_users = ['postgres']

        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=db.get("host"),
            port=db.get("port", 5432),
            user=db.get("user"),
            password=db.get("password"),
            dbname=db.get("dbname", "postgres")
        )

        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Find connections to kill
        min_idle_interval = f"{min_idle_minutes} minutes"

        # Build exclude clause
        exclude_clause = ", ".join([f"'{user}'" for user in exclude_users])

        query = f"""
        SELECT
            pid,
            usename,
            application_name,
            client_addr,
            state,
            state_change,
            EXTRACT(EPOCH FROM (NOW() - state_change))::int AS idle_seconds,
            query
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()  -- Don't kill self
        AND state = 'idle'
        AND state_change < NOW() - INTERVAL '{min_idle_interval}'
        AND usename NOT IN ({exclude_clause})
        ORDER BY state_change ASC;
        """

        cur.execute(query)
        connections_to_kill = cur.fetchall()

        # Convert data for JSON serialization
        for conn_info in connections_to_kill:
            if conn_info.get('state_change'):
                conn_info['state_change'] = str(conn_info['state_change'])
            if conn_info.get('client_addr'):
                conn_info['client_addr'] = str(conn_info['client_addr'])

        result = {
            "dry_run": dry_run,
            "min_idle_minutes": min_idle_minutes,
            "excluded_users": exclude_users,
            "found_count": len(connections_to_kill),
            "connections": connections_to_kill,
            "killed_pids": []
        }

        # Kill connections if not dry run
        if not dry_run and connections_to_kill:
            killed_count = 0
            for conn_info in connections_to_kill:
                pid = conn_info['pid']
                try:
                    cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                    killed = cur.fetchone()
                    if killed:
                        result["killed_pids"].append(pid)
                        killed_count += 1
                except Exception as e:
                    # Log error but continue
                    print(f"Failed to kill PID {pid}: {e}")

            conn.commit()
            result["killed_count"] = killed_count
        else:
            result["message"] = "DRY RUN - no connections killed. Set dry_run=False to actually terminate."

        return result

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
