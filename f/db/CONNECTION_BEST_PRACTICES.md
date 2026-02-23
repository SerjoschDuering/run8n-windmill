# PostgreSQL Connection Management Best Practices

## The Problem

**Long-running Windmill scripts MUST NOT hold database connections open.**

PostgreSQL has a fixed connection limit (default 100). When scripts hold connections open during long operations (API calls, data processing, sleeping), the connection pool gets exhausted, causing "too many clients already" errors.

---

## The Solution

**Open → Query → Close → Process → Reopen if needed**

### ❌ BAD: Holding Connection During Long Operations

```python
import psycopg2
import requests
import time

def main():
    conn = psycopg2.connect(...)  # Connection opened
    cur = conn.cursor()

    # Query database
    cur.execute("SELECT * FROM data WHERE id = %s", (123,))
    data = cur.fetchone()

    # PROBLEM: Connection still open during these long operations
    response = requests.get("https://api.example.com/process", json=data)
    time.sleep(60)  # Waiting for async job
    result = requests.get(f"https://api.example.com/result/{response.json()['job_id']}")

    # Update database
    cur.execute("UPDATE data SET result = %s WHERE id = %s", (result.text, 123))
    conn.commit()

    # Connection held open for 60+ seconds!
    conn.close()
```

**Problem:** Connection held open for 60+ seconds while waiting for API calls.

---

### ✅ GOOD: Close Connection During Long Operations

```python
import psycopg2
import requests
import time
import wmill

def main():
    db = wmill.get_resource("f/resources/run8n_db")

    # STEP 1: Get data from database
    conn = None
    cur = None  # ✅ Initialize before try block
    try:
        conn = psycopg2.connect(connect_timeout=5, **db)  # ✅ Add timeout
        cur = conn.cursor()
        cur.execute("SELECT * FROM data WHERE id = %s", (123,))
        data = cur.fetchone()
    finally:
        if cur: cur.close()
        if conn: conn.close()  # ✅ Connection closed immediately

    # STEP 2: Do long processing WITHOUT holding connection
    response = requests.get("https://api.example.com/process", json=data)
    time.sleep(60)
    result = requests.get(f"https://api.example.com/result/{response.json()['job_id']}")

    # STEP 3: Reopen connection to save results
    conn = None
    cur = None  # ✅ Initialize before try block
    try:
        conn = psycopg2.connect(connect_timeout=5, **db)  # ✅ Add timeout
        cur = conn.cursor()
        cur.execute("UPDATE data SET result = %s WHERE id = %s", (result.text, 123))
        conn.commit()
    finally:
        if cur: cur.close()
        if conn: conn.close()  # ✅ Connection closed again
```

**Solution:** Connection only open during actual database operations (~1 second each).

---

## Common Patterns

### Pattern 1: Context Manager (Preferred)

```python
import wmill
import psycopg2

def main():
    db = wmill.get_resource("f/resources/run8n_db")

    # Context manager automatically handles cleanup
    with psycopg2.connect(connect_timeout=5, **db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM data WHERE id = %s", (123,))
            data = cur.fetchone()
    # ✅ Connection and cursor auto-closed when exiting `with` blocks

    # Do long processing
    result = expensive_operation(data)

    # Reopen for save
    with psycopg2.connect(connect_timeout=5, **db) as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE data SET result = %s WHERE id = %s", (result, 123))
            conn.commit()
```

**Why this is best:** No manual cleanup needed, guaranteed to close even if exceptions occur.

---

### Pattern 2: Batch Processing

```python
import wmill
import psycopg2

def main(batch_size: int = 100):
    db = wmill.get_resource("f/resources/run8n_db")

    # Get batch of work
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(connect_timeout=5, **db)
        cur = conn.cursor()
        cur.execute("SELECT id, data FROM queue LIMIT %s", (batch_size,))
        items = cur.fetchall()
    finally:
        if cur: cur.close()
        if conn: conn.close()  # ✅ Close immediately

    # Process each item (long operation)
    results = []
    for item_id, data in items:
        result = expensive_operation(data)  # Can take minutes
        results.append((item_id, result))

    # Save results in bulk
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(connect_timeout=5, **db)
        cur = conn.cursor()
        for item_id, result in results:
            cur.execute("UPDATE queue SET result = %s WHERE id = %s", (result, item_id))
        conn.commit()
    finally:
        if cur: cur.close()
        if conn: conn.close()  # ✅ Close when done
```

### Pattern 3: Polling/Waiting

```python
import wmill
import psycopg2
import requests
import time

def main(job_id: str):
    db = wmill.get_resource("f/resources/run8n_db")

    # Check job status
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(connect_timeout=5, **db)
        cur = conn.cursor()
        cur.execute("SELECT status FROM jobs WHERE id = %s", (job_id,))
        status = cur.fetchone()[0]
    finally:
        if cur: cur.close()
        if conn: conn.close()  # ✅ Close before polling

    # Poll external API (no DB connection needed)
    while status != "complete":
        time.sleep(30)
        response = requests.get(f"https://api.example.com/jobs/{job_id}")
        status = response.json()["status"]

    # Update final status
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(connect_timeout=5, **db)
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET status = %s WHERE id = %s", (status, job_id))
        conn.commit()
    finally:
        if cur: cur.close()
        if conn: conn.close()  # ✅ Close when done
```

### Pattern 4: Multi-Step Workflows (Windmill Flows)

Instead of one monolithic script holding connections, decompose into flow steps:

```yaml
# f/flows/etl_pipeline.flow/flow.yaml
steps:
  - id: extract
    path: f/db/extract_data
  - id: transform
    path: f/processing/transform
    args:
      data: $res1  # From step 1
  - id: enrich
    path: f/api/enrich
    args:
      data: $res2  # From step 2
  - id: load
    path: f/db/save_results
    args:
      data: $res3  # From step 3
```

**Each script is focused:**
```python
# f/db/extract_data.py
def main(record_id: int):
    db = wmill.get_resource("f/resources/run8n_db")
    with psycopg2.connect(connect_timeout=5, **db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM data WHERE id = %s", (record_id,))
            return cur.fetchone()

# f/db/save_results.py
def main(data: dict):
    db = wmill.get_resource("f/resources/run8n_db")
    with psycopg2.connect(connect_timeout=5, **db) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO results VALUES (%s, %s)", (data['id'], data['result']))
            conn.commit()
```

**Benefits:** Connection only held for seconds per step, not for entire workflow.

---

## Connection Pooling - Why It Doesn't Work in Windmill

⚠️ **WARNING: In-script connection pooling does NOT work in Windmill!**

**Why?** Windmill does not persist module state between script runs. Each execution is a fresh Python process.

```python
# ❌ THIS DOESN'T WORK
from psycopg2 import pool

connection_pool = None  # Will be None EVERY run

def main():
    global connection_pool
    if connection_pool is None:
        connection_pool = pool.SimpleConnectionPool(...)
    # Pool is recreated on EVERY execution - no benefit!
```

### Alternative: Use PgBouncer (External Pooling)

For high-frequency scripts, use an **external connection pooler** like PgBouncer:

```bash
# PgBouncer sits between your scripts and PostgreSQL
Scripts → PgBouncer (pooled) → PostgreSQL
```

**run8n stack setup:**
```python
# Point scripts at PgBouncer instead of PostgreSQL directly
db = {
    "host": "pgbouncer.run8n.xyz",  # Not postgres.run8n.xyz
    "port": 6432,                    # Not 5432
    "user": "...",
    "password": "...",
    "dbname": "..."
}
```

**Benefits:**
- Connection reuse across ALL scripts and services (n8n, NocoDB, Windmill)
- Reduced connection overhead
- Configurable pool sizes per database

**For most scripts:** Just open/close connections properly. PgBouncer handles pooling.

---

## Advanced Tips

### Read-Only Queries: Use Autocommit

For read-only operations, skip transaction overhead:

```python
conn = psycopg2.connect(connect_timeout=5, autocommit=True, **db)
with conn.cursor() as cur:
    cur.execute("SELECT * FROM data")  # No commit needed
```

### Error Recovery: Rollback

If a query fails mid-transaction, rollback before reusing connection:

```python
conn = None
cur = None
try:
    conn = psycopg2.connect(connect_timeout=5, **db)
    cur = conn.cursor()
    cur.execute("UPDATE data SET status = 'processing' WHERE id = %s", (123,))
    result = risky_operation()  # Might fail
    cur.execute("UPDATE data SET result = %s WHERE id = %s", (result, 123))
    conn.commit()
except Exception as e:
    if conn:
        conn.rollback()  # ✅ Rollback failed transaction
    raise
finally:
    if cur: cur.close()
    if conn: conn.close()
```

### PostgreSQL Timeouts: Statement Timeout

For safety, set a max query execution time (prevents runaway queries):

```python
conn = psycopg2.connect(connect_timeout=5, **db)
with conn.cursor() as cur:
    cur.execute("SET statement_timeout = '30s'")  # Max 30 seconds per query
    cur.execute("SELECT * FROM huge_table")  # Will abort if > 30s
```

### Dangerous State: "idle in transaction"

**Worse than "idle"** - holds locks and blocks other queries:

```sql
-- Check for idle in transaction connections
SELECT pid, usename, state, state_change, query
FROM pg_stat_activity
WHERE state = 'idle in transaction'
AND state_change < NOW() - INTERVAL '5 minutes';
```

**Cause:** Opening a transaction but not committing/rolling back.

**Fix:** Always commit or rollback:
```python
try:
    conn = psycopg2.connect(**db)
    cur = conn.cursor()
    cur.execute("UPDATE data SET status = 'done'")
    conn.commit()  # ✅ Don't forget this!
finally:
    conn.close()
```

### run8n Stack Context: Shared Pool

**Important:** Multiple services share the same PostgreSQL connection pool:
- Windmill scripts
- n8n workflows
- NocoDB API
- GoTrue auth

**Why it matters:** A single misbehaving Windmill script can starve connections for all services.

**Best practice:** Keep connections under 5 seconds in Windmill scripts.

---

## Monitoring Connection Usage

Use these scripts to monitor connections:

1. **`f/db/audit_connections.py`** - See all active connections and idle time
2. **`f/db/check_connection_limits.py`** - Check if approaching limits
3. **`f/db/cleanup_idle_connections.py`** - Auto-kill stuck connections (scheduled)

---

## Summary

✅ **DO:**
- Use context managers (`with` statement) for automatic cleanup
- Initialize variables before try blocks (`conn = None`, `cur = None`)
- Add connection timeouts (`connect_timeout=5`)
- Close connections immediately after querying
- Reopen connections when needed
- Use Windmill flows for multi-step operations
- Keep connection lifetime < 5 seconds when possible

❌ **DON'T:**
- Hold connections during API calls
- Hold connections during sleep/wait
- Hold connections during data processing
- Hold connections during file I/O
- Forget to commit or rollback transactions
- Use in-script connection pooling (doesn't persist in Windmill)

### Resource Names in run8n Stack

Standard database resource path: `f/resources/run8n_db`

```python
import wmill
db = wmill.get_resource("f/resources/run8n_db")
# Returns: {"host": "...", "port": 5432, "user": "...", "password": "...", "dbname": "..."}
```

**Rule of thumb:** If your script runs longer than 10 seconds, you're probably holding a connection too long.
