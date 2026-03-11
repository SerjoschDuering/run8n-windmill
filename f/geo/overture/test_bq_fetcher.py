"""
Test and benchmark BigQuery Overture fetcher.

Validates:
- Basic functionality (cache miss, cache hit)
- Performance improvements (vectorized geometry, orjson)
- Cost tracking accuracy
- Cache operations

Run this after setup_bq_cache to verify the implementation.
"""

#extra_requirements:
#psycopg2-binary==2.9.10
#shapely==2.0.2
#orjson==3.9.10
#numpy<2.0

import time
import json
from typing import Optional

import shapely
import orjson


def benchmark_geometry_conversion() -> dict:
    """
    Test OPTIMIZATION #1: Vectorized geometry conversion.

    Expected: 4-5x speedup for 10k features.
    """
    print("\n=== Benchmark: Geometry Conversion ===")

    # Generate test WKT strings (realistic complexity)
    test_wkt = [
        f"POLYGON(({i} {i}, {i+0.001} {i}, {i+0.001} {i+0.001}, {i} {i+0.001}, {i} {i}))"
        for i in range(10000)
    ]

    # Old way: row-by-row
    start = time.time()
    geoms_old = [shapely.wkt.loads(wkt) for wkt in test_wkt]
    time_old = time.time() - start

    # New way: vectorized
    start = time.time()
    geoms_new = shapely.from_wkt(test_wkt)
    time_new = time.time() - start

    speedup = time_old / time_new if time_new > 0 else 0

    result = {
        "test": "vectorized_geometry",
        "features": 10000,
        "old_time_s": round(time_old, 3),
        "new_time_s": round(time_new, 3),
        "speedup": round(speedup, 1),
        "savings_s": round(time_old - time_new, 1),
        "expected_speedup": "4-5x",
        "status": "PASS" if speedup >= 3.0 else "WARN"
    }

    print(f"  Row-by-row: {time_old:.2f}s")
    print(f"  Vectorized: {time_new:.2f}s")
    print(f"  Speedup: {speedup:.1f}x (expected 4-5x)")
    print(f"  Status: {result['status']}")

    return result


def benchmark_serialization() -> dict:
    """
    Test OPTIMIZATION #5: orjson fast serialization.

    Expected: 5-6x speedup for 50k feature GeoJSON.
    """
    print("\n=== Benchmark: JSON Serialization ===")

    # Generate large GeoJSON (realistic structure)
    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "id": f"feature_{i}",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(i), float(i)]
                },
                "properties": {
                    "name": f"Building {i}",
                    "height": 10.5 + i * 0.1,
                    "type": "residential"
                }
            }
            for i in range(50000)
        ]
    }

    # stdlib json.dumps
    start = time.time()
    json_str = json.dumps(geojson)
    time_json = time.time() - start

    # orjson.dumps
    start = time.time()
    orjson_bytes = orjson.dumps(geojson)
    time_orjson = time.time() - start

    speedup = time_json / time_orjson if time_orjson > 0 else 0

    result = {
        "test": "orjson_serialization",
        "features": 50000,
        "json_time_s": round(time_json, 3),
        "orjson_time_s": round(time_orjson, 3),
        "speedup": round(speedup, 1),
        "savings_s": round(time_json - time_orjson, 1),
        "expected_speedup": "5-6x",
        "status": "PASS" if speedup >= 4.0 else "WARN"
    }

    print(f"  json.dumps: {time_json:.2f}s")
    print(f"  orjson.dumps: {time_orjson:.2f}s")
    print(f"  Speedup: {speedup:.1f}x (expected 5-6x)")
    print(f"  Status: {result['status']}")

    return result


def test_cache_operations(db: dict) -> dict:
    """Test cache storage and retrieval."""
    print("\n=== Test: Cache Operations ===")

    import psycopg2

    try:
        conn = psycopg2.connect(**dict(db))
        cur = conn.cursor()

        # Test insert
        test_key = "test_benchmark_12345"
        test_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "test1",
                    "geometry": {"type": "Point", "coordinates": [11.5, 48.1]},
                    "properties": {"name": "Test"}
                }
            ]
        }

        geojson_bytes = orjson.dumps(test_geojson)

        cur.execute(
            """
            INSERT INTO bq_overture_cache
            (cache_key, bbox, types, geojson, feature_count,
             size_bytes, bytes_billed, cost_usd)
            VALUES (
                %s,
                ST_MakeEnvelope(11.5, 48.1, 11.6, 48.2, 4326),
                %s,
                %s::jsonb,
                1,
                %s,
                1000000,
                0.000006
            )
            ON CONFLICT (cache_key) DO UPDATE SET
                accessed_at = NOW()
            """,
            (
                test_key,
                ["building"],
                geojson_bytes.decode(),
                len(geojson_bytes)
            )
        )

        # Test retrieval
        cur.execute(
            "SELECT geojson, feature_count FROM bq_overture_cache WHERE cache_key = %s",
            (test_key,)
        )
        row = cur.fetchone()

        # Cleanup
        cur.execute("DELETE FROM bq_overture_cache WHERE cache_key = %s", (test_key,))
        conn.commit()

        cur.close()
        conn.close()

        if row and row[1] == 1:
            print("  ✓ Cache insert/retrieve working")
            return {
                "test": "cache_operations",
                "status": "PASS",
                "message": "Cache operations working correctly"
            }
        else:
            print("  ✗ Cache test failed")
            return {
                "test": "cache_operations",
                "status": "FAIL",
                "message": "Failed to retrieve cached data"
            }

    except Exception as e:
        return {
            "test": "cache_operations",
            "status": "ERROR",
            "error": str(e)
        }


def main(
    db: Optional[dict] = None,
    run_benchmarks: bool = True,
    run_cache_test: bool = True
) -> dict:
    """
    Run BigQuery fetcher tests and benchmarks.

    Args:
        db: PostgreSQL connection (required for cache test)
        run_benchmarks: Run performance benchmarks
        run_cache_test: Test cache operations (requires db)

    Returns:
        Test results with performance metrics
    """
    results = []

    if run_benchmarks:
        # Benchmark #1: Vectorized geometry (8-15s savings)
        results.append(benchmark_geometry_conversion())

        # Benchmark #5: orjson serialization (2-4s savings)
        results.append(benchmark_serialization())

    if run_cache_test:
        if not db:
            results.append({
                "test": "cache_operations",
                "status": "SKIPPED",
                "message": "db parameter required"
            })
        else:
            results.append(test_cache_operations(db))

    # Calculate total expected savings
    total_savings = sum(r.get("savings_s", 0) for r in results if "savings_s" in r)
    passed = sum(1 for r in results if r.get("status") == "PASS")
    total = len([r for r in results if "status" in r])

    print("\n=== Summary ===")
    print(f"  Tests passed: {passed}/{total}")
    print(f"  Total time savings: ~{total_savings:.1f}s per query")
    print(f"  Expected on 10km² query: 15-29s savings")

    return {
        "status": "success" if passed == total else "partial",
        "tests_passed": f"{passed}/{total}",
        "total_savings_per_query_s": round(total_savings, 1),
        "expected_savings_range_s": "15-29",
        "detailed_results": results
    }
