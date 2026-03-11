#!/usr/bin/env python3
"""
Benchmark BigQuery Overture fetcher for Hamburg and Berlin test areas.
"""

import json
import time
import sys
from pathlib import Path

# Mock wmill module since we're running outside Windmill
class MockWmill:
    @staticmethod
    def write_s3_file(s3_ref, data):
        # Just save to local file instead
        filename = Path(f"/tmp/{s3_ref['s3'].replace('/', '_')}")
        filename.write_bytes(data)
        print(f"  [Mock] Saved to {filename} instead of S3")

sys.modules['wmill'] = MockWmill()

# Add the monorepo to path so we can import the script
sys.path.insert(0, str(Path(__file__).parent))

from f.geo.fetchers.bq_overture import main as fetch_overture


# Test areas
AREAS = {
    "Hamburg 5km²": {
        "west": 9.985,
        "south": 53.540,
        "east": 10.018,
        "north": 53.560,
    },
    "Berlin 10km²": {
        "west": 13.377,
        "south": 52.506,
        "east": 13.423,
        "north": 52.534,
    },
}

# All feature types to test
ALL_TYPES = ["building", "land", "water", "land_use", "place"]


def load_gcp_credentials():
    """Load GCP service account credentials."""
    creds_path = Path.home() / "Downloads" / "n8ntest-425022-988a3c740b23.json"

    if not creds_path.exists():
        raise FileNotFoundError(f"GCP credentials not found at {creds_path}")

    with open(creds_path) as f:
        creds_json = json.load(f)

    return {
        "project_id": creds_json["project_id"],
        "credentials_json": creds_json
    }


def format_size(bytes_val):
    """Format bytes to MB."""
    return f"{bytes_val / 1_048_576:.2f} MB"


def run_benchmark(area_name, bbox, types, gcp_creds):
    """Run a single benchmark test."""
    print(f"\n{'='*70}")
    print(f"Testing: {area_name}")
    print(f"Bbox: {bbox}")
    print(f"Types: {types}")
    print(f"{'='*70}")

    start_time = time.time()

    result = fetch_overture(
        west=bbox["west"],
        south=bbox["south"],
        east=bbox["east"],
        north=bbox["north"],
        types=types,
        use_cache=False,  # Disable cache for pure BigQuery benchmark
        gcp=gcp_creds,
        db=None,
        store_s3=True
    )

    end_time = time.time()
    duration = end_time - start_time

    # Extract metrics
    metadata = result.get("metadata", {})
    features = metadata.get("features_returned", 0)
    cost = metadata.get("cost_usd", 0)
    s3_path = result.get("s3")
    size_mb = result.get("size_mb", 0)

    print(f"\n✓ Completed in {duration:.1f}s")
    print(f"  Features: {features:,}")
    print(f"  Cost: ${cost:.6f}")
    print(f"  S3 Size: {size_mb} MB")
    if s3_path:
        print(f"  S3 Path: {s3_path}")

    return {
        "area": area_name,
        "duration_s": round(duration, 1),
        "features": features,
        "cost_usd": cost,
        "size_mb": size_mb,
        "s3_path": s3_path,
        "types_count": len(types)
    }


def print_results_table(results):
    """Print formatted results table."""
    print("\n" + "="*80)
    print("BENCHMARK RESULTS")
    print("="*80)
    print()
    print("┌──────────────┬──────────┬────────┬──────────┬─────────┬─────────┐")
    print("│     Area     │   Time   │  Cost  │ Features │ S3 Size │  Types  │")
    print("├──────────────┼──────────┼────────┼──────────┼─────────┼─────────┤")

    for r in results:
        print(f"│ {r['area']:<12} │ {r['duration_s']:>6}s │ ${r['cost_usd']:>5.4f} │ {r['features']:>8,} │ {r['size_mb']:>6.2f}M │   {r['types_count']:>2}    │")

    print("└──────────────┴──────────┴────────┴──────────┴─────────┴─────────┘")
    print()

    # Summary stats
    total_cost = sum(r['cost_usd'] for r in results)
    total_features = sum(r['features'] for r in results)
    total_time = sum(r['duration_s'] for r in results)

    print(f"Total Time: {total_time:.1f}s")
    print(f"Total Cost: ${total_cost:.6f}")
    print(f"Total Features: {total_features:,}")
    print()

    # S3 paths
    print("S3 Paths:")
    for r in results:
        if r['s3_path']:
            print(f"  {r['area']}: {r['s3_path']}")
    print()


def main():
    """Run all benchmarks."""
    print("BigQuery Overture Maps Benchmark")
    print("=================================")

    # Load credentials
    print("\nLoading GCP credentials...")
    try:
        gcp_creds = load_gcp_credentials()
        print(f"✓ Loaded credentials for project: {gcp_creds['project_id']}")
    except Exception as e:
        print(f"✗ Failed to load credentials: {e}")
        return 1

    # Run benchmarks
    results = []

    for area_name, bbox in AREAS.items():
        try:
            result = run_benchmark(area_name, bbox, ALL_TYPES, gcp_creds)
            results.append(result)
        except Exception as e:
            print(f"\n✗ Benchmark failed for {area_name}: {e}")
            import traceback
            traceback.print_exc()

    # Print results table
    if results:
        print_results_table(results)

    return 0


if __name__ == "__main__":
    sys.exit(main())
