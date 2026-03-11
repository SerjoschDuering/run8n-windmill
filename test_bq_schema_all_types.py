#!/usr/bin/env python3
"""
Test SQL generation for all 15 Overture types in BigQuery fetcher.
Validates syntax without running queries (no cost).
"""

import sys
sys.path.insert(0, '/Users/Joo/01_Projects/windmill-monorepo/f/geo/fetchers')

from bq_overture import VALID_TYPES, COLUMNS_BY_TYPE, _build_query

# Test bbox (small area in Basel, Switzerland)
WEST, SOUTH, EAST, NORTH = 7.588, 47.557, 7.590, 47.559

def test_sql_generation():
    """Test SQL generation for all 15 types."""
    print("Testing SQL generation for all 15 Overture types")
    print("=" * 80)

    results = []
    for overture_type in sorted(VALID_TYPES):
        print(f"\n{overture_type}:")
        print("-" * 40)

        # Get columns
        cols = COLUMNS_BY_TYPE.get(overture_type, ["id", "names", "sources"])
        print(f"Columns ({len(cols)}): {', '.join(cols)}")

        # Generate SQL
        try:
            query = _build_query(WEST, SOUTH, EAST, NORTH, overture_type)

            # Validate SQL structure
            assert "SELECT" in query, "Missing SELECT"
            assert "FROM `bigquery-public-data.overture_maps." in query, "Missing FROM clause"
            assert f"overture_maps.{overture_type}`" in query, f"Wrong table name"
            assert "ST_Within" in query, "Missing spatial filter"
            assert "ST_AsText(geometry)" in query, "Missing geometry conversion"

            # Check all columns are in query
            for col in cols:
                assert col in query, f"Column '{col}' not in query"

            print(f"✓ SQL generation successful")
            print(f"  Table: bigquery-public-data.overture_maps.{overture_type}")

            results.append({
                "type": overture_type,
                "status": "✓",
                "columns": len(cols)
            })

        except Exception as e:
            print(f"✗ Error: {e}")
            results.append({
                "type": overture_type,
                "status": "✗",
                "error": str(e)
            })

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    success = [r for r in results if r["status"] == "✓"]
    failed = [r for r in results if r["status"] == "✗"]

    print(f"\nTotal types: {len(VALID_TYPES)}")
    print(f"Successful: {len(success)}")
    print(f"Failed: {len(failed)}")

    if failed:
        print("\nFailed types:")
        for r in failed:
            print(f"  - {r['type']}: {r.get('error', 'Unknown error')}")
        return 1

    print("\n✓ All 15 types validated successfully!")

    # Print column count summary
    print("\nColumn counts by type:")
    for r in sorted(results, key=lambda x: x["columns"], reverse=True):
        print(f"  {r['type']:<20} {r['columns']:>2} columns")

    return 0


def show_example_queries():
    """Show example queries for each theme."""
    print("\n" + "=" * 80)
    print("EXAMPLE QUERIES BY THEME")
    print("=" * 80)

    themes = {
        "Base": ["land", "water", "land_use", "land_cover", "infrastructure", "bathymetry"],
        "Buildings": ["building", "building_part"],
        "Places": ["place"],
        "Transportation": ["segment", "connector"],
        "Addresses": ["address"],
        "Divisions": ["division", "division_area", "division_boundary"],
    }

    for theme, types in themes.items():
        print(f"\n{theme} Theme:")
        for t in types:
            query = _build_query(WEST, SOUTH, EAST, NORTH, t)
            # Show just the column list
            cols = COLUMNS_BY_TYPE.get(t, [])
            print(f"  {t}: {', '.join(cols)}")


if __name__ == "__main__":
    result = test_sql_generation()
    if result == 0:
        show_example_queries()
    sys.exit(result)
