#!/usr/bin/env python3
"""
Test SQL generation for all 15 Overture types (no dependencies).
"""

VALID_TYPES = {
    # Base theme (land, water, land_use already supported)
    "land", "water", "land_use", "land_cover", "infrastructure", "bathymetry",
    # Buildings theme
    "building", "building_part",
    # Places theme
    "place",
    # Transportation theme
    "segment", "connector",
    # Addresses theme
    "address",
    # Divisions theme
    "division", "division_area", "division_boundary",
}

COLUMNS_BY_TYPE = {
    # Base theme
    "land": [
        "id", "names", "sources", "subtype"
    ],
    "water": [
        "id", "names", "sources", "subtype"
    ],
    "land_use": [
        "id", "names", "sources", "subtype"
    ],
    "land_cover": [
        "id", "sources", "subtype"  # No names column per schema
    ],
    "infrastructure": [
        "id", "names", "sources", "class", "subtype"
    ],
    "bathymetry": [
        "id", "sources", "depth"  # depth is required property
    ],
    # Buildings theme
    "building": [
        "id", "names", "sources", "class", "subtype",
        "height", "num_floors", "has_parts"
    ],
    "building_part": [
        "id", "names", "sources", "class", "subtype",
        "height", "num_floors", "building_id"
    ],
    # Places theme
    "place": [
        "id", "names", "sources",
        "categories", "confidence"  # No class column in BigQuery
    ],
    # Transportation theme
    "segment": [
        "id", "names", "sources", "class", "subtype",
        "connectors"  # References to connector points
    ],
    "connector": [
        "id", "sources"  # Minimal schema per docs
    ],
    # Addresses theme
    "address": [
        "id", "sources",
        "country", "postcode"  # Core address fields
    ],
    # Divisions theme
    "division": [
        "id", "names", "sources", "subtype",
        "country", "admin_level"
    ],
    "division_area": [
        "id", "names", "sources", "subtype",
        "division_id", "country", "admin_level"
    ],
    "division_boundary": [
        "id", "sources"  # Minimal schema
    ],
}

def _build_query(west, south, east, north, overture_type: str) -> str:
    """Build BigQuery SQL for a single feature type."""
    polygon_wkt = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    cols = COLUMNS_BY_TYPE.get(overture_type, ["id", "names", "sources"])
    col_list = ", ".join(cols)

    query = f"""
    SELECT
        ST_AsText(geometry) AS geometry_wkt,
        {col_list}
    FROM `bigquery-public-data.overture_maps.{overture_type}`
    WHERE ST_Within(geometry, ST_GeogFromText('{polygon_wkt}'))
    LIMIT 50000
    """
    return query


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
            cols = COLUMNS_BY_TYPE.get(t, [])
            print(f"  {t:<20} -> {len(cols):>2} columns: {', '.join(cols)}")


if __name__ == "__main__":
    result = test_sql_generation()
    if result == 0:
        show_example_queries()
    exit(result)
