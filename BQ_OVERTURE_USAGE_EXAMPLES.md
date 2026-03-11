# BigQuery Overture Fetcher - Usage Examples

## All 15 Supported Types by Theme

### Base Theme (6 types)

**Land** - Physical land surfaces
```python
result = main(
    west=7.588, south=47.557, east=7.590, north=47.559,
    types=["land"],
    gcp=gcp_creds, db=db_config
)
# Returns: id, names, sources, subtype
```

**Water** - Water bodies (rivers, lakes, oceans)
```python
types=["water"]
# Returns: id, names, sources, subtype
```

**Land Use** - Human land use classifications
```python
types=["land_use"]
# Returns: id, names, sources, subtype
```

**Land Cover** - ESA WorldCover derived data
```python
types=["land_cover"]
# Returns: id, sources, subtype (NO names column)
```

**Infrastructure** - Bridges, towers, piers, etc.
```python
types=["infrastructure"]
# Returns: id, names, sources, class, subtype
```

**Bathymetry** - Underwater topography
```python
types=["bathymetry"]
# Returns: id, sources, depth (NO names column)
```

### Buildings Theme (2 types)

**Building** - Building footprints
```python
types=["building"]
# Returns: id, names, sources, class, subtype, height, num_floors, has_parts
```

**Building Part** - Individual building components
```python
types=["building_part"]
# Returns: id, names, sources, class, subtype, height, num_floors, building_id
```

### Places Theme (1 type)

**Place** - Points of interest (restaurants, shops, etc.)
```python
types=["place"]
# Returns: id, names, sources, categories, confidence (NO class column)
```

### Transportation Theme (2 types)

**Segment** - Road/rail/water path centerlines
```python
types=["segment"]
# Returns: id, names, sources, class, subtype, connectors
```

**Connector** - Network junction points
```python
types=["connector"]
# Returns: id, sources (minimal schema)
```

### Addresses Theme (1 type)

**Address** - Global address points (ALPHA)
```python
types=["address"]
# Returns: id, sources, country, postcode
```

### Divisions Theme (3 types)

**Division** - Administrative boundary metadata
```python
types=["division"]
# Returns: id, names, sources, subtype, country, admin_level
```

**Division Area** - Polygon areas for divisions
```python
types=["division_area"]
# Returns: id, names, sources, subtype, division_id, country, admin_level
```

**Division Boundary** - Border lines
```python
types=["division_boundary"]
# Returns: id, sources (minimal schema)
```

## Multi-Type Queries

### Urban Analysis Package
```python
types=["building", "place", "segment", "connector"]
# Buildings + POIs + road network
```

### Environmental Analysis
```python
types=["land", "water", "land_cover", "land_use"]
# Complete land classification
```

### Infrastructure Study
```python
types=["infrastructure", "building", "segment"]
# Built environment overview
```

### Administrative Context
```python
types=["division", "division_area", "division_boundary"]
# Complete administrative boundaries
```

## Complete Example

```python
from f.geo.fetchers import bq_overture

# Credentials
gcp_creds = {
    "project_id": "your-project",
    "credentials_json": {...}
}

db_config = {
    "host": "postgres.run8n.xyz",
    "database": "geodata",
    "user": "...",
    "password": "..."
}

# Fetch all urban features for Basel city center
result = bq_overture.main(
    west=7.588,
    south=47.557,
    east=7.590,
    north=47.559,
    types=[
        "building",
        "building_part",
        "place",
        "segment",
        "connector",
        "land_use"
    ],
    use_cache=True,
    gcp=gcp_creds,
    db=db_config,
    store_s3=True  # Large results → S3
)

# Result structure:
if "s3" in result:
    print(f"Stored at: {result['s3']}")
    print(f"Size: {result['size_mb']} MB")
    print(f"Features: {result['metadata']['features_returned']}")
    print(f"Cost: ${result['metadata']['cost_usd']:.6f}")
else:
    print(f"Features: {len(result['features'])}")
    print(f"Metadata: {result['metadata']}")
```

## Important Notes

### Column Differences
- **No 'class' column:** land, water, land_use, place
- **No 'names' column:** land_cover, bathymetry, connector, division_boundary, address
- **Has 'connectors' array:** segment (references connector IDs)
- **Has 'depth' field:** bathymetry (required)

### Minimal Schemas
These types return only id + sources + geometry:
- connector
- division_boundary

### BigQuery vs S3 Differences
- BigQuery schema is simplified
- Some columns in PyArrow/S3 schema may not exist in BigQuery
- Always test queries before production use
- Conservative column lists prevent errors

### Cost Management
- Max bbox: 10 km² (0.1° × 0.1°)
- Use caching to avoid duplicate queries
- Cost: ~$0.00625 per TB scanned
- Segment/connector can be expensive (large datasets)

### Cache Version
- Current: v2 (15-type support)
- v1 caches are invalid (5-type only)
- Cache key includes types requested
