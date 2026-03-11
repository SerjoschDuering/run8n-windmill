# BigQuery Overture Maps Benchmark Results

**Date:** 2026-02-24
**Script:** `/Users/Joo/01_Projects/windmill-monorepo/f/geo/fetchers/bq_overture.py`
**GCP Project:** n8ntest-425022

## Test Configuration

- **Feature Types:** building, land, water, land_use, place (5 types)
- **Cache:** Disabled (pure BigQuery performance)
- **Optimizations Applied:**
  - Vectorized shapely geometry conversion
  - Column projection in SQL
  - Spatial clustering (ST_Within)

## Results

```
┌──────────────┬──────────┬────────┬──────────┬─────────┬─────────┐
│     Area     │   Time   │  Cost  │ Features │  Size   │  Types  │
├──────────────┼──────────┼────────┼──────────┼─────────┼─────────┤
│ Hamburg 5km² │   30.5s │ $0.0166 │   19,600 │  14.11M │    5    │
│ Berlin 10km² │   44.1s │ $0.0174 │   32,318 │  23.89M │    5    │
└──────────────┴──────────┴────────┴──────────┴─────────┴─────────┘
```

### Hamburg 5km² (9.985-10.018 lon, 53.540-53.560 lat)

- **Total Time:** 30.5 seconds
- **Total Cost:** $0.016559 USD
- **Total Features:** 19,600
- **GeoJSON Size:** 14.11 MB

**Per-Type Breakdown:**
- Building: 2,471 features ($0.009263)
- Land: 5,682 features ($0.002760)
- Water: 59 features ($0.002104)
- Land Use: 803 features ($0.002313)
- Place: 10,585 features ($0.000119)

### Berlin 10km² (13.377-13.423 lon, 52.506-52.534 lat)

- **Total Time:** 44.1 seconds
- **Total Cost:** $0.017380 USD
- **Total Features:** 32,318
- **GeoJSON Size:** 23.89 MB

**Per-Type Breakdown:**
- Building: 8,170 features ($0.009525)
- Land: 7,810 features ($0.003117)
- Water: 46 features ($0.002134)
- Land Use: 1,553 features ($0.002342)
- Place: 14,739 features ($0.000262)

## Summary Statistics

- **Total Time:** 74.6 seconds
- **Total Cost:** $0.033939 USD (3.4 cents)
- **Total Features:** 51,918
- **Total Data:** 37.00 MB GeoJSON

## Performance Observations

1. **Cost Efficiency:** Both queries cost ~$0.017 despite 2x area difference
   - Hamburg 5km²: $0.0166 (19,600 features)
   - Berlin 10km²: $0.0174 (32,318 features)
   - Cost is primarily driven by spatial index scanning, not result size

2. **Query Time:** Roughly scales with feature density
   - Hamburg: 30.5s for 19,600 features (642 features/sec)
   - Berlin: 44.1s for 32,318 features (733 features/sec)

3. **Feature Distribution:**
   - Places are most common (25,324 total, ~49%)
   - Buildings are second (10,641 total, ~20%)
   - Land features are third (13,492 total, ~26%)
   - Water and land_use are sparse

4. **Data Size:**
   - Average: 0.7 KB per feature
   - Total bandwidth: 37 MB for 51,918 features

## Cost Projections

Based on these results:

- **1 km² area:** ~$0.003 USD, ~4,000 features, ~6s
- **10 km² area:** ~$0.017 USD, ~30,000 features, ~40s
- **100 km² area:** ~$0.17 USD, ~300,000 features, ~7 minutes (estimated)

**Note:** Actual costs may vary based on feature density in the area.

## Test Environment

- **Python:** 3.9
- **BigQuery SDK:** google-cloud-bigquery==3.17.2
- **Shapely:** 2.0.2
- **Location:** Local machine (macOS)
- **Network:** Standard internet connection
