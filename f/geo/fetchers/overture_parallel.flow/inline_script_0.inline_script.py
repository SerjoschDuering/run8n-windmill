def main(
    results: list,
    bbox: dict,
    types: list,
):
    s3_refs = []
    inline_features = []
    type_breakdown = []
    errors = []
    total_features = 0

    for r in results:
        if not isinstance(r, dict):
            continue
        meta = r.get("metadata", {})
        # Propagate errors from failed sub-jobs
        if "error" in meta:
            errors.append(meta["error"])
        # S3 mode: each result is {"s3": "...", "metadata": {...}}
        if "s3" in r:
            s3_refs.append(r["s3"])
            total_features += meta.get("total_features", 0)
            for ts in meta.get("type_breakdown", []):
                type_breakdown.append(ts)
        # Inline mode: each result is a full FeatureCollection
        elif "features" in r:
            feats = r.get("features", [])
            total_features += len(feats)
            inline_features.extend(feats)
            for ts in meta.get("type_breakdown", []):
                type_breakdown.append(ts)

    result = {
        "metadata": {
            "source": "overture",
            "bbox": bbox,
            "total_features": total_features,
            "types_requested": types,
            "type_breakdown": type_breakdown,
            "parallel": True,
        },
    }
    if s3_refs:
        result["s3_files"] = s3_refs
    if inline_features:
        result["type"] = "FeatureCollection"
        result["features"] = inline_features
    if errors:
        result["metadata"]["errors"] = errors
    return result
