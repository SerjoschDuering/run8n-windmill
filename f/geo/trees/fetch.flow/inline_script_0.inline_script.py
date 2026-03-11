import math

def main(west: float, south: float, east: float, north: float, allow_large_area: bool = False):
    # Calculate area in km²
    center_lat = (north + south) / 2
    km_per_deg_lat = 111.32
    km_per_deg_lon = 111.32 * math.cos(math.radians(center_lat))

    width_km = (east - west) * km_per_deg_lon
    height_km = (north - south) * km_per_deg_lat
    area_km2 = width_km * height_km

    MAX_AREA_KM2 = 5.0

    if area_km2 > MAX_AREA_KM2 and not allow_large_area:
        raise Exception(f"Area {area_km2:.2f} km² exceeds limit of {MAX_AREA_KM2} km². Set allow_large_area=true to bypass.")

    return {
        "area_km2": round(area_km2, 3),
        "bbox": {"west": west, "south": south, "east": east, "north": north}
    }
