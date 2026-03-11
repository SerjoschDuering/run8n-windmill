"""
Windmill Script: Render 2.5D Map Image
Path: f/geo/render_map_image

Renders a static 2.5D map image from simulation grid data using matplotlib.
Used by the MCP server to generate map visualizations without WebGL.

Input: bbox, grid, analysis_type, buildings, trees, pins, image dimensions
Output: base64-encoded PNG image
"""

import base64
import io
import math
from typing import TypedDict, Optional, Literal

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
from matplotlib.collections import PatchCollection, PolyCollection
import numpy as np


# UTCI color scale (thermal stress categories)
UTCI_CMAP_STOPS = [
    (-50, "#1e3a8a"),
    (-40, "#3b82f6"),
    (-27, "#22d3ee"),
    (-13, "#67e8f9"),
    (0, "#a5f3fc"),
    (9, "#22c55e"),
    (26, "#eab308"),
    (32, "#f97316"),
    (38, "#ef4444"),
    (46, "#991b1b"),
]

# Wind color scale (Lawson criteria)
WIND_CMAP_STOPS = [
    (0, "#22c55e"),
    (2, "#84cc16"),
    (4, "#eab308"),
    (6, "#f97316"),
    (8, "#ef4444"),
    (10, "#991b1b"),
]

# UTCI category labels for legend
UTCI_LABELS = [
    (9, "Cold"), (18, "Comfort"), (26, "Moderate"),
    (32, "Strong"), (38, "Very Strong"), (46, "Extreme"),
]

WIND_LABELS = [
    (2, "Sitting"), (4, "Standing"), (6, "Strolling"),
    (8, "Business"), (10, "Uncomfortable"),
]

# Pin colors
PIN_COLORS = {"A": "#ef4444", "B": "#3b82f6", "C": "#22c55e"}


class BBox(TypedDict):
    west: float
    south: float
    east: float
    north: float


class PinInput(TypedDict):
    lat: float
    lon: float
    label: str
    color: str


def build_colormap(stops: list, vmin: float, vmax: float):
    """Build matplotlib colormap from value-color stops."""
    colors_list = []
    positions = []
    for val, color in stops:
        pos = (val - vmin) / (vmax - vmin)
        pos = max(0.0, min(1.0, pos))
        positions.append(pos)
        colors_list.append(color)
    # Ensure positions are monotonically increasing
    cleaned = [(positions[0], colors_list[0])]
    for i in range(1, len(positions)):
        if positions[i] > cleaned[-1][0]:
            cleaned.append((positions[i], colors_list[i]))
    positions = [c[0] for c in cleaned]
    colors_list = [c[1] for c in cleaned]
    return mcolors.LinearSegmentedColormap.from_list(
        "custom", list(zip(positions, colors_list)), N=256
    )


def geo_to_pixel(lon: float, lat: float, bbox: BBox, w: int, h: int):
    """Convert geographic coordinates to pixel coordinates."""
    x = (lon - bbox["west"]) / (bbox["east"] - bbox["west"]) * w
    y = (bbox["north"] - lat) / (bbox["north"] - bbox["south"]) * h
    return x, y


def render_buildings_2_5d(ax, buildings: dict, bbox: BBox, w: int, h: int):
    """Render building footprints with 2.5D shadow effect."""
    if not buildings or "features" not in buildings:
        return
    features = buildings["features"]
    if not features:
        return

    shadow_polys = []
    building_polys = []
    shadow_offset_x = 3
    shadow_offset_y = -3

    for f in features:
        geom = f.get("geometry", {})
        if geom.get("type") not in ("Polygon", "MultiPolygon"):
            continue
        props = f.get("properties", {})
        height = props.get("height", 10)
        # Scale shadow offset by height
        sx = shadow_offset_x * min(height / 10, 3)
        sy = shadow_offset_y * min(height / 10, 3)

        coords_list = geom.get("coordinates", [])
        if geom["type"] == "Polygon":
            coords_list = [coords_list]

        for polygon_coords in coords_list:
            if not polygon_coords:
                continue
            ring = polygon_coords[0]
            pixels = []
            shadow_pixels = []
            for coord in ring:
                lon, lat = coord[0], coord[1]
                px, py = geo_to_pixel(lon, lat, bbox, w, h)
                pixels.append((px, py))
                shadow_pixels.append((px + sx, py + sy))

            if len(pixels) >= 3:
                shadow_polys.append(shadow_pixels)
                building_polys.append(pixels)

    if shadow_polys:
        shadow_col = PolyCollection(
            shadow_polys, facecolors="#00000040",
            edgecolors="none", zorder=2
        )
        ax.add_collection(shadow_col)

    if building_polys:
        building_col = PolyCollection(
            building_polys, facecolors="#334155",
            edgecolors="#475569", linewidths=0.5, zorder=3
        )
        ax.add_collection(building_col)


def render_trees(ax, trees: dict, bbox: BBox, w: int, h: int):
    """Render trees as green circles."""
    if not trees or "features" not in trees:
        return
    features = trees["features"]
    if not features:
        return

    circles = []
    for f in features:
        geom = f.get("geometry", {})
        if geom.get("type") != "Point":
            continue
        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue
        props = f.get("properties", {})
        diameter = props.get("Diameter", props.get("diameter", 5))
        radius_m = diameter / 2

        lon, lat = coords[0], coords[1]
        px, py = geo_to_pixel(lon, lat, bbox, w, h)

        # Convert radius meters to pixels (approximate)
        m_per_px_x = (
            (bbox["east"] - bbox["west"])
            * 111320
            * math.cos(math.radians((bbox["north"] + bbox["south"]) / 2))
            / w
        )
        r_px = max(2, radius_m / m_per_px_x)
        circles.append(mpatches.Circle((px, py), r_px))

    if circles:
        col = PatchCollection(
            circles, facecolors="#22c55e80",
            edgecolors="#16a34a", linewidths=0.5, zorder=4
        )
        ax.add_collection(col)


def render_pins(ax, pins: list, bbox: BBox, w: int, h: int):
    """Render pin markers on the map."""
    if not pins:
        return
    for pin in pins:
        lon = pin.get("lon", 0)
        lat = pin.get("lat", 0)
        label = pin.get("label", "?")
        color = pin.get("color", PIN_COLORS.get(label, "#999"))
        px, py = geo_to_pixel(lon, lat, bbox, w, h)
        ax.plot(px, py, "o", color=color, markersize=12,
                markeredgecolor="white", markeredgewidth=2, zorder=6)
        ax.text(px, py, label, ha="center", va="center",
                fontsize=8, fontweight="bold", color="white", zorder=7)


def render_legend(ax, analysis_type: str, vmin: float, vmax: float,
                  cmap, w: int, h: int):
    """Render horizontal legend bar at bottom."""
    legend_h = 24
    legend_y = h - 10
    legend_x = w * 0.15
    legend_w = w * 0.7

    gradient = np.linspace(0, 1, 256).reshape(1, -1)
    ax.imshow(
        gradient, aspect="auto", cmap=cmap,
        extent=[legend_x, legend_x + legend_w, legend_y, legend_y + legend_h],
        zorder=8
    )
    ax.add_patch(mpatches.FancyBboxPatch(
        (legend_x - 2, legend_y - 2), legend_w + 4, legend_h + 4,
        boxstyle="round,pad=2", facecolor="none",
        edgecolor="#ffffff40", linewidth=1, zorder=8
    ))

    labels = UTCI_LABELS if analysis_type == "utci" else WIND_LABELS
    unit = "C" if analysis_type == "utci" else "m/s"

    ax.text(legend_x - 6, legend_y + legend_h / 2,
            f"{vmin:.0f}{unit}", ha="right", va="center",
            fontsize=7, color="#ccc", zorder=9)
    ax.text(legend_x + legend_w + 6, legend_y + legend_h / 2,
            f"{vmax:.0f}{unit}", ha="left", va="center",
            fontsize=7, color="#ccc", zorder=9)

    for val, lbl in labels:
        frac = (val - vmin) / (vmax - vmin)
        if 0 <= frac <= 1:
            lx = legend_x + frac * legend_w
            ax.text(lx, legend_y - 6, lbl, ha="center", va="top",
                    fontsize=6, color="#aaa", zorder=9)


def main(
    bbox: BBox,
    grid: list,
    analysis_type: Literal["utci", "wind"],
    buildings: Optional[dict] = None,
    trees: Optional[dict] = None,
    pins: Optional[list] = None,
    image_width: int = 1024,
    image_height: int = 1024,
    show_legend: bool = True,
    opacity: float = 0.7,
    output_format: Literal["png", "jpeg"] = "png",
    jpeg_quality: int = 75,
) -> dict:
    """
    Render a 2.5D map image from simulation data.

    Returns: { image_base64: str, width: int, height: int }
    """
    if not grid or not grid[0]:
        return {"error": "Empty grid", "image_base64": "", "width": 0, "height": 0}

    rows = len(grid)
    cols = len(grid[0])
    arr = np.array(grid, dtype=float)

    # Replace NaN sentinel values
    arr[np.isnan(arr)] = np.nan

    # Determine value range
    valid = arr[~np.isnan(arr)]
    if len(valid) == 0:
        return {"error": "All grid values are null", "image_base64": "",
                "width": 0, "height": 0}

    if analysis_type == "utci":
        vmin, vmax = -50, 46
        stops = UTCI_CMAP_STOPS
    else:
        vmin, vmax = 0, 10
        stops = WIND_CMAP_STOPS

    cmap = build_colormap(stops, vmin, vmax)

    # Create figure
    dpi = 100
    fig_w = image_width / dpi
    fig_h = image_height / dpi
    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h), dpi=dpi)
    fig.patch.set_facecolor("#1a1a2e")
    ax.set_facecolor("#1a1a2e")

    # Remove axes
    ax.set_xlim(0, image_width)
    ax.set_ylim(image_height, 0)
    ax.axis("off")
    ax.set_position([0, 0, 1, 1])

    # 1. Render heatmap grid
    ax.imshow(
        arr, cmap=cmap, vmin=vmin, vmax=vmax,
        alpha=opacity, interpolation="bilinear",
        extent=[0, image_width, image_height, 0],
        zorder=1, aspect="auto"
    )

    # 2. Render buildings (2.5D)
    render_buildings_2_5d(ax, buildings, bbox, image_width, image_height)

    # 3. Render trees
    render_trees(ax, trees, bbox, image_width, image_height)

    # 4. Render pins
    render_pins(ax, pins or [], bbox, image_width, image_height)

    # 5. Legend
    if show_legend:
        render_legend(ax, analysis_type, vmin, vmax, cmap,
                      image_width, image_height)

    # Export to buffer
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi,
                pad_inches=0, facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)

    mime = "image/png"
    if output_format == "jpeg":
        # Convert PNG buffer to compressed JPEG via Pillow
        from PIL import Image
        img = Image.open(buf)
        img = img.convert("RGB")  # JPEG doesn't support alpha
        jpeg_buf = io.BytesIO()
        img.save(jpeg_buf, format="JPEG", quality=jpeg_quality, optimize=True)
        jpeg_buf.seek(0)
        buf = jpeg_buf
        mime = "image/jpeg"

    image_base64 = base64.b64encode(buf.read()).decode("utf-8")

    return {
        "image_base64": image_base64,
        "width": image_width,
        "height": image_height,
        "mime_type": mime,
    }
