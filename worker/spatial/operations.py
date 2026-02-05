"""Spatial operations for assessments.

This module provides common spatial operations used across assessments:
- Clipping GeoDataFrames to extents
- Spatial joins with common predicates
- Geometry validation and repair
"""

import geopandas as gpd
from shapely.validation import make_valid

from worker.spatial.utils import apply_precision


def clip_gdf(
    gdf: gpd.GeoDataFrame,
    mask: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Clip a GeoDataFrame to the extent of a mask GeoDataFrame.

    Clips geometries to the boundary of the mask, keeping only portions
    that fall within the mask extent. Matches legacy/opensource_gcn.py behavior.

    Args:
        gdf: Input GeoDataFrame to clip
        mask: Mask GeoDataFrame defining clip extent

    Returns:
        Clipped GeoDataFrame (may have fewer rows than input)

    Example:
        # Clip risk zones to RLB extent
        risk_zones_clipped = clip_gdf(risk_zones, rlb_gdf)
    """
    if gdf.crs != mask.crs:
        mask = mask.to_crs(gdf.crs)

    # Use gpd.clip() to match baseline behavior
    # This is simpler than overlay and produces consistent results
    clipped = gpd.clip(gdf, mask)

    return clipped


def spatial_join_intersect(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Spatial intersection (overlay) operation with precision control.

    Performs a geometric intersection, creating new geometries from the overlapping areas.
    This replaces ArcPy PairwiseIntersect and matches the behavior of legacy/opensource_gcn.py.

    Applies precision model before and after overlay to match ArcGIS XY tolerance behavior.
    This reduces floating-point accumulation errors and ensures consistent geometry operations.

    Args:
        left: Left GeoDataFrame
        right: Right GeoDataFrame
        grid_size: Grid size for precision model in meters (default: 0.0001m = 0.1mm,
                  matching ArcGIS XY Resolution for British National Grid)

    Returns:
        GeoDataFrame with intersected geometries and attributes from both inputs

    Example:
        # Intersect RLB with risk zones to get habitat impact areas
        habitat_impact = spatial_join_intersect(rlb, risk_zones)
    """
    if left.crs != right.crs:
        right = right.to_crs(left.crs)

    # Apply precision to input geometries before overlay
    # This snaps coordinates to a grid, matching ArcGIS XY tolerance behavior
    left_precise = apply_precision(left, grid_size=grid_size)
    right_precise = apply_precision(right, grid_size=grid_size)

    # Perform geometric intersection (not just attribute join)
    # This creates new geometries from the overlapping areas
    result = gpd.overlay(left_precise, right_precise, how="intersection", keep_geom_type=False)

    # Apply precision to result - overlay operations can introduce new vertices
    # and numerical imprecision, so we snap the output back to the grid
    result = apply_precision(result, grid_size=grid_size)

    return result


def make_valid_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Repair invalid geometries using Shapely's make_valid.

    Fixes common geometry issues like self-intersections, unclosed rings,
    etc. This is important for ensuring spatial operations succeed.

    Args:
        gdf: Input GeoDataFrame (may contain invalid geometries)

    Returns:
        GeoDataFrame with repaired geometries

    Example:
        # Repair RLB geometries before processing
        rlb = make_valid_geometries(rlb_gdf)
    """
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.apply(lambda geom: make_valid(geom) if geom else geom)

    return gdf
