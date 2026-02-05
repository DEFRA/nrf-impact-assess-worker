"""Spatial overlay operations for assessments.

This module provides spatial overlay operations commonly used in assessments,
particularly for GCN (Great Crested Newt) assessments that require:
- Buffer operations with dissolve
- Difference (erase) with precision control

These operations replace ArcPy equivalents with GeoPandas/Shapely implementations.

Note: Spatial intersection with precision is provided by spatial_join_intersect()
in worker.spatial.operations.
"""

import geopandas as gpd
from shapely.ops import unary_union

from worker.spatial.utils import apply_precision


def buffer_with_dissolve(
    gdf: gpd.GeoDataFrame,
    distance_m: float,
    dissolve: bool = True,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Buffer geometries with optional dissolve to single geometry.

    Replaces ArcPy PairwiseBuffer operation from legacy GCN script.

    Args:
        gdf: Input GeoDataFrame
        distance_m: Buffer distance in metres
        dissolve: If True, dissolve all buffered geometries into single geometry
                 (default: True)
        grid_size: Grid size for precision model (default: 0.0001m)

    Returns:
        GeoDataFrame with buffered geometries. If dissolve=True, returns single-row
        GeoDataFrame with dissolved geometry. Precision snapping is applied to match
        legacy ArcGIS behavior.
    """

    buffered = gdf.copy()
    buffered["geometry"] = buffered.geometry.buffer(distance_m)

    if dissolve:
        # Dissolve all geometries into a single geometry
        dissolved_geom = unary_union(buffered.geometry)
        buffered = gpd.GeoDataFrame({"geometry": [dissolved_geom]}, crs=buffered.crs)

    # Apply precision snapping to match legacy ArcGIS behavior
    # This is critical for geometry consistency - buffer and unary_union operations
    # can introduce small coordinate variations that cascade through subsequent calculations
    return apply_precision(buffered, grid_size=grid_size)



def spatial_difference_with_precision(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Spatial difference (erase) with precision model applied.

    Replaces ArcPy PairwiseErase operation from legacy GCN script.
    Erases areas of left that overlap with right.

    Args:
        left: GeoDataFrame to erase from
        right: GeoDataFrame defining erase areas
        grid_size: Grid size for precision model (default: 0.0001m)

    Returns:
        Difference overlay result (left minus overlaps with right)
    """

    # Apply precision before overlay
    left_precise = apply_precision(left, grid_size=grid_size)
    right_precise = apply_precision(right, grid_size=grid_size)

    # Perform difference overlay
    result = gpd.overlay(left_precise, right_precise, how="difference", keep_geom_type=False)

    # Apply precision to result - overlay operations can introduce new vertices
    # and numerical imprecision, so we snap the output back to the grid to
    # maintain consistency with ArcGIS precision model behavior
    return apply_precision(result, grid_size=grid_size)

