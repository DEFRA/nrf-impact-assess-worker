"""Spatial overlay operations for assessments.

This module provides spatial overlay operations commonly used in assessments,
particularly for GCN (Great Crested Newt) assessments that require:
- Buffer operations with dissolve
- Difference (erase) with precision control

These operations replace ArcPy equivalents with GeoPandas/Shapely implementations.

Note: Spatial intersection with precision is provided by spatial_join_intersect()
in worker.spatial.operations.
"""

import logging
import os
from concurrent.futures import ProcessPoolExecutor

import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union

from worker.spatial.utils import apply_precision

logger = logging.getLogger(__name__)


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
    parallel: bool = True,
    max_workers: int | None = None,
) -> gpd.GeoDataFrame:
    """Spatial difference (erase) with precision model applied.

    Replaces ArcPy PairwiseErase operation from legacy GCN script.
    Erases areas of left that overlap with right.

    Args:
        left: GeoDataFrame to erase from
        right: GeoDataFrame defining erase areas
        grid_size: Grid size for precision model (default: 0.0001m)
        parallel: Enable chunked parallel difference for large inputs
        max_workers: Number of worker processes (default: 80% of cpu_count)

    Returns:
        Difference overlay result (left minus overlaps with right)
    """
    if left.crs != right.crs:
        right = right.to_crs(left.crs)

    # Apply precision before overlay
    left_precise = apply_precision(left, grid_size=grid_size)
    right_precise = apply_precision(right, grid_size=grid_size)

    # Use sequential for small datasets or when disabled
    if not parallel or len(left_precise) < 100:
        result = gpd.overlay(left_precise, right_precise, how="difference", keep_geom_type=False)
        return apply_precision(result, grid_size=grid_size)

    if max_workers is None:
        max_workers = max(1, int((os.cpu_count() or 4) * 0.8))

    chunks = _partition_by_bounds(left_precise, max_workers)
    if len(chunks) <= 1:
        result = gpd.overlay(left_precise, right_precise, how="difference", keep_geom_type=False)
        return apply_precision(result, grid_size=grid_size)

    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_difference_chunk, chunk, right_precise) for chunk in chunks
            ]
            results = [f.result() for f in futures]
    except (NotImplementedError, PermissionError, OSError) as exc:
        logger.warning(
            f"Parallel spatial_difference unavailable ({exc}); falling back to sequential"
        )
        result = gpd.overlay(left_precise, right_precise, how="difference", keep_geom_type=False)
        return apply_precision(result, grid_size=grid_size)

    result = gpd.GeoDataFrame(
        pd.concat(results, ignore_index=True),
        crs=left_precise.crs,
    )

    # Apply precision to result - overlay operations can introduce new vertices
    # and numerical imprecision, so we snap the output back to the grid to
    # maintain consistency with ArcGIS precision model behavior
    return apply_precision(result, grid_size=grid_size)


def _difference_chunk(
    left_chunk: gpd.GeoDataFrame,
    right_gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Run a difference overlay for one left-side chunk."""
    return gpd.overlay(left_chunk, right_gdf, how="difference", keep_geom_type=False)


def _partition_by_bounds(gdf: gpd.GeoDataFrame, n_chunks: int) -> list[gpd.GeoDataFrame]:
    """Partition GeoDataFrame into non-overlapping spatial chunks."""
    if len(gdf) == 0 or n_chunks <= 1:
        return [gdf]

    bounds = gdf.total_bounds  # minx, miny, maxx, maxy
    x_range = bounds[2] - bounds[0]
    y_range = bounds[3] - bounds[1]

    centroids = gdf.geometry.centroid
    cx = centroids.x
    cy = centroids.y

    chunks = []
    if x_range >= y_range:
        step = x_range / n_chunks
        for i in range(n_chunks):
            min_x = bounds[0] + i * step
            max_x = bounds[0] + (i + 1) * step
            if i < n_chunks - 1:
                chunk = gdf[(cx >= min_x) & (cx < max_x)]
            else:
                chunk = gdf[(cx >= min_x) & (cx <= bounds[2])]
            if len(chunk) > 0:
                chunks.append(chunk)
    else:
        step = y_range / n_chunks
        for i in range(n_chunks):
            min_y = bounds[1] + i * step
            max_y = bounds[1] + (i + 1) * step
            if i < n_chunks - 1:
                chunk = gdf[(cy >= min_y) & (cy < max_y)]
            else:
                chunk = gdf[(cy >= min_y) & (cy <= bounds[3])]
            if len(chunk) > 0:
                chunks.append(chunk)

    return chunks if chunks else [gdf]
