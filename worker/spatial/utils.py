"""General spatial utilities.

This module provides common spatial utilities used across assessments:
- CRS validation and transformation
- Precision model application (for ArcGIS compatibility)
"""

import geopandas as gpd
from shapely import set_precision


def ensure_crs(gdf: gpd.GeoDataFrame, target_crs: str = "EPSG:27700") -> gpd.GeoDataFrame:
    """Ensure GeoDataFrame is in the target CRS, transforming if necessary.

    Args:
        gdf: Input GeoDataFrame
        target_crs: Target coordinate reference system (default: EPSG:27700 / BNG)

    Returns:
        GeoDataFrame in target CRS (transformed if necessary, original if
        already correct)

    Raises:
        ValueError: If input GeoDataFrame has no CRS defined
    """
    if gdf.crs is None:
        msg = "Input GeoDataFrame has no CRS defined"
        raise ValueError(msg)

    if gdf.crs != target_crs:
        return gdf.to_crs(target_crs)

    return gdf


def apply_precision(
    gdf: gpd.GeoDataFrame,
    grid_size: float = 0.0001,
) -> gpd.GeoDataFrame:
    """Apply precision model to geometries to match ArcGIS XY tolerance behavior.

    Snaps geometry coordinates to a grid to reduce numerical differences between
    GEOS (GeoPandas/Shapely) and ArcGIS geometry engines. This helps achieve
    consistent results when comparing open-source implementation against legacy
    ArcGIS outputs.

    ArcGIS XY Resolution default: 0.0001m (0.1mm) for projected coordinate systems.

    Args:
        gdf: Input GeoDataFrame
        grid_size: Grid size in metres (default: 0.0001m = 0.1mm, matches ArcGIS)

    Returns:
        GeoDataFrame with precision-snapped geometries

    Note:
        This operation may slightly modify geometry coordinates and areas.
        Use same grid_size value consistently across all operations in a workflow.

    Reference:
        https://pro.arcgis.com/en/pro-app/latest/help/editing/pdf/parcel_fabric_precision.pdf
    """
    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.apply(
        lambda geom: set_precision(geom, grid_size=grid_size) if geom else geom
    )
    return gdf
