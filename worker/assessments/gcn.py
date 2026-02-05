"""GCN (Great crested newt) impact assessment.

This module implements the GCN assessment following the simplified pluggable
architecture. It owns all GCN domain logic while the platform provides data
access through the repository.
"""

import logging

import geopandas as gpd
import pandas as pd
from geoalchemy2.functions import ST_GeomFromText, ST_Intersects, ST_SetSRID
from shapely.ops import unary_union
from sqlalchemy import select

from worker.models.db import SpatialLayer
from worker.models.enums import SpatialLayerType
from worker.repositories.repository import Repository
from worker.spatial.operations import clip_gdf, make_valid_geometries, spatial_join_intersect
from worker.spatial.overlay import buffer_with_dissolve
from worker.spatial.utils import ensure_crs

logger = logging.getLogger(__name__)

BUFFER_DISTANCE_M = 250


class GcnAssessment:
    """GCN (Great Crested Newt) impact assessment.

    This assessment evaluates impacts on Great Crested Newt populations by:
    - Calculating habitat impacts within risk zones
    - Analyzing pond frequencies by zone and survey status
    - Supporting both national and survey pond datasets
    """

    def __init__(
        self,
        rlb_gdf: gpd.GeoDataFrame,
        metadata: dict,
        repository: Repository,
    ):
        """Initialize GCN assessment.

        Args:
            rlb_gdf: Red Line Boundary GeoDataFrame (already in BNG)
            metadata: Must contain "unique_ref", optionally "survey_ponds_path"
            repository: Data repository for loading reference data
        """
        self.rlb_gdf = rlb_gdf
        self.metadata = metadata
        self.repository = repository

    def run(self) -> dict[str, pd.DataFrame]:
        """Run GCN impact assessment.

        Returns:
            Dictionary with:
            - "habitat_impact": DataFrame with habitat impact by risk zone
            - "pond_frequency": DataFrame with pond counts by zone/status

        Raises:
            KeyError: If required metadata fields are missing
            ValueError: If data validation fails
        """
        unique_ref = self.metadata["unique_ref"]
        survey_ponds_path = self.metadata.get("survey_ponds_path")

        logger.info(f"Running GCN assessment: {unique_ref}")

        # Prepare RLB and buffer first to create spatial filter extent
        rlb = ensure_crs(self.rlb_gdf)
        rlb = make_valid_geometries(rlb)
        rlb["UniqueRef"] = unique_ref

        logger.info(f"Creating {BUFFER_DISTANCE_M}m buffer")
        rlb_buffered = buffer_with_dissolve(rlb, BUFFER_DISTANCE_M, dissolve=True)
        rlb_buffered["Area"] = "Buffer"

        rlb["Area"] = "RLB"
        rlb_with_buffer = pd.concat([rlb, rlb_buffered], ignore_index=True)

        # Create combined extent for spatial filtering
        combined_extent = gpd.GeoDataFrame(
            {"geometry": [unary_union(rlb_with_buffer.geometry)]}, crs=rlb_with_buffer.crs
        )
        filter_wkt = combined_extent.union_all().wkt

        # Load risk zones (14k features - load all, clip locally)
        logger.info("Loading risk zones from repository")
        stmt = select(SpatialLayer).where(
            SpatialLayer.layer_type == SpatialLayerType.GCN_RISK_ZONES
        )
        risk_zones = self.repository.execute_query(stmt, as_gdf=True)

        # Extract RZ from attributes JSONB column
        if "attributes" in risk_zones.columns:
            risk_zones["RZ"] = risk_zones["attributes"].apply(lambda x: x.get("RZ") if x else None)
        logger.info(f"Loaded {len(risk_zones)} risk zone features")

        # Load ponds - either from survey file or national dataset with spatial filtering
        if survey_ponds_path:
            logger.info(f"Loading survey ponds: {survey_ponds_path}")
            ponds = gpd.read_file(survey_ponds_path)
            ponds = ensure_crs(ponds)
            if "PANS" not in ponds.columns:
                raise ValueError("Survey ponds must have 'PANS' column")
            if "TmpImp" not in ponds.columns:
                logger.warning("Survey ponds missing 'TmpImp' column, defaulting to 'F'")
                ponds["TmpImp"] = "F"
        else:
            # Load national ponds with spatial filtering (457k → ~few hundred)
            logger.info("Loading national ponds with spatial filtering")
            stmt = select(SpatialLayer).where(
                SpatialLayer.layer_type == SpatialLayerType.GCN_PONDS,
                ST_Intersects(
                    SpatialLayer.geometry,
                    ST_SetSRID(ST_GeomFromText(filter_wkt), 27700),
                ),
            )
            ponds = self.repository.execute_query(stmt, as_gdf=True)
            ponds["PANS"] = "NS"  # Not Surveyed
            ponds["TmpImp"] = "F"  # No temporary impact
            logger.info(f"Loaded {len(ponds)} ponds within RLB+buffer extent")

        logger.info("Clipping risk zones to RLB+Buffer extent")
        risk_zones_clipped = clip_gdf(risk_zones, combined_extent)
        logger.info(f"Clipped to {len(risk_zones_clipped)} risk zone features")

        logger.info("Assigning ponds to RLB and Buffer areas")
        # CRITICAL: Match legacy script's pond selection order (see docs/bug-fix.md)
        # Step 1: Clip ALL ponds to combined extent first (creates localized subset)
        all_ponds_clipped = spatial_join_intersect(ponds, combined_extent[["geometry"]])

        # Step 2: Select RLB ponds from the clipped subset
        ponds_in_rlb = spatial_join_intersect(all_ponds_clipped, rlb[["geometry"]])
        ponds_in_rlb["Area"] = "RLB"

        # Step 3: Select buffer ponds from clipped subset (inverted selection)
        # Find ponds that intersect RLB, then get ponds NOT in that set
        rlb_intersecting_ponds = gpd.sjoin(
            all_ponds_clipped, rlb[["geometry"]], predicate="intersects", how="inner"
        )
        ponds_in_buffer = all_ponds_clipped[
            ~all_ponds_clipped.index.isin(rlb_intersecting_ponds.index)
        ].copy()
        ponds_in_buffer["Area"] = "Buffer"

        all_ponds = pd.concat([ponds_in_rlb, ponds_in_buffer], ignore_index=True)
        logger.info(f"Found {len(ponds_in_rlb)} ponds in RLB, {len(ponds_in_buffer)} in buffer")

        logger.info("Calculating habitat impact")
        habitat_impact = _calculate_habitat_impact(rlb_with_buffer, risk_zones_clipped, all_ponds)

        logger.info("Calculating pond frequency")
        pond_frequency = _calculate_pond_frequency(
            ponds_in_rlb, ponds_in_buffer, risk_zones_clipped
        )

        logger.info("GCN assessment complete")

        return {"habitat_impact": habitat_impact, "pond_frequency": pond_frequency}


def _calculate_habitat_impact(
    rlb_with_buffer: gpd.GeoDataFrame,
    risk_zones: gpd.GeoDataFrame,
    ponds: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Calculate habitat impact by risk zone.

    Calculates the area of RLB and buffer that falls within GCN risk zones
    (defined as 250m buffer around ponds).

    Args:
        rlb_with_buffer: RLB and buffer combined
        risk_zones: GCN risk zones (already clipped to RLB+buffer extent)
        ponds: All ponds (RLB + buffer)

    Returns:
        DataFrame with columns:
        - Area: "RLB" or "Buffer"
        - RZ: Risk zone ("Red", "Amber", or "Green")
        - Shape_Area: Area in square metres
    """
    # Buffer ponds by 250m and dissolve
    ponds_buffered = buffer_with_dissolve(ponds, BUFFER_DISTANCE_M, dissolve=True)

    # Clip risk zones to pond buffer (only habitat within pond buffers counts)
    risk_zones_in_pond_buffer = clip_gdf(risk_zones, ponds_buffered)

    # Intersect with RLB+Buffer to get habitat impact
    habitat_impact = spatial_join_intersect(rlb_with_buffer, risk_zones_in_pond_buffer)

    # Calculate areas
    habitat_impact["Shape_Area"] = habitat_impact.geometry.area

    # Filter out zero-area geometries (touching but not overlapping)
    # This can happen when geometries share an edge but don't overlap
    habitat_impact = habitat_impact[habitat_impact["Shape_Area"] > 0].copy()

    # Select output columns (drop geometry for attribute-only table)
    result = habitat_impact[["Area", "RZ", "Shape_Area"]].copy()

    return result


def _calculate_pond_frequency(
    ponds_in_rlb: gpd.GeoDataFrame,
    ponds_in_buffer: gpd.GeoDataFrame,
    risk_zones: gpd.GeoDataFrame,
) -> pd.DataFrame:
    """Calculate pond frequency by zone and status.

    Aggregates pond counts by:
    - PANS status (P/A/NS)
    - Area (RLB/Buffer)
    - MaxZone (Red/Amber/Green)
    - TmpImp (T/F)

    Args:
        ponds_in_rlb: Ponds within RLB
        ponds_in_buffer: Ponds in buffer area
        risk_zones: GCN risk zones

    Returns:
        DataFrame with columns:
        - PANS: "P", "A", or "NS"
        - Area: "RLB" or "Buffer"
        - MaxZone: "Red", "Amber", or "Green"
        - TmpImp: "T" or "F"
        - FREQUENCY: Count of ponds
    """
    # Add pond IDs
    ponds_in_rlb = ponds_in_rlb.copy()
    ponds_in_buffer = ponds_in_buffer.copy()

    ponds_in_rlb["Pond_ID"] = ["RLB_" + str(i) for i in range(len(ponds_in_rlb))]
    ponds_in_buffer["Pond_ID"] = ["BUF_" + str(i) for i in range(len(ponds_in_buffer))]

    # Combine and assign risk zones
    all_ponds = pd.concat([ponds_in_rlb, ponds_in_buffer], ignore_index=True)

    # Join ponds with risk zones to get which zones each pond intersects
    ponds_with_zones = spatial_join_intersect(all_ponds, risk_zones[["geometry", "RZ"]])

    # Group by pond and concatenate zones (only need Pond_ID, PANS, TmpImp, Area, RZ)
    pond_zones = (
        ponds_with_zones[["Pond_ID", "PANS", "TmpImp", "Area", "RZ"]]
        .groupby(["Pond_ID", "PANS", "TmpImp", "Area"])["RZ"]
        .apply(lambda x: ":".join(sorted(set(x))))
        .reset_index()
        .rename(columns={"RZ": "CONCATENATE_RZ"})
    )

    # TODO: rework this, to remove nested function
    # Determine MaxZone (Red > Amber > Green)
    def highest_zone(zones: str) -> str:
        zone_list = zones.split(":")
        if "Red" in zone_list:
            return "Red"
        if "Amber" in zone_list:
            return "Amber"
        if "Green" in zone_list:
            return "Green"
        return zone_list[0] if zone_list else "Unknown"

    pond_zones["MaxZone"] = pond_zones["CONCATENATE_RZ"].apply(highest_zone)

    frequency = (
        pond_zones.groupby(["PANS", "Area", "MaxZone", "TmpImp"])
        .size()
        .reset_index(name="FREQUENCY")
    )

    return frequency
