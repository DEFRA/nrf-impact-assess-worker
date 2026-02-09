"""Nutrient impact assessment.

This module implements the nutrient impact assessment following the simplified
pluggable architecture. It owns all nutrient domain logic while the platform
provides data access through the repository.
"""

import logging
import time

import geopandas as gpd
import numpy as np
import pandas as pd
from sqlalchemy import func, select

from worker.calculators import (
    apply_buffer,
    apply_suds_mitigation,
    calculate_land_use_uplift,
    calculate_wastewater_load,
)
from worker.config import CONSTANTS, AssessmentConfig, DebugConfig, RequiredColumns
from worker.debug import save_debug_gdf
from worker.models.db import CoefficientLayer, LookupTable, SpatialLayer
from worker.models.enums import SpatialLayerType
from worker.repositories.repository import Repository

logger = logging.getLogger(__name__)


class NutrientAssessment:
    """Nutrient impact assessment.

    This assessment evaluates nutrient impacts from proposed developments by:
    - Calculating land use change impacts within nutrient neutrality catchments
    - Calculating wastewater treatment impacts
    - Applying SuDS mitigation and precautionary buffers
    - Supporting both nitrogen and phosphorus nutrient pathways
    """

    def __init__(
        self,
        rlb_gdf: gpd.GeoDataFrame,
        metadata: dict,
        repository: Repository,
    ):
        """Initialize nutrient assessment.

        Args:
            rlb_gdf: Red Line Boundary GeoDataFrame
            metadata: Must contain "unique_ref"
            repository: Data repository for loading reference data
        """
        self.rlb_gdf = rlb_gdf
        self.metadata = metadata
        self.repository = repository
        self.config = AssessmentConfig()
        self._debug_config = DebugConfig.from_env()
        self._version_cache: dict[str, int] = {}

    def run(self) -> dict[str, pd.DataFrame]:
        """Run nutrient impact assessment.

        Pipeline steps (mapping to legacy script):
        1. Validate & prepare input (legacy 80-117)
        2. Assign spatial features (legacy 121-182, 314-345)
        3. Calculate land use impacts (legacy 185-250)
        4. Calculate wastewater impacts (legacy 253-395)
        5. Apply buffer & totals (legacy 397-444)
        6. Filter out-of-scope developments (legacy 448-456)

        Returns:
            Dictionary with:
            - "impact_summary": DataFrame with nutrient impacts per development

        Raises:
            ValueError: If required columns are missing or input is invalid
            RuntimeError: If spatial operations fail
        """
        logger.info("Running nutrient impact assessment")
        t_total = time.perf_counter()

        t0 = time.perf_counter()
        rlb_gdf = self._validate_and_prepare_input(self.rlb_gdf)
        logger.info(f"[timing] validate_and_prepare_input: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = self._assign_spatial_features(rlb_gdf)
        logger.info(f"[timing] assign_spatial_features: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = self._calculate_land_use_impacts(rlb_gdf)
        logger.info(f"[timing] calculate_land_use_impacts: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = self._calculate_wastewater_impacts(rlb_gdf)
        logger.info(f"[timing] calculate_wastewater_impacts: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = self._calculate_totals(rlb_gdf)
        logger.info(f"[timing] calculate_totals: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = self._filter_out_of_scope(rlb_gdf)
        logger.info(f"[timing] filter_out_of_scope: {time.perf_counter() - t0:.3f}s")

        save_debug_gdf(rlb_gdf, "99_final_rlb", self.metadata["unique_ref"], self._debug_config)

        logger.info(f"Nutrient assessment complete in {time.perf_counter() - t_total:.3f}s")

        # Return DataFrame (drop geometry for attribute-only table)
        return {"impact_summary": rlb_gdf.drop(columns=["geometry"])}

    def _validate_and_prepare_input(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Validate and prepare input GeoDataFrame.

        Legacy script reference: Lines 80-117

        Steps:
        - Check required columns exist
        - Transform to BNG (EPSG:27700) if needed
        - Recalculate areas after reprojection
        - Assign RLB_ID sequence numbers
        - Trim to expected columns

        Args:
            rlb_gdf: Input GeoDataFrame

        Returns:
            Validated and prepared GeoDataFrame

        Raises:
            ValueError: If required columns are missing
        """
        # Make a copy to avoid modifying the input
        rlb_gdf = rlb_gdf.copy()

        # Normalize legacy input column names to snake_case (input boundary)
        # Only rename columns that are actually present in the input
        input_column_map = {
            "Name": "name",
            "Dwel_Cat": "dwelling_category",
            "Source": "source",
            "Dwellings": "dwellings",
            "Shape_Area": "shape_area",
        }
        columns_to_rename = {
            k: v for k, v in input_column_map.items()
            if k in rlb_gdf.columns and v not in rlb_gdf.columns
        }
        if columns_to_rename:
            rlb_gdf = rlb_gdf.rename(columns=columns_to_rename)

        # Validate required columns (legacy lines 91-95)
        expected_cols = RequiredColumns.all()
        missing_cols = [col for col in expected_cols if col not in rlb_gdf.columns]
        if missing_cols:
            msg = (
                f"Required columns missing from input: {missing_cols}. "
                f"Expected columns: {expected_cols}"
            )
            raise ValueError(
                msg
            )

        # Transform to BNG if needed (legacy lines 83-84)
        if rlb_gdf.crs != CONSTANTS.CRS_BRITISH_NATIONAL_GRID:
            rlb_gdf = rlb_gdf.to_crs(CONSTANTS.CRS_BRITISH_NATIONAL_GRID)

        # Recalculate shape area after reprojection (legacy lines 86-88)
        rlb_gdf[RequiredColumns.SHAPE_AREA] = rlb_gdf.geometry.area

        # Trim to expected columns (legacy line 98)
        rlb_gdf = gpd.GeoDataFrame(rlb_gdf[expected_cols])

        # Assign rlb_id sequence numbers (legacy line 117)
        rlb_gdf["rlb_id"] = range(1, len(rlb_gdf) + 1)

        return rlb_gdf

    def _resolve_latest_version(self, layer_type: SpatialLayerType) -> int:
        """Fetch the latest version number for a spatial layer type (cached)."""
        cache_key = f"spatial_{layer_type.name}"
        if cache_key not in self._version_cache:
            stmt = select(func.max(SpatialLayer.version)).where(
                SpatialLayer.layer_type == layer_type
            )
            result = self.repository.execute_query(stmt, as_gdf=False)
            self._version_cache[cache_key] = result[0] if result else 1
        return self._version_cache[cache_key]

    def _resolve_latest_coeff_version(self) -> int:
        """Fetch the latest version number for the coefficient layer (cached)."""
        cache_key = "coefficient"
        if cache_key not in self._version_cache:
            stmt = select(func.max(CoefficientLayer.version))
            result = self.repository.execute_query(stmt, as_gdf=False)
            self._version_cache[cache_key] = result[0] if result else 1
        return self._version_cache[cache_key]

    def _assign_spatial_features(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Assign spatial features via batched majority overlap.

        Legacy script reference: Lines 121-182, 314-345

        Uses a single DB session with one temp table for all 3 assignments
        (WwTW, LPA, subcatchment) instead of creating/dropping 3 separate
        temp tables.

        Args:
            rlb_gdf: GeoDataFrame with rlb_id assigned

        Returns:
            GeoDataFrame with spatial assignments added
        """
        logger.info("Assigning spatial features via batched PostGIS overlap")

        t0 = time.perf_counter()

        # Resolve versions once
        wwtw_ver = self._resolve_latest_version(SpatialLayerType.WWTW_CATCHMENTS)
        lpa_ver = self._resolve_latest_version(SpatialLayerType.LPA_BOUNDARIES)
        sub_ver = self._resolve_latest_version(SpatialLayerType.SUBCATCHMENTS)

        # Batch all 3 assignments in a single DB session / temp table
        batch_results = self.repository.batch_majority_overlap_postgis(
            input_gdf=rlb_gdf,
            input_id_col="rlb_id",
            assignments=[
                {
                    "overlay_table": SpatialLayer,
                    "overlay_filter": (
                        (SpatialLayer.layer_type == SpatialLayerType.WWTW_CATCHMENTS)
                        & (SpatialLayer.version == wwtw_ver)
                    ),
                    "overlay_attr_col": SpatialLayer.attributes["WwTw_ID"].astext,
                    "output_field": "majority_wwtw_id",
                    "default_value": self.config.fallback_wwtw_id,
                },
                {
                    "overlay_table": SpatialLayer,
                    "overlay_filter": (
                        (SpatialLayer.layer_type == SpatialLayerType.LPA_BOUNDARIES)
                        & (SpatialLayer.version == lpa_ver)
                    ),
                    "overlay_attr_col": SpatialLayer.attributes["NAME"].astext,
                    "output_field": "majority_name",
                    "default_value": "UNKNOWN",
                },
                {
                    "overlay_table": SpatialLayer,
                    "overlay_filter": (
                        (SpatialLayer.layer_type == SpatialLayerType.SUBCATCHMENTS)
                        & (SpatialLayer.version == sub_ver)
                    ),
                    "overlay_attr_col": SpatialLayer.attributes["OPCAT_NAME"].astext,
                    "output_field": "majority_opcat_name",
                    "default_value": None,
                },
            ],
        )

        # Merge results
        rlb_gdf = rlb_gdf.merge(batch_results["majority_wwtw_id"], on="rlb_id", how="left")
        rlb_gdf["majority_wwtw_id"] = pd.to_numeric(
            rlb_gdf["majority_wwtw_id"], errors="coerce"
        ).fillna(self.config.fallback_wwtw_id).astype(int)
        save_debug_gdf(
            rlb_gdf, "04_after_wwtw_assignment",
            self.metadata["unique_ref"], self._debug_config,
        )

        rlb_gdf = rlb_gdf.merge(batch_results["majority_name"], on="rlb_id", how="left")
        save_debug_gdf(
            rlb_gdf, "05_after_lpa_assignment",
            self.metadata["unique_ref"], self._debug_config,
        )

        rlb_gdf = rlb_gdf.merge(batch_results["majority_opcat_name"], on="rlb_id", how="left")
        save_debug_gdf(
            rlb_gdf, "06_after_subcatchment_assignment",
            self.metadata["unique_ref"], self._debug_config,
        )

        elapsed = time.perf_counter() - t0
        logger.info(f"[timing] spatial: batched PostGIS majority_overlap (3 layers): {elapsed:.3f}s")

        return rlb_gdf

    def _calculate_land_use_impacts(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate land use change nutrient impacts.

        Uses a single PostGIS 3-way intersection query (RLB × coefficient × NN catchment)
        to replace Python-side overlay chains. Only intersection areas are returned
        (no geometry transfer needed).

        Args:
            rlb_gdf: GeoDataFrame with spatial assignments

        Returns:
            GeoDataFrame with land use impact columns added
        """
        logger.info("Calculating land use impacts")

        # Resolve versions for both layers
        nn_version = self._resolve_latest_version(SpatialLayerType.NN_CATCHMENTS)
        coeff_version = self._resolve_latest_coeff_version()

        # Single PostGIS query for the 3-way intersection
        land_use_intersections = self.repository.land_use_intersection_postgis(
            input_gdf=rlb_gdf,
            coeff_version=coeff_version,
            nn_version=nn_version,
        )
        logger.info(
            f"PostGIS land use intersection returned {len(land_use_intersections):,} rows"
        )

        if len(land_use_intersections) == 0:
            logger.info("No 3-way intersections found - no land use impacts")
            rlb_gdf["area_in_nn_catchment_ha"] = np.nan
            rlb_gdf["n_lu_uplift"] = np.nan
            rlb_gdf["p_lu_uplift"] = np.nan
            rlb_gdf["nn_catchment"] = None
            rlb_gdf["n_lu_post_suds"] = 0.0
            rlb_gdf["p_lu_post_suds"] = 0.0
            return rlb_gdf

        # Convert coefficient columns to numeric (legacy lines 211-215)
        land_use_intersections["n_resi_coeff"] = pd.to_numeric(
            land_use_intersections["n_resi_coeff"], errors="coerce"
        )
        land_use_intersections["lu_curr_n_coeff"] = pd.to_numeric(
            land_use_intersections["lu_curr_n_coeff"], errors="coerce"
        )
        land_use_intersections["p_resi_coeff"] = pd.to_numeric(
            land_use_intersections["p_resi_coeff"], errors="coerce"
        )
        land_use_intersections["lu_curr_p_coeff"] = pd.to_numeric(
            land_use_intersections["lu_curr_p_coeff"], errors="coerce"
        )

        # Calculate uplift per intersection (legacy lines 218-223)
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=land_use_intersections["area_in_nn_catchment_ha"],
            current_nitrogen_coeff=land_use_intersections["lu_curr_n_coeff"],
            residential_nitrogen_coeff=land_use_intersections["n_resi_coeff"],
            current_phosphorus_coeff=land_use_intersections["lu_curr_p_coeff"],
            residential_phosphorus_coeff=land_use_intersections["p_resi_coeff"],
        )
        land_use_intersections["n_lu_uplift"] = n_uplift
        land_use_intersections["p_lu_uplift"] = p_uplift

        # Aggregate uplift by rlb_id (legacy lines 230-242)
        uplift_sum = (
            land_use_intersections.groupby("rlb_id")
            .agg(
                {
                    "area_in_nn_catchment_ha": "sum",
                    "n_lu_uplift": "sum",
                    "p_lu_uplift": "sum",
                    "n2k_site_n": lambda x: "; ".join(sorted(set(x.dropna()))),
                }
            )
            .reset_index()
            .rename(columns={"n2k_site_n": "nn_catchment"})
        )

        # Merge summed uplifts back onto original RLB (legacy lines 245-249)
        rlb_gdf = rlb_gdf.merge(uplift_sum, on="rlb_id", how="left")

        # Apply SuDS mitigation (legacy lines 273-283)
        n_post_suds, p_post_suds = apply_suds_mitigation(
            nitrogen_uplift=rlb_gdf["n_lu_uplift"].fillna(0),
            phosphorus_uplift=rlb_gdf["p_lu_uplift"].fillna(0),
            dwelling_count=rlb_gdf["dwellings"],
            suds_config=self.config.suds,
        )
        rlb_gdf["n_lu_post_suds"] = n_post_suds
        rlb_gdf["p_lu_post_suds"] = p_post_suds

        return rlb_gdf

    def _calculate_wastewater_impacts(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate wastewater treatment nutrient impacts.

        Legacy script reference: Lines 253-395

        Steps:
        - Load and merge rates lookup
        - Calculate daily water usage
        - Load and merge WwTW lookup
        - Fill wwtw_subcatchment for fallback cases
        - Calculate wastewater loads

        Args:
            rlb_gdf: GeoDataFrame with land use impacts

        Returns:
            GeoDataFrame with wastewater impact columns added
        """
        logger.info("Calculating wastewater impacts")
        t_ww = time.perf_counter()

        # Guard against duplicate columns from upstream overlay/merge operations.
        # gpd.overlay can produce suffixed duplicates (e.g. name_1, name_2) but in
        # some data conditions pandas 3.x merges propagate true duplicates that break
        # downstream arithmetic alignment.
        dupes = rlb_gdf.columns[rlb_gdf.columns.duplicated()].tolist()
        if dupes:
            logger.warning(f"Dropping duplicate columns from rlb_gdf: {dupes}")
            rlb_gdf = rlb_gdf.loc[:, ~rlb_gdf.columns.duplicated()]

        # Load rates lookup (legacy lines 253-271)
        t0 = time.perf_counter()
        stmt = (
            select(LookupTable)
            .where(LookupTable.name == "rates_lookup")
            .order_by(LookupTable.version.desc())
            .limit(1)
        )
        rates_lookup_obj = self.repository.execute_query(stmt, as_gdf=False)[0]
        rates_lookup = pd.DataFrame(rates_lookup_obj.data)

        # Select only needed columns to avoid column name collisions during merge
        rates_lookup = rates_lookup[
            ["nn_catchment", "occupancy_rate", "water_usage_L_per_person_day"]
        ].drop_duplicates(subset=["nn_catchment"])
        logger.info(f"[timing] wastewater: load rates_lookup: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        rlb_gdf = rlb_gdf.merge(
            rates_lookup, how="left", on="nn_catchment"
        )

        # Fill missing values using mean of rows with same majority_wwtw_id
        for col in ["occupancy_rate", "water_usage_L_per_person_day"]:
            rlb_gdf[col] = rlb_gdf.groupby("majority_wwtw_id")[col].transform(
                lambda x: x.fillna(x.mean())
            )

        # Legacy line 287
        rlb_gdf["daily_water_usage_L"] = rlb_gdf["dwellings"] * (
            rlb_gdf["occupancy_rate"] * rlb_gdf["water_usage_L_per_person_day"]
        )
        elapsed = time.perf_counter() - t0
        logger.info(f"[timing] wastewater: merge rates + fill + daily_water: {elapsed:.3f}s")

        # Load WwTW lookup (legacy lines 294-311)
        t0 = time.perf_counter()
        stmt = (
            select(LookupTable)
            .where(LookupTable.name == "wwtw_lookup")
            .order_by(LookupTable.version.desc())
            .limit(1)
        )
        wwtw_lookup_obj = self.repository.execute_query(stmt, as_gdf=False)[0]
        wwtw_lookup = pd.DataFrame(wwtw_lookup_obj.data)

        # Select only needed columns to avoid column name collisions during merge
        wwtw_lookup = wwtw_lookup[
            [
                "wwtw_code",
                "wwtw_name",
                "wwtw_subcatchment",
                "nitrogen_conc_2025_2030_mg_L",
                "nitrogen_conc_2030_onwards_mg_L",
                "phosphorus_conc_2025_2030_mg_L",
                "phosphorus_conc_2030_onwards_mg_L",
            ]
        ]

        # Convert WwTW codes to integer format (legacy lines 302-303)
        wwtw_lookup["wwtw_code"] = pd.to_numeric(wwtw_lookup["wwtw_code"], errors="coerce").astype(
            "Int64"
        )

        # Deduplicate lookup to prevent many-to-many merge
        wwtw_lookup = wwtw_lookup.drop_duplicates(subset=["wwtw_code"])
        logger.info(f"[timing] wastewater: load wwtw_lookup: {time.perf_counter() - t0:.3f}s")

        t0 = time.perf_counter()
        # Merge WwTW lookup data (legacy lines 306-311)
        rlb_gdf = rlb_gdf.merge(
            wwtw_lookup, how="left", left_on="majority_wwtw_id", right_on="wwtw_code"
        )

        # Fill wwtw_subcatchment for fallback (legacy lines 343-345)
        mask = (rlb_gdf["majority_wwtw_id"] == self.config.fallback_wwtw_id) & (
            rlb_gdf["wwtw_subcatchment"].isna()
        )
        rlb_gdf.loc[mask, "wwtw_subcatchment"] = rlb_gdf.loc[mask, "majority_opcat_name"]

        # Drop duplicate wwtw_code column (legacy line 353)
        rlb_gdf = rlb_gdf.drop(columns=["wwtw_code"], errors="ignore")

        # Convert concentration columns to float (legacy lines 356-364)
        cols_to_float = [
            "nitrogen_conc_2025_2030_mg_L",
            "nitrogen_conc_2030_onwards_mg_L",
            "phosphorus_conc_2025_2030_mg_L",
            "phosphorus_conc_2030_onwards_mg_L",
        ]
        rlb_gdf[cols_to_float] = rlb_gdf[cols_to_float].astype(float)
        elapsed = time.perf_counter() - t0
        logger.info(f"[timing] wastewater: merge wwtw + type conversion: {elapsed:.3f}s")

        # Calculate wastewater loads (legacy lines 378-389)
        t0 = time.perf_counter()
        # Temporary loads (2025-2030)
        _, n_wwtw_temp, p_wwtw_temp = calculate_wastewater_load(
            dwellings=rlb_gdf["dwellings"],
            occupancy_rate=rlb_gdf["occupancy_rate"].fillna(0),
            water_usage_litres_per_person_per_day=rlb_gdf["water_usage_L_per_person_day"].fillna(0),
            nitrogen_conc_mg_per_litre=rlb_gdf["nitrogen_conc_2025_2030_mg_L"].fillna(0),
            phosphorus_conc_mg_per_litre=rlb_gdf["phosphorus_conc_2025_2030_mg_L"].fillna(0),
        )
        rlb_gdf["n_wwtw_temp"] = n_wwtw_temp
        rlb_gdf["p_wwtw_temp"] = p_wwtw_temp

        # Permanent loads (2030 onwards)
        _, n_wwtw_perm, p_wwtw_perm = calculate_wastewater_load(
            dwellings=rlb_gdf["dwellings"],
            occupancy_rate=rlb_gdf["occupancy_rate"].fillna(0),
            water_usage_litres_per_person_per_day=rlb_gdf["water_usage_L_per_person_day"].fillna(0),
            nitrogen_conc_mg_per_litre=rlb_gdf["nitrogen_conc_2030_onwards_mg_L"].fillna(0),
            phosphorus_conc_mg_per_litre=rlb_gdf["phosphorus_conc_2030_onwards_mg_L"].fillna(0),
        )
        rlb_gdf["n_wwtw_perm"] = n_wwtw_perm
        rlb_gdf["p_wwtw_perm"] = p_wwtw_perm
        elapsed = time.perf_counter() - t0
        logger.info(f"[timing] wastewater: calculate loads (vectorized): {elapsed:.3f}s")
        logger.info(f"[timing] wastewater: TOTAL: {time.perf_counter() - t_ww:.3f}s")

        return rlb_gdf

    def _calculate_totals(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Calculate total impacts with precautionary buffer.

        Legacy script reference: Lines 397-444

        Steps:
        - Apply precautionary buffer
        - Calculate development area in hectares
        - Batch rounding to 2dp

        Args:
            rlb_gdf: GeoDataFrame with all impact calculations

        Returns:
            GeoDataFrame with total impact columns and final formatting
        """
        logger.info("Calculating totals with precautionary buffer")

        # Apply precautionary buffer (legacy lines 401-411)
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=rlb_gdf["n_lu_post_suds"].fillna(0),
            phosphorus_land_use_post_suds=rlb_gdf["p_lu_post_suds"].fillna(0),
            nitrogen_wastewater=rlb_gdf["n_wwtw_perm"].fillna(0),
            phosphorus_wastewater=rlb_gdf["p_wwtw_perm"].fillna(0),
            precautionary_buffer_percent=self.config.precautionary_buffer_percent,
        )
        rlb_gdf["n_total"] = n_total
        rlb_gdf["p_total"] = p_total

        # Legacy line 417
        rlb_gdf["dev_area_ha"] = (
            rlb_gdf[RequiredColumns.SHAPE_AREA] / CONSTANTS.SQUARE_METRES_PER_HECTARE
        ).round(2)

        # Batch rounding to 2dp (legacy lines 435-443)
        round_cols = [
            "area_in_nn_catchment_ha",
            "n_wwtw_temp",
            "p_wwtw_temp",
            "n_wwtw_perm",
            "p_wwtw_perm",
            "n_total",
            "p_total",
        ]
        existing_round_cols = [col for col in round_cols if col in rlb_gdf.columns]
        rlb_gdf[existing_round_cols] = rlb_gdf[existing_round_cols].round(2)

        return rlb_gdf

    def _filter_out_of_scope(self, rlb_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Filter out developments that are out of scope.

        Legacy script reference: Lines 448-456

        Removes:
        - Developments outside NN catchment AND outside WwTW catchment
        - Package Treatment Plant defaults outside NN catchment

        Args:
            rlb_gdf: GeoDataFrame with all calculations

        Returns:
            Filtered GeoDataFrame containing only in-scope developments
        """
        # Remove developments outside all catchments (legacy line 452)
        rlb_gdf = rlb_gdf[
            ~((rlb_gdf["area_in_nn_catchment_ha"].isna()) & (rlb_gdf["wwtw_name"].isna()))
        ]

        # Remove Package Treatment Plant outside NN (legacy line 456)
        return rlb_gdf[
            ~(
                (rlb_gdf["area_in_nn_catchment_ha"].isna())
                & (rlb_gdf["wwtw_name"] == "Package Treatment Plant default")
            )
        ]

