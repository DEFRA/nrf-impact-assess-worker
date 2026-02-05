"""Repository for querying PostGIS spatial and lookup data.

This module provides a unified interface for querying spatial layers and lookup
tables stored in PostGIS using SQLAlchemy 2.x query builder patterns.
"""

import logging
from typing import Any

import geopandas as gpd
import pandas as pd
from sqlalchemy import Select, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from worker.models.db import Base
from worker.models.enums import SpatialLayerType

logger = logging.getLogger(__name__)


class Repository:
    """Repository for accessing spatial reference data and lookup tables.

    Provides session management and query execution for PostGIS-backed spatial data.
    Supports two return modes via execute_query():
    - GeoDataFrame for spatial analysis workflows (as_gdf=True)
    - ORM objects for direct database operations (as_gdf=False)

    The repository is intentionally minimal - callers build their own SQLAlchemy
    SELECT statements and pass them to execute_query(). This keeps the repository
    simple and generic while giving assessment scripts full control over queries.

    See docs/postgis-migration.md for usage examples.

    Attributes:
        engine: SQLAlchemy engine for database connections
    """

    def __init__(self, engine: Engine):
        """Initialize repository with SQLAlchemy engine.

        Args:
            engine: SQLAlchemy engine configured for PostGIS database
        """
        self.engine = engine
        self._session_factory = sessionmaker(bind=engine, expire_on_commit=False)

    def session(self) -> Session:
        """Create a new SQLAlchemy session.

        Provides direct access to sessions for advanced use cases requiring
        transaction control or complex operations.

        Returns:
            New SQLAlchemy Session instance
        """
        return self._session_factory()

    def execute_query(self, stmt: Select, as_gdf: bool = False) -> gpd.GeoDataFrame | list[Base]:
        """Execute a SQLAlchemy SELECT statement.

        Args:
            stmt: SQLAlchemy SELECT statement to execute
            as_gdf: If True, return GeoDataFrame. If False, return list of ORM objects.

        Returns:
            GeoDataFrame if as_gdf=True, list of ORM objects otherwise

        Raises:
            ValueError: If as_gdf=True but query doesn't include geometry column
        """
        with self.session() as session:
            if as_gdf:
                # Use geopandas to read spatial data as GeoDataFrame
                # Note: geom_col='geometry' assumes geometry column name
                return gpd.read_postgis(
                    stmt, session.connection(), geom_col="geometry", crs="EPSG:27700"
                )
            # Execute and return ORM objects
            result = session.scalars(stmt).all()
            return list(result)

    def majority_overlap_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        overlay_table: type[Base],
        overlay_filter: Any,
        input_id_col: str,
        overlay_attr_col: Any,
        output_field: str,
        default_value: Any = None,
    ) -> pd.DataFrame:
        """Perform majority overlap assignment using PostGIS server-side.

        Executes a single batch SQL query instead of one query per input feature.
        Inserts input geometries into a temporary table, then uses a LATERAL join
        to find the overlay feature with the largest intersection area for each input.

        This is much faster than Python gpd.overlay() because:
        1. Uses PostGIS GiST spatial indexes
        2. Single database round-trip for all features
        3. Aggregation done server-side in SQL

        Args:
            input_gdf: Input features with geometry
            overlay_table: SQLAlchemy model for overlay layer
            overlay_filter: WHERE clause to filter overlay (e.g., version filter)
            input_id_col: ID column in input_gdf
            overlay_attr_col: Column name (str) or SQLAlchemy expression
                (e.g., ``SpatialLayer.attributes["WwTw_ID"].astext`` for JSONB)
            output_field: Name for output column
            default_value: Value when no intersection found

        Returns:
            DataFrame with input_id_col and output_field columns
        """
        if len(input_gdf) == 0:
            return pd.DataFrame(columns=[input_id_col, output_field])

        # Resolve overlay_attr_col: string → getattr, otherwise use as expression
        if isinstance(overlay_attr_col, str):
            overlay_attr = getattr(overlay_table, overlay_attr_col)
        else:
            overlay_attr = overlay_attr_col

        with self.session() as session:
            # 1. Create a temporary table for input geometries
            session.execute(text(
                "CREATE TEMPORARY TABLE _tmp_input_geom ("
                "  input_id integer, "
                "  geom geometry(Geometry, 27700)"
                ") ON COMMIT DROP"
            ))

            # 2. Bulk-insert all input geometries in one statement
            insert_values = [
                {"input_id": int(row[input_id_col]), "geom_wkt": row.geometry.wkt}
                for _, row in input_gdf.iterrows()
            ]
            session.execute(
                text(
                    "INSERT INTO _tmp_input_geom (input_id, geom) "
                    "VALUES (:input_id, ST_SetSRID(ST_GeomFromText(:geom_wkt), 27700))"
                ),
                insert_values,
            )

            # 3. Build a spatial index on the temp table for efficient joining
            session.execute(text(
                "CREATE INDEX ON _tmp_input_geom USING GIST (geom)"
            ))

            # 4. Build the LATERAL join query as raw SQL.
            #    Compile the overlay filter and attr expression, then replace
            #    the schema-qualified table name with alias "t".
            table = overlay_table.__table__
            schema = table.schema
            table_name = table.name
            qualified = f"{schema}.{table_name}" if schema else table_name

            filter_str = str(overlay_filter.compile(
                dialect=session.bind.dialect,
                compile_kwargs={"literal_binds": True},
            ))
            attr_str = str(overlay_attr.compile(
                dialect=session.bind.dialect,
                compile_kwargs={"literal_binds": True},
            ))

            # Replace schema-qualified table references with alias
            filter_sql = filter_str.replace(f"{qualified}.", "t.")
            attr_sql = attr_str.replace(f"{qualified}.", "t.")

            raw_sql = text(f"""
                SELECT i.input_id, best.attr_val
                FROM _tmp_input_geom i
                LEFT JOIN LATERAL (
                    SELECT {attr_sql} AS attr_val
                    FROM {qualified} t
                    WHERE {filter_sql}
                      AND ST_Intersects(t.geometry, i.geom)
                    ORDER BY ST_Area(ST_Intersection(t.geometry, i.geom)) DESC
                    LIMIT 1
                ) best ON true
            """)

            rows = session.execute(raw_sql).fetchall()

        df = pd.DataFrame(rows, columns=[input_id_col, output_field])

        if default_value is not None:
            df[output_field] = df[output_field].fillna(default_value)

        return df

    def land_use_intersection_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        coeff_version: int,
        nn_version: int,
    ) -> pd.DataFrame:
        """Perform 3-way spatial intersection (RLB × coefficient × NN catchment) in PostGIS.

        Replaces the Python-side overlay chain with a single SQL query that:
        1. Inserts RLB geometries into a temp table
        2. JOINs against coefficient_layer and spatial_layer (nn_catchments)
        3. Returns intersection areas in hectares (no geometry transfer)

        Args:
            input_gdf: RLB GeoDataFrame with rlb_id, dwellings, name,
                       dwelling_category, source, and geometry columns
            coeff_version: Version of the coefficient layer to use
            nn_version: Version of the NN catchments spatial layer to use

        Returns:
            DataFrame with columns: rlb_id, dwellings, name, dwelling_category,
            source, crome_id, lu_curr_n_coeff, lu_curr_p_coeff, n_resi_coeff,
            p_resi_coeff, n2k_site_n, area_in_nn_catchment_ha
        """
        if len(input_gdf) == 0:
            return pd.DataFrame(columns=[
                "rlb_id", "dwellings", "name", "dwelling_category", "source",
                "crome_id", "lu_curr_n_coeff", "lu_curr_p_coeff",
                "n_resi_coeff", "p_resi_coeff",
                "n2k_site_n", "area_in_nn_catchment_ha",
            ])

        with self.session() as session:
            # 1. Create temp table for RLB geometries
            session.execute(text(
                "CREATE TEMPORARY TABLE _tmp_rlb ("
                "  rlb_id integer, "
                "  dwellings integer, "
                "  name text, "
                "  dwelling_category text, "
                "  source text, "
                "  geom geometry(Geometry, 27700)"
                ") ON COMMIT DROP"
            ))

            # 2. Bulk-insert RLB geometries
            insert_values = [
                {
                    "rlb_id": int(row["rlb_id"]),
                    "dwellings": int(row["dwellings"]),
                    "name": str(row["name"]),
                    "dwelling_category": str(row["dwelling_category"]),
                    "source": str(row["source"]),
                    "geom_wkt": row.geometry.wkt,
                }
                for _, row in input_gdf.iterrows()
            ]
            session.execute(
                text(
                    "INSERT INTO _tmp_rlb "
                    "(rlb_id, dwellings, name, dwelling_category, source, geom) "
                    "VALUES (:rlb_id, :dwellings, :name, :dwelling_category, :source, "
                    "ST_SetSRID(ST_GeomFromText(:geom_wkt), 27700))"
                ),
                insert_values,
            )

            # 3. Build spatial index on temp table
            session.execute(text(
                "CREATE INDEX ON _tmp_rlb USING GIST (geom)"
            ))

            # 4. 3-way intersection query
            raw_sql = text("""
                SELECT
                    r.rlb_id, r.dwellings, r.name, r.dwelling_category, r.source,
                    c.crome_id, c.lu_curr_n_coeff, c.lu_curr_p_coeff,
                    c.n_resi_coeff, c.p_resi_coeff,
                    nn.attributes->>'N2K_Site_N' AS n2k_site_n,
                    ST_Area(ST_Intersection(ST_Intersection(r.geom, c.geometry), nn.geometry))
                        / 10000.0 AS area_in_nn_catchment_ha
                FROM _tmp_rlb r
                JOIN nrf_reference.coefficient_layer c
                    ON c.version = :coeff_version
                    AND ST_Intersects(r.geom, c.geometry)
                JOIN nrf_reference.spatial_layer nn
                    ON nn.layer_type = CAST(:nn_layer_type AS nrf_reference.spatial_layer_type)
                    AND nn.version = :nn_version
                    AND ST_Intersects(r.geom, nn.geometry)
                    AND ST_Intersects(c.geometry, nn.geometry)
                WHERE ST_Area(
                    ST_Intersection(ST_Intersection(r.geom, c.geometry), nn.geometry)
                ) > 0
            """)

            rows = session.execute(
                raw_sql,
                {
                    "coeff_version": coeff_version,
                    "nn_version": nn_version,
                    "nn_layer_type": SpatialLayerType.NN_CATCHMENTS.name,
                },
            ).fetchall()

        columns = [
            "rlb_id", "dwellings", "name", "dwelling_category", "source",
            "crome_id", "lu_curr_n_coeff", "lu_curr_p_coeff",
            "n_resi_coeff", "p_resi_coeff",
            "n2k_site_n", "area_in_nn_catchment_ha",
        ]
        return pd.DataFrame(rows, columns=columns)

    def intersection_postgis(
        self,
        input_gdf: gpd.GeoDataFrame,
        overlay_table: type[Base],
        overlay_filter: Any,
        overlay_columns: list[str],
    ) -> gpd.GeoDataFrame:
        """Perform spatial intersection using PostGIS server-side.

        Returns intersection geometries with attributes from the overlay layer.
        Much faster than gpd.overlay() for large overlay layers.

        Args:
            input_gdf: Input features
            overlay_table: SQLAlchemy model for overlay layer
            overlay_filter: WHERE clause for overlay
            overlay_columns: Columns to include from overlay

        Returns:
            GeoDataFrame with intersection geometries
        """
        from geoalchemy2.functions import (
            ST_GeomFromText,
            ST_Intersection,
            ST_Intersects,
            ST_SetSRID,
        )

        input_union = input_gdf.union_all()
        input_wkt = input_union.wkt

        overlay_cols = [getattr(overlay_table, col) for col in overlay_columns]

        stmt = (
            select(
                *overlay_cols,
                ST_Intersection(
                    overlay_table.geometry,
                    ST_SetSRID(ST_GeomFromText(input_wkt), 27700),
                ).label("geometry"),
            ).where(
                overlay_filter,
                ST_Intersects(
                    overlay_table.geometry,
                    ST_SetSRID(ST_GeomFromText(input_wkt), 27700),
                ),
            )
        )

        return gpd.read_postgis(stmt, self.engine, geom_col="geometry", crs="EPSG:27700")

    def close(self) -> None:
        """Close the repository and dispose of the engine.

        Should be called when the repository is no longer needed,
        typically at application shutdown.
        """
        self.engine.dispose()

    def __enter__(self) -> "Repository":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - close repository."""
        self.close()
