"""SQLAlchemy database models for PostGIS reference data.

This module defines the database schema for storing spatial reference data
and lookup tables in PostgreSQL with PostGIS extension.

Design approach:
- Dedicated CoefficientLayer model for 5.4M coefficient polygons (performance-critical)
- Unified SpatialLayer model for supporting spatial data (catchments, boundaries)
- JSONB-based LookupTable model for lookup tables (WwTW, rates)
- PostgreSQL ENUM types for spatial layer discriminators
- UUID primary keys for all tables
- Explicit columns (no JSONB) for optimal query performance
- Timezone-aware timestamps with server-side defaults
- GIST spatial indexes on geometry columns
"""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, Enum, Float, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from worker.models.enums import SpatialLayerType


class Base(DeclarativeBase):
    """Base class for all database models."""



class CoefficientLayer(Base):
    """Dedicated model for coefficient polygons (5.4M records).

    This table stores the nutrient coefficient data used for land use calculations.
    Separated from SpatialLayer for optimal query performance on large datasets.

    Attributes:
        id: UUID primary key
        version: Version number for this layer (default: 1, incrementing)
        geometry: PostGIS geometry (POLYGON, SRID 27700)
        crome_id: CROME polygon identifier
        land_use_cat: Land use category
        nn_catchment: Nutrient Neutrality catchment name
        subcatchment: WFD subcatchment name
        lu_curr_n_coeff: Current land use nitrogen coefficient (kg/ha/yr)
        lu_curr_p_coeff: Current land use phosphorus coefficient (kg/ha/yr)
        n_resi_coeff: Residential nitrogen coefficient (kg/ha/yr)
        p_resi_coeff: Residential phosphorus coefficient (kg/ha/yr)
        created_at: Timestamp when record was created (server-side default)

    Note:
        Versioning approach: We don't update records in place. Instead, we add new
        versions with incremented version numbers. This is why there's no updated_at
        column - each version is immutable after creation.
    """

    __tablename__ = "coefficient_layer"
    __table_args__ = {"schema": "nrf_reference"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    # Geometry column (SRID 27700 = British National Grid)
    # Using MULTIPOLYGON to support both single and multiI part polygons
    geometry: Mapped[Any] = mapped_column(
        Geometry(geometry_type="MULTIPOLYGON", srid=27700, spatial_index=True),
        nullable=False,
    )

    # Coefficient-specific columns (snake_case)
    crome_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    land_use_cat: Mapped[str | None] = mapped_column(String, nullable=True)
    nn_catchment: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    subcatchment: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # Nutrient coefficients
    lu_curr_n_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)
    lu_curr_p_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)
    n_resi_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)
    p_resi_coeff: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return f"<CoefficientLayer(id={self.id}, crome_id={self.crome_id})>"


class SpatialLayer(Base):
    """Unified model for supporting spatial data (catchments, boundaries).

    This table stores smaller spatial reference datasets (WwTW catchments,
    LPA boundaries, NN catchments, subcatchments) using a discriminator column.

    Attributes:
        id: UUID primary key
        layer_type: Discriminator identifying the type of spatial data
        version: Version number for this layer (default: 1, incrementing)
        geometry: PostGIS geometry (flexible type, SRID 27700)
        name: Optional name/identifier for this feature
        attributes: JSONB storage for all source attributes (flexible schema per layer type)
        created_at: Timestamp when record was created (server-side default)

    Note:
        Versioning approach: We don't update records in place. Instead, we add new
        versions with incremented version numbers. This is why there's no updated_at
        column - each version is immutable after creation.

        Attributes column stores layer-specific fields in JSONB format:
        - WWTW catchments: WwTw_ID, etc.
        - LPA boundaries: NAME, etc.
        - NN catchments: N2K_Site_N, etc.
        - Subcatchments: OPCAT_NAME, etc.
    """

    __tablename__ = "spatial_layer"
    __table_args__ = {"schema": "nrf_reference"}

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    layer_type: Mapped[SpatialLayerType] = mapped_column(
        Enum(SpatialLayerType, name="spatial_layer_type", schema="nrf_reference"),
        nullable=False,
        index=True,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    # Geometry column (SRID 27700 = British National Grid)
    # Using flexible GEOMETRY type since different layers may have different geometry types
    geometry: Mapped[Any] = mapped_column(
        Geometry(geometry_type="GEOMETRY", srid=27700, spatial_index=True),
        nullable=False,
    )

    name: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    # JSONB storage for all source attributes (flexible per layer type)
    attributes: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return f"<SpatialLayer(id={self.id}, layer_type={self.layer_type}, name={self.name})>"


class LookupTable(Base):
    """JSONB-based storage for lookup tables (WwTW, rates).

    This table stores entire lookup tables as JSONB arrays, allowing flexible
    schema evolution and matching the legacy pattern of loading full tables
    into pandas DataFrames for in-memory joins.

    Attributes:
        id: UUID primary key
        name: Identifier for the lookup table (e.g., "wwtw_lookup", "rates_lookup")
        version: Version number for this lookup table (default: 1, incrementing)
        data: JSONB array containing all rows for this lookup table
        schema: Optional JSONB object defining column types for validation
        description: Optional description of this lookup table
        source: Optional data source/provider
        license: Optional license/attribution information
        created_at: Timestamp when record was created (server-side default)

    Note:
        Versioning approach: We don't update records in place. Instead, we add new
        versions with incremented version numbers. This is why there's no updated_at
        column - each version is immutable after creation.
    """

    __tablename__ = "lookup_table"
    __table_args__ = (
        UniqueConstraint("name", "version", name="uq_lookup_name_version"),
        {"schema": "nrf_reference"},
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, index=True)

    # JSONB array storing all rows for this lookup table
    data: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)

    # Optional schema definition for validation
    schema: Mapped[dict[str, str] | None] = mapped_column(JSONB, nullable=True)

    description: Mapped[str | None] = mapped_column(String, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    license: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    def __repr__(self) -> str:
        return f"<LookupTable(id={self.id}, name={self.name}, rows={len(self.data)})>"
