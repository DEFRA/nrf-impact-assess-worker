# Architecture

**Status**: ✅ Core infrastructure complete and tested (as of 2025-11-14)

## Overview

This document defines the architecture for impact assessments in the NRF worker. The key principle is:

> **Assessment scripts own their domain logic. The platform provides data access and handles I/O.**

This approach gives data scientists autonomy to implement their full workflow and calculation logic using familiar tools, while the platform provides a robust and performant foundation for data access and orchestration.

## Design principles

1.  **Simple Contract**: Assessment scripts have a clear, minimal interface: `run(rlb, metadata, repo) -> dict[str, DataFrame]`.
2.  **Data Scientist Autonomy**: Assessment authors control their full workflow and calculation logic.
3.  **Reuse Where Helpful**: Common geospatial utilities are available in `worker.spatial`, but their use is not mandatory.
4.  **Separation of Concerns**: Assessment logic is separate from data persistence and output handling.
5.  **No Unnecessary Abstraction**: The architecture avoids forcing common patterns where domain workflows differ significantly.
6.  **Generic Repository**: Data access is handled through a generic, parameterized repository, not domain-specific methods.

## System architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Worker main entry point                  │
│  (Handles: DB connections, S3, file I/O, job orchestration)     │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │   Assessment Runner   │
                    │  (worker/runner)      │
                    └───────────┬───────────┘
                                │
                    ┌───────────┴────────────┐
                    │                        │
        ┌───────────▼──────────┐  ┌─────────▼──────────┐
        │  Assessment Script   │  │  Assessment Script  │
        │  assessments/gcn.py  │  │assessments/nutrient.│
        │                      │  │        py           │
        │ - Uses Repository    │  │ - Uses Repository   │
        │ - Returns DataFrames │  │ - Returns DataFrames│
        └──────────┬───────────┘  └──────────┬──────────┘
                   │                         │
        ┌──────────▼──────────┐   ┌─────────▼──────────┐
        │   GCN Adapter       │   │  Nutrient Adapter  │
        │ adapters/gcn_adapter│   │adapters/nutrient_  │
        │         .py         │   │    adapter.py      │
        │                     │   │                    │
        │ DataFrame → Pydantic│   │DataFrame → Pydantic│
        └─────────────────────┘   └────────────────────┘
                   │                         │
                   └────────────┬────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   Domain Models       │
                    │  (Pydantic objects)   │
                    └───────────┬───────────┘
                                │
                    ┌───────────▼───────────┐
                    │  Persistence Layer    │
                    │ - Database            │
                    │ - File outputs (.psv) │
                    │ - S3 uploads          │
                    │ - Email notifications │
                    └───────────────────────┘
```

## Data flow

1.  **Platform Receives Job**: Loads the Red Line Boundary (RLB), validates it, and creates a `Repository` instance.
2.  **Platform Calls Runner**: The runner loads the appropriate assessment module (e.g., `assessments.gcn`).
3.  **Assessment Executes**: The script calls `assessment.run(rlb, metadata, repository)`.
    *   It uses the repository to load necessary spatial layers and lookup tables.
    *   It performs all domain-specific calculations.
    *   It returns a dictionary of `pandas` or `geopandas` DataFrames.
4.  **Platform Converts Results**: The runner loads the corresponding adapter (e.g., `adapters.gcn_adapter`) to convert the DataFrames into Pydantic domain models.
5.  **Platform Persists Results**: The final models are saved to the database, and file outputs (e.g., PSV) are generated and uploaded.

## Data backend: PostGIS

Reference data is stored in a PostgreSQL database with the PostGIS extension, which enables high-performance spatial queries.

### Database schema

The schema consists of three main tables in the `nrf_reference` schema:

1.  **`coefficient_layer`**: A dedicated, optimized table for the 5.4 million coefficient polygons. It uses explicit columns for coefficients for faster querying than JSONB.
2.  **`spatial_layer`**: A unified table for all other supporting spatial data (e.g., WwTW catchments, LPA boundaries). It uses a PostgreSQL `ENUM` (`spatial_layer_type`) to discriminate between layer types.
3.  **`lookup_table`**: A flexible table that stores non-spatial lookup data (e.g., WwTW permits, occupancy rates) as a JSONB column. This allows lookup schemas to evolve without database migrations.

All tables include an integer `version` column to support data provenance and reproducibility.

### Repository pattern

The `Repository` class (`worker/repositories/repository.py`) provides a clean, generic interface for accessing the PostGIS data, abstracting away the underlying SQLAlchemy and GeoAlchemy2 implementation.

**Key Methods:**

```python
class Repository:
    """Generic repository for accessing spatial and lookup data."""

    def load_spatial_layer(
        self,
        layer_type: SpatialLayerType,
        bbox: tuple | None = None,
        version: str | None = None
    ) -> gpd.GeoDataFrame:
        """Load a spatial layer by type, with optional bbox filtering."""
        pass

    def load_lookup(
        self,
        lookup_name: str,
        version: int | None = None
    ) -> pd.DataFrame:
        """Load a lookup table by name."""
        pass

    def load_coefficient_layer(
        self,
        bbox: tuple | None = None,
        version: int | None = None
    ) -> gpd.GeoDataFrame:
        """Load the large coefficient layer, with optional bbox filtering."""
        pass
```

This pattern allows assessment authors to request data without writing SQL or knowing the specific table structures.

## Assessment contract

Every assessment is a Python module containing a `run()` function with the following signature:

```python
def run(
    rlb_gdf: gpd.GeoDataFrame,
    metadata: dict,
    repository: Repository
) -> dict[str, gpd.GeoDataFrame | pd.DataFrame]:
    """
    Run the impact assessment.

    Args:
        rlb_gdf: Red Line Boundary GeoDataFrame (validated and in BNG CRS).
        metadata: Assessment metadata (e.g., unique_ref).
        repository: Repository instance for loading reference data.

    Returns:
        A dictionary of result DataFrames, keyed by a descriptive name.
    """
```

The assessment script is responsible for all domain logic. It should not write files or have other side effects; it simply returns data.

## Example: Nutrient assessment

The following shows a simplified version of how the nutrient assessment implements the contract.

```python
# worker/assessments/nutrient.py
import geopandas as gpd
import pandas as pd
from worker.repositories import Repository
from worker.models.enums import SpatialLayerType

def run(
    rlb_gdf: gpd.GeoDataFrame,
    metadata: dict,
    repository: Repository
) -> dict[str, pd.DataFrame]:
    """Run nutrient mitigation assessment."""
    # 1. Load spatial layers using the repository
    wwtw_catchments = repository.load_spatial_layer(SpatialLayerType.WWTW_CATCHMENTS)
    # ... load other layers

    # 2. Load lookup tables
    wwtw_lookup = repository.load_lookup("wwtw_lookup")
    rates_lookup = repository.load_lookup("rates_lookup")

    # 3. Load large coefficient layer with performance-critical bbox filtering
    coefficient_layer = repository.load_coefficient_layer(bbox=rlb_gdf.total_bounds)

    # 4. Perform domain-specific calculations...
    # (Spatial assignments, land use impacts, wastewater impacts, etc.)
    results = _calculate_land_use_impacts(rlb_gdf, coefficient_layer, ...)
    results = _calculate_wastewater_impacts(results, wwtw_lookup, ...)

    # 5. Return the final DataFrame in a dictionary
    return {
        "impact_summary": results.drop(columns=["geometry"])
    }

# Helper functions for calculations...
def _calculate_land_use_impacts(...): ...
def _calculate_wastewater_impacts(...): ...
```

## Future enhancements

-   **Bbox Pre-filtering**: While the repository interface supports it, bbox filtering is not yet implemented for all large layers. This is a possible/suggested optimisation that could help for certain queries.
