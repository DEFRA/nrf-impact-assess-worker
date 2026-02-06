# Repository design and interface

This document describes the design of the repository pattern used in the worker and provides a guide on how to use it to query reference data from the PostGIS database.

## Design philosophy

The repository provides a clean, generic interface for accessing PostGIS data, abstracting away the underlying SQLAlchemy and GeoAlchemy2 implementation details. The key principle is:

> **The repository is a generic query executor. Assessment scripts own the query logic.**

This means the repository does not contain domain-specific methods like `load_gcn_pobunds()` or `load_nutrient_lookup()`. Instead, it provides a single, flexible method (`execute_query`) that accepts a standard SQLAlchemy `SELECT` statement.

This approach has several benefits:
-   **Data Scientist autonomy**: Assessment authors have full control to build complex queries without needing to modify the repository.
-   **Simplicity**: The repository remains small, generic, and easy to maintain.
-   **Flexibility**: Any query that can be expressed in SQLAlchemy can be executed.

## The `Repository` class

**Module**: `worker.repositories.repository.py`

The `Repository` class is initialized with a SQLAlchemy engine and provides one primary method for data access.

### `execute_query()`

This is the main method for retrieving data.

**Signature:**
```python
def execute_query(self, stmt: Select, as_gdf: bool = False) -> gpd.GeoDataFrame | list[Base]:
```

**Parameters:**
-   `stmt`: A SQLAlchemy `SELECT` statement object.
-   `as_gdf`: A boolean that controls the return type.
    -   If `True`, the query result is returned as a `geopandas.GeoDataFrame`. This is used for all spatial queries. The query must include a geometry column.
    -   If `False` (default), the result is returned as a list of SQLAlchemy ORM objects (e.g., a list of `LookupTable` instances). This is used for non-spatial lookup data.

## How to query data

All queries follow the same pattern:
1.  Import the required database models (e.g., `SpatialLayer`, `LookupTable`) from `worker.models.db`.
2.  Import `select` from `sqlalchemy`.
3.  Build a `select()` statement.
4.  Call `repository.execute_query()` with the statement.
5.  Process the results (either a GeoDataFrame or a list of objects).

### Example 1: Querying a spatial layer

This example shows how to load the GCN Risk Zones spatial layer as a GeoDataFrame. This is a common pattern in most assessments.

```python
# In an assessment's run() method...
from sqlalchemy import select
from worker.models.db import SpatialLayer
from worker.models.enums import SpatialLayerType

# 1. Build the SELECT statement
stmt = select(SpatialLayer).where(
    SpatialLayer.layer_type == SpatialLayerType.GCN_RISK_ZONES
)

# 2. Execute the query, returning a GeoDataFrame
risk_zones_gdf = repository.execute_query(stmt, as_gdf=True)

# 3. The result is ready for use in spatial analysis
print(f"Loaded {len(risk_zones_gdf)} risk zone features")
```

### Example 2: Querying a non-spatial lookup table

This example shows how to load the `rates_lookup` table. Since this is non-spatial data, we request ORM objects (`as_gdf=False`) and convert them to a pandas DataFrame manually.

```python
# In an assessment's run() method...
import pandas as pd
from sqlalchemy import select
from worker.models.db import LookupTable

# 1. Build the SELECT statement to get the latest version
stmt = (
    select(LookupTable)
    .where(LookupTable.name == "rates_lookup")
    .order_by(LookupTable.version.desc())
    .limit(1)
)

# 2. Execute the query, returning a list of ORM objects
result_objects = repository.execute_query(stmt, as_gdf=False)
rates_lookup_obj = result_objects[0]

# 3. Convert the .data attribute to a pandas DataFrame
rates_lookup_df = pd.DataFrame(rates_lookup_obj.data)

# 4. The result is ready for use
rlb_gdf = rlb_gdf.merge(rates_lookup_df, on="nn_catchment", how="left")
```

### Example 3: Querying large layers

This example from the nutrient assessment shows how to query the dedicated `CoefficientLayer` table. The pattern is the same as a standard spatial query.
```python
# In an assessment's run() method...
from sqlalchemy import func, select
from worker.models.db import CoefficientLayer

# 1. Build the SELECT statement for the latest version
stmt = select(CoefficientLayer).where(
    CoefficientLayer.version == select(func.max(CoefficientLayer.version)).scalar_subquery()
)

# 2. Execute the query to get a GeoDataFrame
crome_gdf = repository.execute_query(stmt, as_gdf=True)

# 3. Intersect the loaded data with the area of interest
land_use_intersections = gpd.overlay(rlb_gdf, crome_gdf, how="intersection")
```

**Note**: We could look for opportunities to filter by bounding box before loading very large layer. This could result in
performance gains.


## Best practices

1.  **Be specific in your queries**: Always use a `where()` clause to filter by `layer_type` for `SpatialLayer` or `name` for `LookupTable`.
2.  **Handle versions**: For data that changes over time, always select the latest version using `order_by(Model.version.desc()).limit(1)` or by querying for a specific version number.
3.  **Use `as_gdf=True` for spatial data**: This is the most efficient way to load data for use with GeoPandas.
4.  **Use `as_gdf=False` for lookups**: This avoids the overhead of spatial processing for non-spatial data.
5.  **Keep query logic in the assessment**: The assessment module is the correct place to define the queries it needs. Do not add query logic to the repository.
