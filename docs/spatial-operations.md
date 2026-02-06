# Spatial operations reference

This document describes the spatial utilities available in the `worker.spatial` package. These utilities provide common geospatial operations used across environmental impact assessments, replacing legacy ArcPy functionality with open-source GeoPandas/Shapely implementations.

## Package structure

```
worker/spatial/
├── utils.py          # CRS handling and precision models
├── operations.py     # Clipping, intersection, geometry validation
├── assignments.py    # Spatial attribute assignment strategies
└── overlay.py        # Buffer and difference operations
```

## Table of contents

- [CRS and Precision Utilities](#crs-and-precision-utilities)
- [Basic Spatial Operations](#basic-spatial-operations)
- [Spatial Assignment Strategies](#spatial-assignment-strategies)
- [Overlay Operations](#overlay-operations)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)

---

## CRS and precision utilities

**Module**: `worker.spatial.utils`

### `ensure_crs()`

Ensure GeoDataFrame is in the target coordinate reference system, transforming if necessary.

**Signature:**
```python
def ensure_crs(
    gdf: gpd.GeoDataFrame,
    target_crs: str = "EPSG:27700"
) -> gpd.GeoDataFrame
```

**Parameters:**
- `gdf`: Input GeoDataFrame
- `target_crs`: Target CRS (default: EPSG:27700 / British National Grid)

**Returns:**
- GeoDataFrame in target CRS (transformed if necessary, original if already correct)

**Raises:**
- `ValueError`: If input GeoDataFrame has no CRS defined

**Example:**
```python
from worker.spatial.utils import ensure_crs

# Ensure RLB is in British National Grid
rlb_bng = ensure_crs(rlb_gdf)

# Transform to custom CRS
rlb_wgs84 = ensure_crs(rlb_gdf, target_crs="EPSG:4326")
```

---

### `apply_precision()`

Apply precision model to geometries to match ArcGIS XY tolerance behavior. Snaps geometry coordinates to a grid to reduce numerical differences between GEOS (GeoPandas/Shapely) and ArcGIS geometry engines.

**Signature:**
```python
def apply_precision(
    gdf: gpd.GeoDataFrame,
    grid_size: float = 0.0001
) -> gpd.GeoDataFrame
```

**Parameters:**
- `gdf`: Input GeoDataFrame
- `grid_size`: Grid size in metres (default: 0.0001m = 0.1mm, matches ArcGIS)

**Returns:**
- GeoDataFrame with precision-snapped geometries

**Notes:**
- This operation may slightly modify geometry coordinates and areas
- Use same `grid_size` value consistently across all operations in a workflow
- Critical for achieving consistent results when comparing outputs against legacy ArcGIS scripts

**When to use:**
- Before/after overlay operations (intersection, union, difference)
- When validating outputs against legacy ArcGIS baselines
- To reduce floating-point accumulation errors in complex spatial workflows

**Example:**
```python
from worker.spatial.utils import apply_precision

# Apply default 0.1mm precision (matches ArcGIS)
precise_gdf = apply_precision(gdf)

# Custom precision for different use case
coarse_gdf = apply_precision(gdf, grid_size=0.001)  # 1mm
```

**Reference:** [ArcGIS Precision Documentation](https://pro.arcgis.com/en/pro-app/latest/help/editing/pdf/parcel_fabric_precision.pdf)

---

## Basic spatial operations

**Module**: `worker.spatial.operations`

### `clip_gdf()`

Clip a GeoDataFrame to the extent of a mask GeoDataFrame. Keeps only portions that fall within the mask extent.

**Signature:**
```python
def clip_gdf(
    gdf: gpd.GeoDataFrame,
    mask: gpd.GeoDataFrame
) -> gpd.GeoDataFrame
```

**Parameters:**
- `gdf`: Input GeoDataFrame to clip
- `mask`: Mask GeoDataFrame defining clip extent

**Returns:**
- Clipped GeoDataFrame (may have fewer rows than input)

**Example:**
```python
from worker.spatial.operations import clip_gdf

# Clip risk zones to RLB extent
risk_zones_clipped = clip_gdf(risk_zones_gdf, rlb_gdf)
```

---

### `spatial_join_intersect()`

Spatial intersection (overlay) operation with precision control. Performs a geometric intersection, creating new geometries from overlapping areas. Applies precision model before and after overlay to match ArcGIS behavior.

**Signature:**
```python
def spatial_join_intersect(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    grid_size: float = 0.0001
) -> gpd.GeoDataFrame
```

**Parameters:**
- `left`: Left GeoDataFrame
- `right`: Right GeoDataFrame
- `grid_size`: Grid size for precision model (default: 0.0001m)

**Returns:**
- GeoDataFrame with intersected geometries and attributes from both inputs

**Replaces:** ArcPy `PairwiseIntersect`

**Example:**
```python
from worker.spatial.operations import spatial_join_intersect

# Intersect RLB with risk zones to get habitat impact areas
habitat_impact = spatial_join_intersect(rlb_gdf, risk_zones_gdf)
habitat_impact["area_sqm"] = habitat_impact.geometry.area
```

---

### `make_valid_geometries()`

Repair invalid geometries using Shapely's `make_valid`. Fixes common geometry issues like self-intersections, unclosed rings, etc.

**Signature:**
```python
def make_valid_geometries(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame
```

**Parameters:**
- `gdf`: Input GeoDataFrame (may contain invalid geometries)

**Returns:**
- GeoDataFrame with repaired geometries

**When to use:**
- After loading user-submitted shapefiles/geojson
- Before performing spatial operations that require valid geometries
- When encountering GEOS topology exceptions

**Example:**
```python
from worker.spatial.operations import make_valid_geometries

# Repair RLB geometries before processing
rlb_valid = make_valid_geometries(rlb_gdf)
```

---

## Spatial assignment strategies

**Module**: `worker.spatial.assignments`

These functions assign attributes from overlay layers to input features based on spatial relationships.

### `majority_overlap()`

Assign overlay attribute based on largest spatial overlap. For each input feature, finds the overlay feature with the largest overlapping area.

**Signature:**
```python
def majority_overlap(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    default_value: Optional[Any] = None
) -> gpd.GeoDataFrame
```

**Parameters:**
- `input_gdf`: Input features (developments, sites, etc.)
- `overlay_gdf`: Overlay features (catchments, zones, etc.)
- `input_id_col`: ID column in input_gdf
- `overlay_attr_col`: Attribute column to assign from overlay_gdf
- `output_field`: Name of output field to create in input_gdf
- `default_value`: Value for features with no overlap (default: None)

**Returns:**
- `input_gdf` with new column containing assigned values

**Use case:** Assigning a development to a single catchment/zone based on which one it mostly overlaps

**Example:**
```python
from worker.spatial.assignments import majority_overlap

# Assign each development to its majority WwTW catchment
rlb_gdf = majority_overlap(
    input_gdf=rlb_gdf,
    overlay_gdf=wwtw_catchments_gdf,
    input_id_col="id",
    overlay_attr_col="WwTw_ID",
    output_field="majority_wwtw_id",
    default_value=141  # Fallback ID
)
```

---

### `any_intersection()`

Assign all intersecting overlay attributes as a list. For each input feature, collects all overlay features that intersect it.

**Signature:**
```python
def any_intersection(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str
) -> gpd.GeoDataFrame
```

**Parameters:**
- `input_gdf`: Input features
- `overlay_gdf`: Overlay features
- `input_id_col`: ID column in input_gdf
- `overlay_attr_col`: Attribute column to assign from overlay_gdf
- `output_field`: Name of output field to create in input_gdf

**Returns:**
- `input_gdf` with new column containing list of assigned values

**Use case:** When a development intersects multiple zones and you need all of them

**Example:**
```python
from worker.spatial.assignments import any_intersection

# Get all risk zones that intersect each pond
ponds_gdf = any_intersection(
    input_gdf=ponds_gdf,
    overlay_gdf=risk_zones_gdf,
    input_id_col="pond_id",
    overlay_attr_col="risk_zone",
    output_field="intersecting_zones"
)
# Result: ponds_gdf["intersecting_zones"] = ["Red", "Amber"] or ["Green"]
```

---

### `nearest()`

Assign nearest overlay feature attribute. For each input feature, finds the nearest overlay feature.

**Signature:**
```python
def nearest(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    max_distance: Optional[float] = None
) -> gpd.GeoDataFrame
```

**Parameters:**
- `input_gdf`: Input features
- `overlay_gdf`: Overlay features
- `input_id_col`: ID column in input_gdf
- `overlay_attr_col`: Attribute column to assign from overlay_gdf
- `output_field`: Name of output field to create in input_gdf
- `max_distance`: Maximum distance to search in metres (None = unlimited)

**Returns:**
- `input_gdf` with new column containing assigned values

**Use case:** When features don't overlap but you need to assign the closest one

**Example:**
```python
from worker.spatial.assignments import nearest

# Assign nearest treatment plant to developments outside catchments
rlb_gdf = nearest(
    input_gdf=rlb_gdf,
    overlay_gdf=treatment_plants_gdf,
    input_id_col="id",
    overlay_attr_col="plant_id",
    output_field="nearest_plant",
    max_distance=5000  # Only within 5km
)
```

---

### `intersection()`

Perform full spatial intersection overlay. Unlike assignment operations that add a column, this returns the full intersection overlay result with geometries split at boundaries.

**Signature:**
```python
def intersection(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    preserve_input_fields: bool = True
) -> gpd.GeoDataFrame
```

**Parameters:**
- `input_gdf`: Input features
- `overlay_gdf`: Overlay features
- `preserve_input_fields`: If True, preserve all input fields in result

**Returns:**
- Intersection overlay result (may have more rows than input)

**Use case:** When you need the actual intersection geometries (e.g., for area-weighted calculations)

**Example:**
```python
from worker.spatial.assignments import intersection

# Get intersection geometries for area calculations
overlays = intersection(rlb_gdf, coefficient_layer_gdf)
overlays["overlap_area_ha"] = overlays.geometry.area / 10000

# Calculate area-weighted coefficients
overlays["weighted_n"] = overlays["N_coeff"] * overlays["overlap_area_ha"]
weighted_totals = overlays.groupby("id")["weighted_n"].sum()
```

---

### `execute_assignment()`

Main entry point for executing any assignment strategy. Used by the assessment runner.

**Signature:**
```python
def execute_assignment(
    input_gdf: gpd.GeoDataFrame,
    overlay_gdf: gpd.GeoDataFrame,
    strategy: str,
    input_id_col: str,
    overlay_attr_col: str,
    output_field: str,
    **kwargs
) -> gpd.GeoDataFrame
```

**Parameters:**
- `input_gdf`: Input features
- `overlay_gdf`: Overlay features
- `strategy`: Strategy name (`'majority_overlap'`, `'any_intersection'`, `'nearest'`, `'intersection'`)
- `input_id_col`: ID column in input_gdf
- `overlay_attr_col`: Attribute column from overlay_gdf
- `output_field`: Name of output field
- `**kwargs`: Additional strategy-specific parameters

**Example:**
```python
from worker.spatial.assignments import execute_assignment

# Flexible assignment based on strategy parameter
result_gdf = execute_assignment(
    input_gdf=rlb_gdf,
    overlay_gdf=catchments_gdf,
    strategy="majority_overlap",
    input_id_col="id",
    overlay_attr_col="catchment_name",
    output_field="assigned_catchment",
    default_value="Unknown"  # Strategy-specific kwarg
)
```

---

## Overlay operations

**Module**: `worker.spatial.overlay`

Operations commonly used in GCN assessments and other workflows requiring buffer/difference operations.

### `buffer_with_dissolve()`

Buffer geometries with optional dissolve to single geometry. Applies precision snapping to match legacy ArcGIS behavior.

**Signature:**
```python
def buffer_with_dissolve(
    gdf: gpd.GeoDataFrame,
    distance_m: float,
    dissolve: bool = True,
    grid_size: float = 0.0001
) -> gpd.GeoDataFrame
```

**Parameters:**
- `gdf`: Input GeoDataFrame
- `distance_m`: Buffer distance in metres
- `dissolve`: If True, dissolve all buffered geometries into single geometry (default: True)
- `grid_size`: Grid size for precision model (default: 0.0001m)

**Returns:**
- GeoDataFrame with buffered geometries. If `dissolve=True`, returns single-row GeoDataFrame with dissolved geometry.

**Replaces:** ArcPy `PairwiseBuffer`

**Example:**
```python
from worker.spatial.overlay import buffer_with_dissolve

# Create 250m buffer around RLB (dissolved to single geometry)
rlb_buffered = buffer_with_dissolve(rlb_gdf, distance_m=250, dissolve=True)

# Create individual buffers (no dissolve)
ponds_buffered = buffer_with_dissolve(ponds_gdf, distance_m=250, dissolve=False)
```

---

### `spatial_difference_with_precision()`

Spatial difference (erase) with precision model applied. Erases areas of left GeoDataFrame that overlap with right GeoDataFrame.

**Signature:**
```python
def spatial_difference_with_precision(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
    grid_size: float = 0.0001
) -> gpd.GeoDataFrame
```

**Parameters:**
- `left`: GeoDataFrame to erase from
- `right`: GeoDataFrame defining erase areas
- `grid_size`: Grid size for precision model (default: 0.0001m)

**Returns:**
- Difference overlay result (left minus overlaps with right)

**Replaces:** ArcPy `PairwiseErase`

**Example:**
```python
from worker.spatial.overlay import spatial_difference_with_precision

# Select ponds NOT in RLB (inverted selection)
ponds_in_buffer = spatial_difference_with_precision(
    left=all_ponds_gdf,
    right=rlb_gdf
)
```

---
---

## Best practices

### 1. Always validate CRS

```python
from worker.spatial.utils import ensure_crs

# Good - explicit CRS check and transform
rlb_gdf = ensure_crs(rlb_gdf, target_crs="EPSG:27700")

# Bad - assume CRS is correct
# May cause incorrect results if input is in different CRS
```

### 2. Repair geometries early

```python
from worker.spatial.operations import make_valid_geometries

# Good - repair immediately after loading
rlb_gdf = gpd.read_file("input.shp")
rlb_gdf = make_valid_geometries(rlb_gdf)

# Bad - spatial operations may fail on invalid geometries
```

### 3. Use consistent precision

```python
from worker.spatial.utils import apply_precision

# Good - same grid_size throughout workflow
GRID_SIZE = 0.0001

gdf1 = apply_precision(gdf1, grid_size=GRID_SIZE)
gdf2 = apply_precision(gdf2, grid_size=GRID_SIZE)
result = spatial_join_intersect(gdf1, gdf2, grid_size=GRID_SIZE)

# Bad - mixing different precision values
gdf1 = apply_precision(gdf1, grid_size=0.0001)
gdf2 = apply_precision(gdf2, grid_size=0.001)  # Different!
```

### 4. Choose the right assignment strategy

| Scenario | Strategy | Reason |
|----------|----------|--------|
| Development must be in ONE catchment | `majority_overlap` | Assigns single value based on largest overlap |
| Development touches MULTIPLE zones | `any_intersection` | Returns list of all intersecting values |
| No overlap but need closest feature | `nearest` | Finds nearest feature within max_distance |
| Need intersection geometries for area calculations | `intersection` | Returns full overlay with split geometries |

### 5. Handle PostGIS UUID conflicts

```python
# The assignments module already handles this automatically
# But if manually using gpd.overlay(), be aware:

# Good - drop 'id' column before overlay
overlay_gdf_clean = overlay_gdf.drop(columns=["id"], errors="ignore")
result = gpd.overlay(input_gdf, overlay_gdf_clean, how="intersection")

# Bad - pandas will rename conflicting 'id' columns to 'id_1', 'id_2'
result = gpd.overlay(input_gdf, overlay_gdf, how="intersection")
```

### 6. Precision is critical for regression testing

When validating outputs against legacy ArcGIS scripts:

```python
from worker.spatial.utils import apply_precision

# ALWAYS use precision model when comparing against ArcGIS baselines
gdf = apply_precision(gdf, grid_size=0.0001)  # Matches ArcGIS XY Resolution

# This reduces numerical differences between GEOS and ArcGIS engines
# Critical for passing regression tests with tight tolerances
```

---

## Performance tips

1. **Clip large layers before operations**: Use `clip_gdf()` to reduce dataset size before expensive operations
2. **Use bbox filtering in repository**: For PostGIS queries, filter by bounding box before loading into memory
3. **Dissolve buffers**: Use `dissolve=True` in `buffer_with_dissolve()` to reduce geometry complexity
4. **Avoid repeated assignments**: If assigning multiple attributes from same overlay layer, do it in one pass with `intersection()` then aggregate

---

## Migration from ArcPy

| ArcPy Function | Replacement | Notes |
|----------------|-------------|-------|
| `arcpy.PairwiseBuffer()` | `buffer_with_dissolve()` | Includes precision snapping |
| `arcpy.PairwiseIntersect()` | `spatial_join_intersect()` | With precision control |
| `arcpy.PairwiseErase()` | `spatial_difference_with_precision()` | Applies precision model |
| `arcpy.Clip_analysis()` | `clip_gdf()` | Direct replacement |
| `arcpy.SpatialJoin()` | `majority_overlap()` or `any_intersection()` | Depends on join type |
| `arcpy.Near_analysis()` | `nearest()` | With optional max_distance |
| `arcpy.RepairGeometry()` | `make_valid_geometries()` | Uses Shapely make_valid |
| `arcpy.Project()` | `ensure_crs()` | Validates and transforms |

---

## Related documentation

- **Configuration**: See `configuration.md` for GCN precision settings
- **Repository**: See `docs/postgis-migration.md` for data loading patterns
- **Assessment Creation**: See `docs/creating-assessments.md` for using these utilities in assessments

---

## API reference

For complete function signatures and implementation details, see:
- `worker/spatial/utils.py`
- `worker/spatial/operations.py`
- `worker/spatial/assignments.py`
- `worker/spatial/overlay.py`
