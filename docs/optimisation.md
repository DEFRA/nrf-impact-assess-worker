# Assessment optimisations

This document describes the spatial filtering optimisations applied to the impact assessments.

---

## Nutrient assessment

### Problem

The original implementation loaded **all 5.4M coefficient polygons** on every assessment, regardless of input size. This took ~90 seconds and dominated execution time.
It's true that this cost can be amortized in a long running process by loading at startup and holding this layer in memory for reuse accross many assessments, the approach limits options if we want to explore other ways of handling these types of referernce data sets.

For example it may be the case when there are more assessment we could move to a model of tasks that are activated by external events, e.g. message on queue, file in bucket. Then the tasks can then ber instantiated, run and when finished are shut down.

If we move to more efficient handling of large layers it opens up other approaches.

### Solution: NN catchment spatial filtering

The coefficient layer is only used where it intersects with both the RLB and the NN catchment. Understanding the data flow:

1. **RLB → WwTW catchment**: RLB is assigned to a WwTW catchment via `majority_overlap`
2. **RLB ∩ Coefficient Layer**: RLB intersects with coefficient polygons
3. **Result ∩ NN Catchment**: The intersection is filtered by NN catchment boundaries

The NN catchment geometry defines where land use impacts apply - only coefficient polygons within NN catchments matter. The WwTW catchment determines wastewater treatment routing (a separate calculation).

#### Implementation

1. Load NN catchments first
2. Find which NN catchments intersect with the RLBs
3. Use those NN catchment geometries to spatially filter the coefficient layer via PostGIS `ST_Intersects`
4. Early return when no RLBs intersect NN catchments (no land use impacts possible)

This reduces data loaded from ~5.4M to ~5k-50k polygons depending on NN catchment size.

### Benchmark results

| Test                             | Baseline | Optimised | Speedup   |
|----------------------------------|----------|-----------|-----------|
| Single site (shapefile)          | 91.20s   | 12.93s    | **7.1x**  |
| Single site (geojson)            | 88.60s   | 12.93s    | **6.9x**  |
| Full Broads & Wensum (245 sites) | 91.49s   | 21.10s    | **4.3x**  |

**Key observation**: The baseline timing is nearly identical (~90s) regardless of input size (1 site vs 245 sites). This confirms that coefficient layer loading dominated execution time.

The optimised version shows input-dependent timing (13s for single site, 21s for 245 sites) - processing time now scales with the amount of work being done.

---

## GCN assessment

### Problem

The original implementation loaded **all 457k national pond features** on every assessment, regardless of input size. This took ~20 seconds and dominated execution time for the no-survey route.

| Layer | Features | Load Time |
|-------|----------|-----------|
| national_ponds | 457,161 | 20.4s |
| risk_zones | 14,303 | 6.3s |

### Solution: RLB+buffer spatial filtering

For the no-survey route (using national ponds dataset), ponds are only relevant if they fall within or near the RLB. The assessment already creates a 250m buffer around the RLB for analysis.

#### Implementation

1. Prepare RLB and create 250m buffer first
2. Create combined extent geometry (RLB + buffer)
3. Use that extent to spatially filter national ponds via PostGIS `ST_Intersects`
4. Survey route unchanged (loads user-provided pond file)

This reduces data loaded from ~457k to typically <100 ponds for single-site assessments.

### Benchmark results

| Test | Baseline | Optimised | Speedup |
|------|----------|-----------|---------|
| No-survey route | 45.20s | 6.60s | **6.8x** |
| Survey route | 26.92s | 6.50s | **4.1x** |
| **Total (both tests)** | **72.15s** | **13.10s** | **5.5x** |

**Key observation**: The survey route also benefits because risk_zones loading (6.3s) is no longer blocked behind the slow national_ponds load. Both routes now complete in ~6.5s.
