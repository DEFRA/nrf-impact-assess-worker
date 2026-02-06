# Data inventory

This document catalogs all data inputs used by the NRF Impact Assessment Worker.

> **Warning:** Reference data must be downloaded before running the worker locally. The `iat_input/` directory is git-ignored and must be populated manually from the NRF shared drive in the AD3 AWS account. See [Obtaining reference data](#obtaining-reference-data) for details.

## Overview

The system supports two assessment types:

1. **Nutrient Mitigation Scheme (NMS)** - Calculates nitrogen/phosphorus impact from housing developments
2. **Great Crested Newt (GCN) Assessment** - Assesses habitat/pond impact on protected species

---

## User-Submitted Inputs (Per Assessment)

| Input | Source | Assessment | Role |
|-------|--------|------------|------|
| **Red Line Boundary (RLB)** | Geometry only | Both | Development site boundary - the area being assessed |
| **Development Metadata** | SQS message / service callback | Nutrients | Dwelling count, development details for wastewater calculation |
| **Survey Ponds** (optional) | Geometry + attributes | GCN | Applicant-surveyed pond locations with GCN presence/absence status |

### Red line boundary

The RLB is treated as geometry-only. Development metadata (dwelling counts, categories, etc.) is provided separately via:
- SQS message payload triggering the assessment
- Callback to an external service containing user-submitted data

This separation allows the geometry to come from various sources (uploaded shapefile, drawn on map, etc.) while metadata flows through the application layer.

### Development metadata (Nutrients)

Provided via message/service, not in the RLB geometry:
- `dwellings` - Number of dwellings proposed
- `development_id` - Unique identifier for the development
- Additional fields as required by the nutrient calculation

### Survey ponds (GCN)

Optional applicant-provided pond survey data with required attribute fields:
- `PANS` - Presence/Absence/Not Surveyed status: `P` (Present), `A` (Absent), `NS` (Not Surveyed)
- `TmpImp` - Temporary Impact flag: `T` (True) or `F` (False)

---

## Reference data - Nutrient mitigation

| Dataset | Format | Features | Role |
|---------|--------|----------|------|
| **Coefficient Layer** | GeoPackage | 5.4M polygons | Pre-computed N/P runoff coefficients per land parcel |
| **WwTW Catchments** | Shapefile | Polygons | Wastewater treatment works catchment boundaries |
| **LPA Boundaries** | Shapefile | Polygons | Local Planning Authority boundaries |
| **NN Catchments** | Shapefile | 34 polygons | Nutrient Neutrality catchment boundaries (Natura 2000 sites) |
| **Subcatchments** | Shapefile | Polygons | WFD operational catchments |

### Coefficient layer

The coefficient layer is the core dataset for land-use nutrient calculations. Each polygon represents a land parcel with pre-computed coefficients.

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `cromeid` | String | Unique parcel identifier |
| `Land_use_cat` | String | Land use category |
| `NN_Catchment` | String | Nutrient Neutrality catchment name |
| `SubCatchment` | String | WFD subcatchment name |
| `LU_CurrNcoeff` | Numeric | Current land-use nitrogen coefficient (kg/ha/yr) |
| `LU_CurrPcoeff` | Numeric | Current land-use phosphorus coefficient (kg/ha/yr) |
| `N_ResiCoeff` | Numeric | Residential nitrogen coefficient (kg/ha/yr) |
| `P_ResiCoeff` | Numeric | Residential phosphorus coefficient (kg/ha/yr) |

### WwTW catchments

Mapped catchment areas for wastewater treatment works.

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `WwTw_ID` | Integer | Unique WwTW identifier (links to lookup table) |
| `WwTW` | String | WwTW name |

### NN catchments

Nutrient Neutrality catchment boundaries defining in-scope areas for assessment.

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `N2K_Site_N` | String | Natura 2000 site name |
| `SSSIname` | String | SSSI name |
| `Shape_Area` | Numeric | Catchment area |

### Subcatchments

WFD Surface Water Operational Catchments for finer-grained attribution.

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `OPCAT_NAME` | String | Operational catchment name |

---

## Reference Data - GCN Assessment

| Dataset | Format | Role |
|---------|--------|------|
| **Risk Zones** | FileGDB | National GCN risk zone classification |
| **National Ponds** | FileGDB | National pond dataset (fallback when no survey) |
| **EDP Edges** | FileGDB | District Licensing scheme boundaries |

### Risk zones

National GCN habitat suitability zones. Layer: `GCN_RZ_NRF_Final`

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `RZ` | String | Risk zone: `Red`, `Amber`, or `Green` |
| `MHW` | String | Mean High Water indicator |

### National ponds

National pond dataset used when applicant does not provide survey data. Layer: `NRF_Ponds`

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `Pond_ID` | String | Unique pond identifier |
| `PondArea` | Integer | Pond area |
| `CoreFringe` | String | Core or fringe classification |
| `Pond_Status` | Integer | Pond status code |

### EDP edges

European District Licensing Partner boundaries for splitting results. Layer: `EDP_Edge`

**Key Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `EDPName` | String | Delivery partner name |

---

## Lookup Tables

Stored in SQLite database, loaded into PostGIS as JSONB.

### WwTW lookup

Wastewater treatment works permit limits for nutrient discharge calculations.

| Field | Type | Description |
|-------|------|-------------|
| `WwTW_code` | Integer | WwTW identifier (links to spatial layer) |
| `WwTW_name` | String | WwTW name |
| `Subcatchment_name` | String | Associated subcatchment |
| `Nitrogen_2025_2030` | Numeric | N permit limit 2025-2030 (mg/L) |
| `Nitrogen_2030_onwards` | Numeric | N permit limit post-2030 (mg/L) |
| `Phosphorus_2025_2030` | Numeric | P permit limit 2025-2030 (mg/L) |
| `Phosphorus_2030_onwards` | Numeric | P permit limit post-2030 (mg/L) |

### Rates lookup

Occupancy and water usage rates by catchment.

| Field | Type | Description |
|-------|------|-------------|
| `NN_Catchment` | String | Catchment name (join key) |
| `Occ_Rate` | Numeric | Average occupancy rate (persons/dwelling) |
| `Water_Usage_L_Day` | Integer | Water usage (litres/person/day) |

---

## Processing Flow

### Nutrient assessment

```
RLB
 |
 +---> intersect Coefficient Layer ---> land-use N/P uplift (kg/yr)
 |
 +---> majority-intersect WwTW Catchments ---> WwTW Lookup ---> wastewater N/P load
 |
 +---> intersect NN Catchments ---> determine in-scope area
 |
 v
Total N/P (kg/yr) = Land-use uplift + Wastewater load + Precautionary buffer (20%)
```

### GCN assessment

```
RLB
 |
 +---> 250m buffer ---> clip Risk Zones ---> habitat impact by zone (m²)
 |
 +---> select Ponds (survey or national) ---> 250m pond buffer
 |
 v
Habitat within pond buffer ---> intersect EDP Edges ---> impact by delivery partner

Output:
- Habitat area (m²) by zone and location (RLB vs buffer)
- Pond counts by zone, presence status, and delivery partner
```

---

## File locations

Reference data is loaded into PostGIS from files in the `iat_input/` directory. This directory is git-ignored and must be populated manually.

### Obtaining reference data

Reference data is available from the NRF shared drive in the AD3 AWS account. Contact the team for access details.

### Expected directory structure

```
iat_input/
├── nutrients/
│   ├── NMSCoefficientLayerTEST.gpkg    # Coefficient layer
│   ├── wwtw files/
│   │   └── WwTW_all_features.shp       # WwTW catchments
│   ├── LPA/
│   │   └── LPA_National.shp            # LPA boundaries
│   ├── Catchments/
│   │   ├── NN_Catchments_03_2024.shp   # NN catchments
│   │   └── WFD_Surface_Water_...shp    # Subcatchments
│   └── SQL_Lookups/
│       └── Interim_coeffs.sqlite       # Lookup tables
│
└── gcn/
    ├── RZ.gdb                          # Risk zones (GCN_RZ_NRF_Final layer)
    └── IIAT_Layers.gdb                 # NRF_Ponds, EDP_Edge layers
```

**Note**: Sample RLB files for testing are committed to `tests/data/inputs/` and are not stored in `iat_input/`. See `tests/data/README.md` for details.

### Loading data

Once files are in place, load them into PostGIS using:

```bash
uv run python scripts/load_data.py
```

See [Local Development Guide](local-development.md) for full setup instructions.
