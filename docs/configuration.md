# Configuration reference

> ⚠️ **Important Note for Production Deployment (CDP)**
>
> The configuration details described in this document, particularly for AWS resources (SQS, S3) and Database connections, are specific to this experimental repository and its local development environment.
>
> In a production CDP environment, these aspects are typically managed differently (e.g., via CDP's built-in service discovery, secrets management, and platform-specific environment variables).
>
> **Do not take the AWS and Database configuration patterns presented here as representative of how production services are configured in CDP.** Always refer to the official CDP documentation for production configuration best practices.

This document describes all configuration options available in the NRF Impact Assessment Worker. Configuration is managed via Pydantic settings classes that can be overridden using environment variables or `.env` files.

## Configuration philosophy

The worker separates configuration into three categories:

1. **Physical Constants** - Immutable conversion factors and CRS definitions (not configurable)
2. **Business Logic Configuration** - Assessment parameters that may vary by deployment or testing scenarios
3. **Infrastructure Configuration** - AWS resources, database connections, and worker settings

## Table of contents

- [Physical Constants](#physical-constants)
- [Assessment Configuration](#assessment-configuration)
  - [Nutrient Mitigation](#nutrient-mitigation-assessment)
  - [GCN Assessment](#gcn-great-crested-newt-assessment)
- [Database Configuration](#database-configuration)
- [AWS Configuration](#aws-configuration)
- [Worker Configuration](#worker-configuration)
- [Column Definitions](#column-definitions)
- [Usage Examples](#usage-examples)

---

## Physical constants

**Module**: `worker.config.CONSTANTS`
**Type**: `PhysicalConstants` (frozen dataclass)
**Configurable**: No

These constants represent fixed conversion factors and coordinate reference systems that should never vary:

| Constant | Value | Description |
|----------|-------|-------------|
| `CRS_BRITISH_NATIONAL_GRID` | `"EPSG:27700"` | British National Grid coordinate reference system |
| `DAYS_PER_YEAR` | `365.25` | Days per year (accounting for leap years) |
| `SQUARE_METRES_PER_HECTARE` | `10000.0` | Conversion factor: m² to hectares |
| `MILLIGRAMS_PER_KILOGRAM` | `1000000.0` | Conversion factor: mg to kg |

---

## Assessment configuration

### Nutrient mitigation assessment

**Class**: `AssessmentConfig`
**Environment Prefix**: `IAT_`
**Default Instance**: `DEFAULT_CONFIG`

Configuration for nutrient mitigation impact assessment calculations.

#### Configuration fields

| Field | Type | Default | Environment Variable | Description |
|-------|------|---------|---------------------|-------------|
| `precautionary_buffer_percent` | `float` | `20.0` | `IAT_PRECAUTIONARY_BUFFER_PERCENT` | Additional buffer applied to total nutrient impacts (%) |
| `fallback_wwtw_id` | `int` | `141` | `IAT_FALLBACK_WWTW_ID` | Default WwTW ID for developments outside modeled catchments |
| `suds` | `SuDsConfig` | See below | `SUDS_*` | SuDS mitigation configuration |

#### Computed properties

```python
@property
def precautionary_buffer_factor(self) -> float:
    """Returns buffer as decimal factor (e.g., 0.20 for 20%)"""
```

#### SuDS configuration

**Class**: `SuDsConfig`
**Environment Prefix**: `SUDS_`

Sustainable Drainage Systems (SuDS) mitigation parameters:

| Field | Type | Default | Environment Variable | Description |
|-------|------|---------|---------------------|-------------|
| `threshold_dwellings` | `int` | `50` | `SUDS_THRESHOLD_DWELLINGS` | Minimum dwellings to trigger SuDS requirements |
| `flow_capture_percent` | `float` | `100.0` | `SUDS_FLOW_CAPTURE_PERCENT` | Percentage of flow entering SuDS system |
| `removal_rate_percent` | `float` | `25.0` | `SUDS_REMOVAL_RATE_PERCENT` | Nutrient removal efficiency of SuDS (%) |

**Computed Properties:**
```python
@property
def total_reduction_factor(self) -> float:
    """Combined reduction factor (e.g., 0.25 for 100% capture × 25% removal)"""
```

---

### GCN (Great Crested Newt) assessment

**Class**: `GcnConfig`
**Environment Prefix**: `GCN_`
**Default Instance**: `DEFAULT_GCN_CONFIG`

Configuration for GCN (Great Crested Newt) habitat impact assessment.

#### Configuration fields

| Field | Type | Default | Environment Variable | Description |
|-------|------|---------|---------------------|-------------|
| `buffer_distance_m` | `int` | `250` | `GCN_BUFFER_DISTANCE_M` | Buffer distance around RLB (metres) |
| `pond_buffer_distance_m` | `int` | `250` | `GCN_POND_BUFFER_DISTANCE_M` | Buffer distance around ponds (metres) |
| `merge_distance_m` | `int` | `500` | `GCN_MERGE_DISTANCE_M` | Sites within this distance create single buffer (metres) |
| `precision_grid_size` | `float` | `0.0001` | `GCN_PRECISION_GRID_SIZE` | Coordinate precision grid size (metres, 0.1mm matches ArcGIS XY Resolution) |
| `target_crs` | `str` | `"EPSG:27700"` | `GCN_TARGET_CRS` | Target coordinate reference system (British National Grid) |

#### Validation

- All distance fields must be `>= 0`
- `precision_grid_size` must be `> 0`

```

---

## Database configuration

**Class**: `DatabaseSettings`
**Environment Prefix**: `DB_`

PostgreSQL/PostGIS database connection settings.

| Field | Type | Default | Environment Variable | Description |
|-------|------|---------|---------------------|-------------|
| `url` | `PostgresDsn` | `"postgresql://postgres@localhost:5432/nrf_impact"` | `DB_URL` | PostgreSQL connection URL |

#### Connection string format

```
postgresql://[user[:password]@]host[:port]/database
```

**Examples:**
- Local development: `postgresql://postgres@localhost:5432/nrf_impact`
- With authentication: `postgresql://user:password@db.example.com:5432/nrf_impact`
- AWS RDS: `postgresql://username:password@nrf-db.abc123.eu-west-2.rds.amazonaws.com:5432/nrf_impact`

```

---

## AWS configuration

**Class**: `AWSConfig`
**Environment Prefix**: `AWS_`

AWS resource identifiers for ECS worker deployment.

| Field | Type | Required | Environment Variable | Description |
|-------|------|----------|---------------------|-------------|
| `s3_input_bucket` | `str` | **Yes** | `AWS_S3_INPUT_BUCKET` | S3 bucket for input shapefiles/geojson |
| `sqs_queue_url` | `str` | **Yes** | `AWS_SQS_QUEUE_URL` | SQS queue URL for job messages |
| `region` | `str` | No | `AWS_REGION` | AWS region (default: `eu-west-2`) |

#### Validation

- `s3_input_bucket` and `sqs_queue_url` must not be empty strings
- All whitespace-only values are rejected

```

---

## Worker configuration

**Class**: `WorkerConfig`
**Environment Prefix**: `SQS_`

Worker polling and job processing settings.

| Field | Type | Default | Range | Environment Variable | Description |
|-------|------|---------|-------|---------------------|-------------|
| `wait_time_seconds` | `int` | `20` | 1-20 | `SQS_WAIT_TIME_SECONDS` | SQS long polling wait time (seconds) |
| `visibility_timeout` | `int` | `300` | 30-43200 | `SQS_VISIBILITY_TIMEOUT` | Job processing timeout (seconds) |
| `max_messages` | `int` | `1` | 1-10 | `SQS_MAX_MESSAGES` | Messages to receive per poll |
| `graceful_shutdown_timeout` | `int` | `30` | 0-300 | `SQS_GRACEFUL_SHUTDOWN_TIMEOUT` | Time to finish job on SIGTERM (seconds) |

#### Field descriptions

- **`wait_time_seconds`**: How long SQS long polling should wait for messages (reduces empty receives)
- **`visibility_timeout`**: How long a message is hidden from other workers while processing (should exceed max job duration)
- **`max_messages`**: Number of messages to receive per SQS call (worker currently processes one at a time)
- **`graceful_shutdown_timeout`**: Time allowed to finish current job when worker receives shutdown signal

```

---

## Column definitions

### RequiredColumns

**Class**: `RequiredColumns`

Required column names in input Red Line Boundary shapefile (normalized to snake_case).

| Constant | Value | Description |
|----------|-------|-------------|
| `ID` | `"id"` | Unique identifier |
| `NAME` | `"name"` | Development name |
| `DWELLING_CATEGORY` | `"dwelling_category"` | Dwelling type/category |
| `SOURCE` | `"source"` | Data source identifier |
| `DWELLINGS` | `"dwellings"` | Number of dwellings |
| `SHAPE_AREA` | `"shape_area"` | Area in square metres |
| `GEOMETRY` | `"geometry"` | Geometry column |

**Helper method:**
```python
RequiredColumns.all()  # Returns list of all required columns
```

### OutputColumns

**Class**: `OutputColumns`

Column names in final assessment output CSV/PSV files.

See `worker/config.py` lines 160-230 for complete list of 32 output columns.

**Helper method:**
```python
OutputColumns.final_output_order()  # Returns ordered list of all output columns
```

## Configuration validation

All Pydantic settings classes provide automatic validation:

```python
from worker.config import WorkerConfig
from pydantic import ValidationError

try:
    # This will fail - wait_time_seconds must be 1-20
    config = WorkerConfig(wait_time_seconds=30)
except ValidationError as e:
    print(e)
    # Validation error with details about valid range
```

---

## Best practices

1. **Use `.env` files for local development** - Keep environment-specific settings out of code
2. **Use deployment platform for production** - Set environment variables via CDP service settings page in portal.
3. **Don't commit `.env` files** - Already in `.gitignore`
4. **Document any non-default values** - If you override defaults in production, document why
5. **Test with different configurations** - Create custom config instances for unit tests
6. **Use default instances sparingly** - `DEFAULT_CONFIG` and `DEFAULT_GCN_CONFIG` are convenient but loading fresh instances ensures environment variables are respected

---

## Related files

- **Configuration implementation**: `worker/config.py`
- **Environment variable template**: `.env.example`
- **Database engine factory**: `worker/repositories/engine.py`
- **AWS client initialization**: `worker/aws/s3.py`, `worker/aws/sqs.py`
