"""Configuration and constants for the NRF Impact Assessment Worker.

This module defines all business rules, constants, and configuration
for multiple environmental impact assessments.

Includes configuration for:
- Nutrient mitigation assessment (AssessmentConfig with IAT_ prefix)
- GCN (Great Crested Newt) assessment (GcnConfig with GCN_ prefix)
- Database connection (DatabaseSettings with DB_ prefix)
- AWS resources and worker polling (AWSConfig, WorkerConfig)

Configuration can be overridden via:
1. Environment variables (e.g., IAT_PRECAUTIONARY_BUFFER_PERCENT=25.0, GCN_BUFFER_DISTANCE_M=300)
2. .env file in the current directory
3. Default values in code
"""

import os
from dataclasses import dataclass
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class PhysicalConstants:
    """Physical and mathematical constants used in impact calculations.

    These are NOT configurable - they represent fixed conversion factors,
    mathematical constants, and standard coordinate reference systems that
    should never vary.

    All attributes are immutable (frozen=True prevents modification).
    """

    # Geographic coordinate reference system
    CRS_BRITISH_NATIONAL_GRID: str = "EPSG:27700"

    # Unit conversion factors
    DAYS_PER_YEAR: float = 365.25  # Accounting for leap years
    SQUARE_METRES_PER_HECTARE: float = 10_000.0
    MILLIGRAMS_PER_KILOGRAM: float = 1_000_000.0


# Module-level singleton for physical constants
CONSTANTS = PhysicalConstants()


class SuDsConfig(BaseSettings):
    """Configuration for Sustainable Drainage Systems (SuDS) mitigation.

    Can be overridden via environment variables with SUDS_ prefix:
    - SUDS_THRESHOLD_DWELLINGS
    - SUDS_FLOW_CAPTURE_PERCENT
    - SUDS_REMOVAL_RATE_PERCENT

    Attributes:
        threshold_dwellings: Minimum number of dwellings to trigger SuDS requirements
        flow_capture_percent: Percentage of flow entering SuDS system
        removal_rate_percent: Nutrient removal efficiency of SuDS
    """

    model_config = SettingsConfigDict(
        env_prefix="SUDS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    threshold_dwellings: int = Field(
        default=50, description="Number of dwellings required to trigger SuDS"
    )
    flow_capture_percent: float = Field(
        default=100.0, description="Percentage of flow entering SuDS system"
    )
    removal_rate_percent: float = Field(default=25.0, description="SuDS nutrient removal rate (%)")

    @property
    def total_reduction_factor(self) -> float:
        """Calculate total reduction as a decimal factor.

        Returns:
            Combined reduction factor (e.g., 0.25 for 25% removal)
        """
        return (self.flow_capture_percent / 100) * (self.removal_rate_percent / 100)


class AssessmentConfig(BaseSettings):
    """Main configuration for nutrient impact assessment business rules.

    Can be overridden via environment variables with IAT_ prefix:
    - IAT_PRECAUTIONARY_BUFFER_PERCENT
    - IAT_FALLBACK_WWTW_ID
    - SUDS_* variables for nested SuDS configuration

    Attributes:
        precautionary_buffer_percent: Additional buffer applied to total impacts
        suds: SuDS mitigation configuration
        fallback_wwtw_id: Default ID for developments outside modeled WwTW catchments

    Note:
        Physical constants (days per year, unit conversions) are in the CONSTANTS
        module variable, not configurable via environment variables.
    """

    model_config = SettingsConfigDict(
        env_prefix="IAT_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    precautionary_buffer_percent: float = Field(
        default=20.0,
        description="Precautionary buffer added to total nutrient impacts (%)",
    )
    suds: SuDsConfig = Field(
        default_factory=SuDsConfig, description="SuDS mitigation configuration"
    )
    fallback_wwtw_id: int = Field(
        default=141, description="WwTW ID for developments outside modeled catchments"
    )

    @property
    def precautionary_buffer_factor(self) -> float:
        """Calculate precautionary buffer as a decimal factor.

        Returns:
            Buffer factor (e.g., 0.20 for 20% buffer)
        """
        return self.precautionary_buffer_percent / 100


class RequiredColumns:
    """Required column names in input Red Line Boundary shapefile (normalized snake_case)."""

    ID = "id"
    NAME = "name"
    DWELLING_CATEGORY = "dwelling_category"
    SOURCE = "source"
    DWELLINGS = "dwellings"
    SHAPE_AREA = "shape_area"
    GEOMETRY = "geometry"

    @classmethod
    def all(cls) -> list[str]:
        """Get list of all required columns."""
        return [
            cls.ID,
            cls.NAME,
            cls.DWELLING_CATEGORY,
            cls.SOURCE,
            cls.DWELLINGS,
            cls.SHAPE_AREA,
            cls.GEOMETRY,
        ]


class OutputColumns:
    """Column names in final output CSV."""

    RLB_ID = "RLB_ID"
    ID = "id"
    NAME = "Name"
    DWELLING_CATEGORY = "Dwel_Cat"
    SOURCE = "Source"
    DWELLINGS = "Dwellings"
    DEV_AREA_HA = "Dev_Area_Ha"
    AREA_IN_NN_CATCHMENT = "AreaInNNCatchment"
    NN_CATCHMENT = "NN_Catchment"
    DEV_SUBCATCHMENT = "Dev_SubCatchment"
    MAJORITY_LPA = "Majority_LPA"
    MAJORITY_WWTW_ID = "Majority_WwTw_ID"
    WWTW_NAME = "WwTW_name"
    WWTW_SUBCATCHMENT = "WwTw_SubCatchment"
    N_LU_UPLIFT = "N_LU_Uplift"
    P_LU_UPLIFT = "P_LU_Uplift"
    N_LU_POST_SUDS = "N_LU_postSuDS"
    P_LU_POST_SUDS = "P_LU_postSuDS"
    OCC_RATE = "Occ_Rate"
    WATER_USAGE_L_DAY = "Water_Usage_L_Day"
    LITRES_USED = "Litres_used"
    NITROGEN_2025_2030 = "Nitrogen_2025_2030"
    NITROGEN_2030_ONWARDS = "Nitrogen_2030_onwards"
    PHOSPHORUS_2025_2030 = "Phosphorus_2025_2030"
    PHOSPHORUS_2030_ONWARDS = "Phosphorus_2030_onwards"
    N_WWTW_TEMP = "N_WwTW_Temp"
    P_WWTW_TEMP = "P_WwTW_Temp"
    N_WWTW_PERM = "N_WwTW_Perm"
    P_WWTW_PERM = "P_WwTW_Perm"
    N_TOTAL = "N_Total"
    P_TOTAL = "P_Total"

    @classmethod
    def final_output_order(cls) -> list[str]:
        """Get the ordered list of columns for final CSV output."""
        return [
            cls.RLB_ID,
            cls.ID,
            cls.NAME,
            cls.DWELLING_CATEGORY,
            cls.SOURCE,
            cls.DWELLINGS,
            cls.DEV_AREA_HA,
            cls.AREA_IN_NN_CATCHMENT,
            cls.NN_CATCHMENT,
            cls.DEV_SUBCATCHMENT,
            cls.MAJORITY_LPA,
            cls.MAJORITY_WWTW_ID,
            cls.WWTW_NAME,
            cls.WWTW_SUBCATCHMENT,
            cls.N_LU_UPLIFT,
            cls.P_LU_UPLIFT,
            cls.N_LU_POST_SUDS,
            cls.P_LU_POST_SUDS,
            cls.OCC_RATE,
            cls.WATER_USAGE_L_DAY,
            cls.LITRES_USED,
            cls.NITROGEN_2025_2030,
            cls.NITROGEN_2030_ONWARDS,
            cls.PHOSPHORUS_2025_2030,
            cls.PHOSPHORUS_2030_ONWARDS,
            cls.N_WWTW_TEMP,
            cls.P_WWTW_TEMP,
            cls.N_WWTW_PERM,
            cls.P_WWTW_PERM,
            cls.N_TOTAL,
            cls.P_TOTAL,
        ]


DEFAULT_CONFIG = AssessmentConfig()


class GcnConfig(BaseSettings):
    """Configuration for GCN (Great Crested Newt) impact assessment.

    Can be overridden via environment variables with GCN_ prefix:
    - GCN_BUFFER_DISTANCE_M
    - GCN_POND_BUFFER_DISTANCE_M
    - GCN_MERGE_DISTANCE_M
    - GCN_PRECISION_GRID_SIZE
    - GCN_TARGET_CRS

    Attributes:
        buffer_distance_m: Buffer distance around RLB in metres
        pond_buffer_distance_m: Buffer distance around ponds in metres
        merge_distance_m: Sites within this distance create single buffer
        precision_grid_size: Coordinate precision grid size for geometry operations
        target_crs: Target coordinate reference system (British National Grid)
    """

    model_config = SettingsConfigDict(
        env_prefix="GCN_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    buffer_distance_m: int = Field(
        default=250, ge=0, description="Buffer distance around RLB (metres)"
    )
    pond_buffer_distance_m: int = Field(
        default=250, ge=0, description="Buffer distance around ponds (metres)"
    )
    merge_distance_m: int = Field(
        default=500, ge=0, description="Sites within this distance create single buffer (metres)"
    )
    precision_grid_size: float = Field(
        default=0.0001,
        gt=0,
        description="Coordinate precision grid size (metres, 0.1mm matches ArcGIS XY Resolution)",
    )
    target_crs: str = Field(
        default="EPSG:27700", description="British National Grid coordinate reference system"
    )


DEFAULT_GCN_CONFIG = GcnConfig()


class DatabaseSettings(BaseSettings):
    """Database connection configuration for PostGIS.

    Supports two modes:
    1. Local development: Uses static password from DB_LOCAL_PASSWORD
    2. CDP Cloud (IAM): Uses IAM authentication with short-lived RDS tokens

    Environment variables:
    - DB_HOST: Database host (default: localhost)
    - DB_PORT: Database port (default: 5432)
    - DB_DATABASE: Database name (default: nrf_impact)
    - DB_USER: Database user (default: postgres)
    - DB_IAM_AUTHENTICATION: Enable IAM auth (default: true)
    - DB_LOCAL_PASSWORD: Static password for local dev (default: empty)
    - DB_SSL_MODE: SSL mode - require, verify-ca, verify-full (default: require)
    - DB_RDS_TRUSTSTORE: Name of TRUSTSTORE_* env var for RDS cert (default: RDS_ROOT_CA)

    When DB_IAM_AUTHENTICATION=true:
    - Requests short-lived tokens from AWS RDS
    - Enables SSL/TLS with configured ssl_mode
    - Uses CA cert from TRUSTSTORE_* env var if available
    - Connection pool recycling is set to 10 min (tokens expire at 15 min)
    """

    model_config = SettingsConfigDict(
        env_prefix="DB_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Connection parameters
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    database: str = Field(default="nrf_impact", description="Database name")
    user: str = Field(default="postgres", description="Database user")

    # Authentication mode
    iam_authentication: bool = Field(
        default=True,
        description="Use IAM authentication for RDS (set to false for local dev)",
    )
    local_password: str = Field(
        default="",
        description="Static password for local development",
    )

    # SSL/TLS configuration
    ssl_mode: str = Field(
        default="require",
        description="SSL mode for database connections (require, verify-ca, verify-full)",
    )
    rds_truststore: str = Field(
        default="RDS_ROOT_CA",
        description="Name of TRUSTSTORE_* env var containing RDS CA cert (default: TRUSTSTORE_RDS_ROOT_CA)",
    )

    @property
    def connection_url(self) -> str:
        """Build connection URL from individual parameters.

        Password is not included - it's injected by the engine factory
        (either static password or IAM token).
        """
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"


class AWSConfig(BaseSettings):
    """AWS resource configuration for ECS worker deployment."""

    model_config = SettingsConfigDict(
        env_prefix="AWS_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # S3 Configuration (input only)
    s3_input_bucket: str
    region: str = Field(default="eu-west-2")

    # SQS Configuration
    sqs_queue_url: str

    # Optional endpoint URL for LocalStack (local development)
    endpoint_url: str | None = Field(default=None, description="Override AWS endpoint for LocalStack")

    # Model config for validation
    @field_validator("s3_input_bucket", "sqs_queue_url")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            msg = "AWS resource identifiers cannot be empty"
            raise ValueError(msg)
        return v


class WorkerConfig(BaseSettings):
    """Worker polling and processing configuration."""

    model_config = SettingsConfigDict(
        env_prefix="SQS_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    wait_time_seconds: int = Field(default=20, ge=1, le=20)
    visibility_timeout: int = Field(default=300, ge=30, le=43200)
    max_messages: int = Field(default=1, ge=1, le=10)
    graceful_shutdown_timeout: int = Field(default=30, ge=0, le=300)

    # Parallelization settings
    parallel_spatial_ops: bool = Field(
        default=True, description="Enable parallel spatial operations"
    )
    max_spatial_workers: int | None = Field(
        default=None, description="Number of worker processes for spatial ops (None = auto-detect)"
    )


class ApiServerConfig(BaseSettings):
    """Configuration for the HTTP API server.

    Can be overridden via environment variables with API_ prefix:
    - API_PORT (default: 8085)
    - API_JOB_SUBMISSION_ENABLED: Enable HTTP job submission (default: false)

    The API server runs in a separate process to ensure responsiveness
    during CPU-intensive operations in the main worker process. It provides:
    - /health - Health check endpoint for CDP ECS monitoring
    - /job - HTTP job submission endpoint (when API_JOB_SUBMISSION_ENABLED=true)
    """

    model_config = SettingsConfigDict(
        env_prefix="API_",
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

    port: int = Field(default=8085, ge=1, le=65535, description="Port for the API server")
    job_submission_enabled: bool = Field(
        default=False,
        description="Enable HTTP job submission endpoint (default: false)",
    )


class DebugConfig:
    """Debug output configuration.

    WARNING: For local development only. Never enable in production.
    - Adds disk I/O overhead
    - Consumes storage space
    - May expose sensitive geometry data
    """

    def __init__(
        self,
        enabled: bool = False,
        output_dir: Path = Path("/tmp/iat-debug"),
    ):
        self.enabled = enabled
        self.output_dir = output_dir

    @classmethod
    def from_env(cls) -> "DebugConfig":
        return cls(
            enabled=os.environ.get("DEBUG_OUTPUT", "false").lower() == "true",
            output_dir=Path(os.environ.get("DEBUG_OUTPUT_DIR", "/tmp/iat-debug")),
        )


