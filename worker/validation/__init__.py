"""Validation module for Red Line Boundary geometry files and development data.

This module provides validators for:
1. Geometry validation - Validates geometry files (shapefile or GeoJSON: CRS, topology, coordinates)
2. Development data validation - Validates CLI/local test data with embedded attributes

Note: In production, geometry files contain only geometry. Development data
(dwelling_type, number_of_dwellings) comes from ImpactAssessmentJob and is
validated by Pydantic. EmbeddedDevelopmentDataValidator is for CLI/local testing only.
"""

from worker.validation.development_data import EmbeddedDevelopmentDataValidator
from worker.validation.errors import ValidationError
from worker.validation.geometry import GeometryValidator
from worker.validation.protocols import DevelopmentDataValidator

__all__ = [
    "ValidationError",
    "DevelopmentDataValidator",
    "GeometryValidator",
    "EmbeddedDevelopmentDataValidator",
]
