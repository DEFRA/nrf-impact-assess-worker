"""Assessment execution infrastructure.

This package provides the simple runner for executing assessments:
- run_assessment(): Main entry point for running any registered assessment type

Assessments follow a simple pattern:
- Constructor: __init__(rlb_gdf, metadata, repository)
- Run method: run() -> dict[str, DataFrame]
"""

from worker.runner.runner import run_assessment

__all__ = [
    "run_assessment",
]
