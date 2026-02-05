"""Output strategies for impact assessment results."""

from worker.outputs.base import OutputStrategy
from worker.outputs.csv import CSVOutputStrategy

__all__ = ["OutputStrategy", "CSVOutputStrategy"]
