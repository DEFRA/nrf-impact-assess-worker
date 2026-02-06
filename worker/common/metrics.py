"""CloudWatch metrics integration via AWS Embedded Metrics Format (EMF).

Provides utilities for sending metrics to CloudWatch in ECS/CDP environments.
The EMF library handles formatting and transmission to the CloudWatch agent.

Configuration via environment variables:
- AWS_EMF_ENVIRONMENT: Set to "local" for local CloudWatch agent
- AWS_EMF_AGENT_ENDPOINT: CloudWatch agent endpoint (e.g., tcp://127.0.0.1:25888)
- AWS_EMF_NAMESPACE: CloudWatch namespace for metrics
- AWS_EMF_LOG_GROUP_NAME: Log group for EMF metrics
"""

from logging import getLogger

from aws_embedded_metrics import metric_scope
from aws_embedded_metrics.storage_resolution import StorageResolution

logger = getLogger(__name__)


@metric_scope
def _put_metric(metric_name: str, value: float, unit: str, metrics) -> None:
    """Internal function to put a metric with EMF decorator.

    Note: The aws_embedded_metrics library has known issues with async frameworks.
    See: https://github.com/awslabs/aws-embedded-metrics-python/issues/52
    """
    logger.debug("put metric: %s - %s - %s", metric_name, value, unit)
    metrics.put_metric(metric_name, value, unit, StorageResolution.STANDARD)


def counter(metric_name: str, value: float = 1) -> None:
    """Increment a CloudWatch counter metric.

    Wraps the EMF put_metric call with exception handling to ensure
    metric failures don't crash the application.

    Args:
        metric_name: Name of the metric in CloudWatch
        value: Counter value to record (default: 1)
    """
    try:
        _put_metric(metric_name, value, "Count")
    except Exception as e:
        logger.error("Error calling put_metric: %s", e)
