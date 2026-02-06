"""SQLAlchemy engine factory for PostGIS connection management.

Supports both local development (static password) and CDP cloud deployment
(IAM authentication with short-lived RDS tokens).
"""

import logging
import os

import boto3
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool

from worker.config import AWSConfig, DatabaseSettings

logger = logging.getLogger(__name__)

# Token lifetime is 15 minutes; recycle connections at 10 minutes
# to ensure fresh tokens before expiry
IAM_TOKEN_POOL_RECYCLE_SECONDS = 600


def _get_iam_auth_token(settings: DatabaseSettings, region: str) -> str:
    """Generate a short-lived IAM authentication token for RDS.

    Args:
        settings: Database settings with host, port, and user.
        region: AWS region for the RDS instance.

    Returns:
        Short-lived authentication token (valid for 15 minutes).
    """
    client = boto3.client("rds", region_name=region)
    token = client.generate_db_auth_token(
        DBHostname=settings.host,
        Port=settings.port,
        DBUsername=settings.user,
        Region=region,
    )
    logger.debug("Generated IAM auth token for RDS connection")
    return token


def _get_password(settings: DatabaseSettings, region: str) -> str:
    """Get the appropriate password based on authentication mode.

    Args:
        settings: Database settings.
        region: AWS region (used for IAM auth).

    Returns:
        Password string (IAM token or static local password).
    """
    if settings.iam_authentication:
        return _get_iam_auth_token(settings, region)
    return settings.local_password


def create_db_engine(
    settings: DatabaseSettings | None = None,
    aws_config: AWSConfig | None = None,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
    use_null_pool: bool = False,
) -> Engine:
    """Create a SQLAlchemy engine from database settings.

    Supports two authentication modes:
    1. Local development: Uses static password from DB_LOCAL_PASSWORD
    2. CDP Cloud (IAM): Uses short-lived tokens from AWS RDS

    When IAM authentication is enabled:
    - Generates fresh tokens for each new connection
    - Enables SSL/TLS with RDS CA certificate
    - Sets pool_recycle to 10 minutes (tokens expire at 15 min)

    Args:
        settings: Database connection settings. If None, uses default settings.
        aws_config: AWS configuration for region. If None, uses AWS_REGION env var.
        pool_size: Number of connections to keep in the pool (default: 5)
        max_overflow: Max overflow connections beyond pool_size (default: 10)
        echo: Enable SQLAlchemy query logging (default: False)
        use_null_pool: Use NullPool instead of QueuePool for testing (default: False)

    Returns:
        Configured SQLAlchemy Engine instance
    """
    if settings is None:
        settings = DatabaseSettings()

    # Get AWS region for IAM auth
    region = aws_config.region if aws_config else os.environ.get("AWS_REGION", "eu-west-2")

    # Build base connection URL
    base_url = settings.connection_url

    # Build connection arguments
    connect_args: dict = {}

    if settings.iam_authentication:
        # Configure SSL for RDS IAM authentication
        # Uses sslmode=require which encrypts the connection but doesn't verify
        # the certificate against a CA bundle (matches CDP Node.js pattern with
        # rejectUnauthorized: false). CDP provides certs via cdp-app-config but
        # the secureContext pattern doesn't directly apply to Python/psycopg2.
        connect_args["sslmode"] = "require"
        logger.info("SSL enabled with sslmode=require for IAM authentication")

    # Create engine based on pooling strategy
    if use_null_pool:
        # NullPool: No connection pooling, useful for testing
        # Get password once since connections aren't pooled
        password = _get_password(settings, region)
        url_with_password = base_url.replace(
            f"{settings.user}@",
            f"{settings.user}:{password}@",
        )
        engine = create_engine(
            url_with_password,
            poolclass=NullPool,
            echo=echo,
            connect_args=connect_args if connect_args else {},
        )
    else:
        # QueuePool: Standard connection pooling for production
        # For IAM auth, we need fresh tokens for each connection
        pool_recycle = IAM_TOKEN_POOL_RECYCLE_SECONDS if settings.iam_authentication else None

        # For IAM auth, use event listener to inject fresh token
        # For local auth, include password in URL
        if settings.iam_authentication:
            # Create engine without password - we'll inject it via event
            engine = create_engine(
                base_url,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_recycle=pool_recycle,
                pool_pre_ping=True,  # Verify connections before use
                echo=echo,
                connect_args=connect_args,
            )

            # Register event listener to inject fresh IAM token for each connection
            @event.listens_for(engine, "do_connect")
            def provide_token(_dialect, _conn_rec, _cargs, cparams):
                """Inject fresh IAM token before each connection."""
                cparams["password"] = _get_iam_auth_token(settings, region)

            logger.info(
                "Created engine with IAM authentication (pool_recycle=%ds)",
                pool_recycle,
            )
        else:
            # Local development: include static password in URL
            password = settings.local_password
            if password:
                url_with_password = base_url.replace(
                    f"{settings.user}@",
                    f"{settings.user}:{password}@",
                )
            else:
                url_with_password = base_url

            engine = create_engine(
                url_with_password,
                poolclass=QueuePool,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
                echo=echo,
                connect_args=connect_args if connect_args else {},
            )
            logger.info("Created engine with local authentication")

    return engine
