"""SQLAlchemy engine factory for PostGIS connection management."""

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool

from worker.config import DatabaseSettings


def create_db_engine(
    settings: DatabaseSettings | None = None,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    echo: bool = False,
    use_null_pool: bool = False,
) -> Engine:
    """Create a SQLAlchemy engine from database settings.

    Helper factory function for creating engines. The SpatialDataRepository
    accepts an Engine directly, allowing flexibility in configuration.

    Args:
        settings: Database connection settings. If None, uses default settings.
        pool_size: Number of connections to keep in the pool (default: 5)
        max_overflow: Max overflow connections beyond pool_size (default: 10)
        echo: Enable SQLAlchemy query logging (default: False)
        use_null_pool: Use NullPool instead of QueuePool for testing (default: False)

    Returns:
        Configured SQLAlchemy Engine instance
    """
    if settings is None:
        settings = DatabaseSettings()

    # Convert Pydantic PostgresDsn to string
    url = str(settings.url)

    # Select pooling strategy
    if use_null_pool:
        # NullPool: No connection pooling, useful for testing
        poolclass = NullPool
        engine = create_engine(url, poolclass=poolclass, echo=echo)
    else:
        # QueuePool: Standard connection pooling for production
        engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=echo,
        )

    return engine
