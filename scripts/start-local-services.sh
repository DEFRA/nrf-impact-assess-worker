#!/bin/bash
# =============================================================================
# NRF Impact Assessment Worker - Start local services including LocalStack
# =============================================================================
# This script sets up the local development infrastructure for frontend/API
# developers to integrate with the worker.
#
# What it does:
# 1. Starts LocalStack (S3 + SQS) and PostgreSQL services
# 2. Runs database migrations
# 3. Loads full reference data (~5 minutes)
# 4. Builds and starts the worker
#
# After running this script:
# - All services are running in the background
# - Use ./scripts/local-test.sh to submit sanity test job
# - Use docker compose logs -f worker to watch processing
#
# Usage:
#   ./scripts/start-local-services.sh             # Skip data loading (default)
#   ./scripts/start-local-services.sh --load-data # Load full reference data
#
# Prerequisites:
# - Docker and Docker Compose installed
# - UV package manager installed
# - Reference data in iat_input/ directory
# =============================================================================

set -e

# Output colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

LOAD_DATA=false

for arg in "$@"; do
    case $arg in
        --load-data)
            LOAD_DATA=true
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: $0 [--load-data]"
            exit 1
            ;;
    esac
done

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo ""
    echo -e "${GREEN}======================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}======================================${NC}"
}

log_section "Step 1: Starting LocalStack and PostgreSQL"

log_info "Starting services with docker compose..."
docker compose up -d localstack postgres

log_info "Waiting for services to be healthy..."

# Wait for LocalStack with retries
MAX_RETRIES=30
RETRY_COUNT=0
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"' 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        log_error "LocalStack failed to start after ${MAX_RETRIES} seconds"
        exit 1
    fi
    sleep 1
done
log_success "LocalStack is healthy"

# Wait for PostgreSQL with retries
RETRY_COUNT=0
until docker compose ps postgres | grep -q "healthy"; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        log_error "PostgreSQL failed to become healthy after ${MAX_RETRIES} seconds"
        docker compose logs postgres
        exit 1
    fi
    sleep 1
done
log_success "PostgreSQL is healthy"

log_section "Step 2: Running database migrations"

log_info "Running alembic migrations..."
uv run alembic upgrade head
log_success "Migrations complete"

if [[ "$LOAD_DATA" == "true" ]]; then
    log_section "Step 3: Loading Reference Data"

    log_info "Loading FULL reference data (this will take ~8 minutes)..."
    yes | uv run python scripts/load_data.py
    log_success "Reference data loaded"
else
    log_section "Step 3: Skipping Reference Data Load"
    log_info "Using existing reference data (--load-data flag not provided)"
fi

log_info "Step 4: Building and starting worker..."
docker compose build worker
docker compose up -d

log_success "All services started"

echo ""
echo -e "${GREEN}Local development stack is running${NC}"
echo ""
echo "Services:"
echo "  - LocalStack S3:  s3://nrf-inputs (http://localhost:4566)"
echo "  - LocalStack SQS: nrf-assessment-queue"
echo "  - PostgreSQL:     postgresql://localhost:5432/nrf_impact"
echo "  - Worker:         Running in background"
echo ""
echo "Next steps:"
echo "  1. Submit a test job:"
echo "     ./scripts/local-test.sh"
echo ""
echo "  2. Watch worker logs:"
echo "     docker compose logs -f worker"
echo ""
echo "  3. Stop all services:"
echo "     docker compose down"
echo ""
