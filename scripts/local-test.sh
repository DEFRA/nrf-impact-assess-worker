#!/bin/bash
# =============================================================================
# NRF Impact Assessment Worker - Test Job Submission
# =============================================================================
# This script submits a test job to the local development stack.
#
# Prerequisites:
# - Services must be running (use ./scripts/start-local-services.sh first)
# - LocalStack and PostgreSQL must be healthy
# - Reference data must be loaded
#
# Usage:
#   ./scripts/local-test.sh                                    # Use default test shapefile
#   ./scripts/local-test.sh path/to/your/shapefile.shp         # Use custom shapefile
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TEST_SHAPEFILE="${1:-tests/data/inputs/nutrients/BnW_small_under_1_hectare/BnW_small_under_1_hectare.shp}"

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

# =============================================================================
# Pre-flight Checks
# =============================================================================
log_section "Pre-flight Checks"

# Check if shapefile exists
if [[ ! -f "$TEST_SHAPEFILE" ]]; then
    log_error "Shapefile not found: $TEST_SHAPEFILE"
    exit 1
fi
log_success "Shapefile found: $TEST_SHAPEFILE"

# Start services if not already running
log_info "Checking if services are running..."
if ! curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"' 2>/dev/null \
   || ! docker compose ps postgres | grep -q "healthy"; then
    log_info "Services not running, starting them..."
    docker compose up -d
    log_info "Waiting for services to become healthy..."
    # Wait for LocalStack
    for i in $(seq 1 30); do
        if curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"' 2>/dev/null; then
            break
        fi
        sleep 2
    done
    # Wait for PostgreSQL
    for i in $(seq 1 30); do
        if docker compose ps postgres | grep -q "healthy"; then
            break
        fi
        sleep 2
    done
fi

# Verify services are healthy
if ! curl -s http://localhost:4566/_localstack/health | grep -q '"s3": "running"' 2>/dev/null; then
    log_error "LocalStack failed to start"
    exit 1
fi
log_success "LocalStack is healthy"

if ! docker compose ps postgres | grep -q "healthy"; then
    log_error "PostgreSQL failed to start"
    exit 1
fi
log_success "PostgreSQL is healthy"

# =============================================================================
# Submit Test Job
# =============================================================================
log_section "Submitting Test Job"

log_info "Using shapefile: $TEST_SHAPEFILE"
log_info "Submitting to LocalStack..."

uv run python scripts/submit_job.py \
    "$TEST_SHAPEFILE" \
    --email "test@example.com" \
    --dwelling-type "house" \
    --dwellings 1 \
    --name "Local Test Development"

log_success "Test job submitted successfully!"

# =============================================================================
# Next Steps
# =============================================================================
log_section "Next Steps"

echo ""
echo "Your test job has been queued. To see it being processed:"
echo ""
echo "  1. Check worker logs:"
echo "     docker compose logs -f worker"
echo ""
echo "  2. You should see log messages like these:"
echo "     - \"Worker started, polling for jobs...\""
echo "     - \"processing complete, message deleted from queue\""
echo ""
