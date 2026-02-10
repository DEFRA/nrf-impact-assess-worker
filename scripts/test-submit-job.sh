#!/bin/bash
# =============================================================================
# NRF Impact Assessment Worker - Test Job Submission
# =============================================================================
# Submit test jobs to either local development or CDP cloud environments.
#
# Usage:
#   ./scripts/test-submit-job.sh                          # Local, default shapefile
#   ./scripts/test-submit-job.sh --cloud                  # Cloud, default shapefile
#   ./scripts/test-submit-job.sh path/to/file.shp         # Local, custom shapefile
#   ./scripts/test-submit-job.sh --cloud path/to/file.shp # Cloud, custom shapefile
#
# Environment Variables:
#   Local mode (default):
#     API_URL         - Override local URL (default: http://localhost:8085)
#
#   Cloud mode (--cloud):
#     CDP_API_KEY     - API key for CDP environment (required)
#     CDP_BASE_URL    - Base URL (default: ephemeral-protected.api.dev.cdp-int.defra.cloud)
#     CDP_SERVICE     - Service name (default: nrf-impact-assess-worker)
#
#   Job parameters (both modes):
#     JOB_EMAIL           - Developer email (default: test@example.com)
#     JOB_DWELLING_TYPE   - Dwelling type (default: house)
#     JOB_DWELLINGS       - Number of dwellings (default: 1)
#     JOB_DEV_NAME        - Development name (default: Test Development)
#     JOB_ASSESSMENT_TYPE - Assessment type: nutrient or gcn (default: nutrient)
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
CLOUD_MODE=false
SHAPEFILE_ARG=""

for arg in "$@"; do
    case $arg in
        --cloud|-c)
            CLOUD_MODE=true
            ;;
        *)
            SHAPEFILE_ARG="$arg"
            ;;
    esac
done

# Configuration
TEST_SHAPEFILE="${SHAPEFILE_ARG:-tests/data/inputs/nutrients/BnW_small_under_1_hectare/BnW_small_under_1_hectare.shp}"

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

if [[ "$CLOUD_MODE" == "true" ]]; then
    log_info "Mode: CLOUD"

    # Check for API key in cloud mode
    if [[ -z "$CDP_API_KEY" ]]; then
        log_error "CDP_API_KEY environment variable is not set"
        echo ""
        echo "Set it with:"
        echo "  export CDP_API_KEY='your-api-key'"
        exit 1
    fi
    log_success "CDP_API_KEY is set"

    CDP_BASE_URL="${CDP_BASE_URL:-ephemeral-protected.api.dev.cdp-int.defra.cloud}"
    CDP_SERVICE="${CDP_SERVICE:-nrf-impact-assess-worker}"
    ENDPOINT_URL="https://${CDP_BASE_URL}/${CDP_SERVICE}/job"
else
    log_info "Mode: LOCAL"
    API_URL="${API_URL:-http://localhost:8085}"
    ENDPOINT_URL="${API_URL}/job"

    # Check if local server is running
    if ! curl -s "${API_URL}/health" > /dev/null 2>&1; then
        log_error "Local API server is not running at ${API_URL}"
        echo ""
        echo "Start the worker first:"
        echo "  export JOB_SUBMISSION_ENABLED=true"
        echo "  uv run python -m worker.main"
        exit 1
    fi
    log_success "Local API server is running"
fi

# Check if shapefile exists
if [[ ! -f "$TEST_SHAPEFILE" ]]; then
    log_error "Shapefile not found: $TEST_SHAPEFILE"
    exit 1
fi
log_success "Shapefile found: $TEST_SHAPEFILE"

# Check if jq is available for JSON processing
if ! command -v jq &> /dev/null; then
    log_error "jq is required but not installed"
    echo "Install it with: brew install jq"
    exit 1
fi
log_success "jq is available"

# =============================================================================
# Convert Shapefile to GeoJSON
# =============================================================================
log_section "Converting Shapefile to GeoJSON"

# Create temp file for GeoJSON
TEMP_GEOJSON=$(mktemp -t test-submit-job-XXXXXX.geojson)
trap "rm -f $TEMP_GEOJSON" EXIT

log_info "Converting: $TEST_SHAPEFILE"

# Use Python with geopandas to convert
uv run python -c "
import geopandas as gpd

gdf = gpd.read_file('$TEST_SHAPEFILE')
gdf.to_file('$TEMP_GEOJSON', driver='GeoJSON')
print(f'Converted {len(gdf)} feature(s) to GeoJSON')
"

log_success "Shapefile converted to GeoJSON"

# Read the geometry from the GeoJSON file
GEOMETRY=$(cat "$TEMP_GEOJSON")

# =============================================================================
# Build Request Payload
# =============================================================================
log_section "Preparing Job Submission"

# Job parameters
DEVELOPER_EMAIL="${JOB_EMAIL:-test@example.com}"
DWELLING_TYPE="${JOB_DWELLING_TYPE:-house}"
NUMBER_OF_DWELLINGS="${JOB_DWELLINGS:-1}"
DEVELOPMENT_NAME="${JOB_DEV_NAME:-Test Development}"
ASSESSMENT_TYPE="${JOB_ASSESSMENT_TYPE:-nutrient}"

log_info "Developer email: $DEVELOPER_EMAIL"
log_info "Dwelling type: $DWELLING_TYPE"
log_info "Number of dwellings: $NUMBER_OF_DWELLINGS"
log_info "Development name: $DEVELOPMENT_NAME"
log_info "Assessment type: $ASSESSMENT_TYPE"

# Build the full request payload
REQUEST_PAYLOAD=$(jq -n \
    --argjson geometry "$GEOMETRY" \
    --arg email "$DEVELOPER_EMAIL" \
    --arg dwelling_type "$DWELLING_TYPE" \
    --argjson dwellings "$NUMBER_OF_DWELLINGS" \
    --arg dev_name "$DEVELOPMENT_NAME" \
    --arg assessment_type "$ASSESSMENT_TYPE" \
    '{
        geometry: $geometry,
        developer_email: $email,
        dwelling_type: $dwelling_type,
        number_of_dwellings: $dwellings,
        development_name: $dev_name,
        assessment_type: $assessment_type
    }')

# =============================================================================
# Submit Job
# =============================================================================
log_section "Submitting Job"

log_info "Endpoint: $ENDPOINT_URL"

# Submit the job
if [[ "$CLOUD_MODE" == "true" ]]; then
    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "x-api-key: $CDP_API_KEY" \
        -H "Content-Type: application/json" \
        -d "$REQUEST_PAYLOAD" \
        "$ENDPOINT_URL")
else
    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$REQUEST_PAYLOAD" \
        "$ENDPOINT_URL")
fi

# Extract HTTP status code (last line) and body (everything else)
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')

if [[ "$HTTP_CODE" -eq 200 ]] || [[ "$HTTP_CODE" -eq 201 ]]; then
    log_success "Job submitted successfully!"
    echo ""
    echo "Response:"
    echo "$RESPONSE_BODY" | jq .

    JOB_ID=$(echo "$RESPONSE_BODY" | jq -r '.job_id // empty')
elif [[ "$HTTP_CODE" -eq 403 ]]; then
    log_error "Job submission endpoint is disabled"
    echo ""
    echo "Ensure JOB_SUBMISSION_ENABLED=true is set in the environment"
    exit 1
else
    log_error "Failed to submit job (HTTP $HTTP_CODE)"
    echo ""
    echo "Response:"
    echo "$RESPONSE_BODY" | jq . 2>/dev/null || echo "$RESPONSE_BODY"
    exit 1
fi

# =============================================================================
# Next Steps
# =============================================================================
log_section "Next Steps"

echo ""
echo "Your job has been queued. To monitor progress:"
echo ""
if [[ "$CLOUD_MODE" == "true" ]]; then
    echo "  1. Check CloudWatch logs in the CDP console"
    echo "  2. Look for log messages containing: 'Processing job: $JOB_ID'"
else
    echo "  1. Check worker logs:"
    echo "     docker compose logs -f worker"
    echo "     # or check the terminal running the worker"
    echo ""
    echo "  2. Look for log messages containing: 'Processing job: $JOB_ID'"
fi
echo ""
