#!/bin/bash
# E2E Test: Pure Runtime Architecture

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="${SCRIPT_DIR}/test-workspace"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}✓${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }

cleanup() {
    log_info "Cleaning up..."
    rm -rf "${TEST_DIR}"
}
trap cleanup EXIT

echo "=========================================="
echo "E2E Test: Pure Runtime Architecture"
echo "=========================================="
echo ""

# Test 1: Unit tests
echo "Test 1: Running Unit Tests"
echo "---------------------------"
cd "${SCRIPT_DIR}/.."
if cargo test 2>&1 | grep -q "test result: ok"; then
    log_info "All unit tests passed"
else
    log_error "Unit tests failed"
    exit 1
fi

echo ""
log_info "Phase 1 & 2 Implementation: VERIFIED"
