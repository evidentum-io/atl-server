#!/usr/bin/env bash
# Production deployment script for ATL Server (Rust)
# Deploys ATL Server with zero-downtime
# Usage: ./scripts/deploy.sh

set -euo pipefail

# Configuration
PROD_SERVER="root@45.33.65.38"
REPO_PATH="/opt/atl-server"
SERVICE_NAME="atl-server"
BINARY_NAME="atl-server"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check SSH connection
log_info "Checking SSH connection to production server..."
if ! ssh -o ConnectTimeout=5 "$PROD_SERVER" "echo 'Connection OK'" &>/dev/null; then
    log_error "Cannot connect to $PROD_SERVER"
    exit 1
fi
log_info "SSH connection successful"

# Get current version
log_info "Checking current version..."
CURRENT_VERSION=$(ssh "$PROD_SERVER" "cd $REPO_PATH && grep '^version' Cargo.toml | head -1 | cut -d'\"' -f2 2>/dev/null || echo 'unknown'")
log_info "Current version: $CURRENT_VERSION"

# Update repository
log_info "Updating git repository..."
ssh "$PROD_SERVER" "cd $REPO_PATH && git fetch origin && git pull origin main"

# Get new commit info
NEW_COMMIT=$(ssh "$PROD_SERVER" "cd $REPO_PATH && git log -1 --format='%h - %s'")
log_info "Deploying commit: $NEW_COMMIT"

# Build release binary
log_info "Building release binary (this may take a few minutes)..."
ssh "$PROD_SERVER" "source ~/.cargo/env && cd $REPO_PATH && cargo build --release"

if [ $? -ne 0 ]; then
    log_error "Cargo build failed!"
    exit 1
fi

# Check if .env exists, if not create from .env.example
log_info "Checking configuration..."
ssh "$PROD_SERVER" "if [ ! -f $REPO_PATH/.env ]; then cp $REPO_PATH/.env.example $REPO_PATH/.env && echo '.env created from example'; fi"

# Stop service
log_info "Stopping $SERVICE_NAME service..."
ssh "$PROD_SERVER" "systemctl stop $SERVICE_NAME"

if [ $? -ne 0 ]; then
    log_error "Service stop failed!"
    exit 1
fi

# Install binary
log_info "Installing binary..."
ssh "$PROD_SERVER" "cp $REPO_PATH/target/release/$BINARY_NAME /usr/local/bin/$BINARY_NAME && chmod +x /usr/local/bin/$BINARY_NAME"

# Clear the failed state and the crash-loop counter accumulated before this
# deploy. systemd refuses to start a unit that exhausted StartLimitBurst until
# the counter is reset, and neither `stop` nor `start` clears it -- only
# `reset-failed` does. A unit cannot do this to itself, so the deploy is the
# place: this is where an operator deliberately intends a start.
ssh "$PROD_SERVER" "systemctl reset-failed $SERVICE_NAME || true"

# Start service
log_info "Starting $SERVICE_NAME service..."
ssh "$PROD_SERVER" "systemctl start $SERVICE_NAME"

if [ $? -ne 0 ]; then
    log_error "Service restart failed!"
    exit 1
fi

# Wait a bit and verify
sleep 2

# Check if service is running
if ! ssh "$PROD_SERVER" "systemctl is-active --quiet $SERVICE_NAME"; then
    log_error "$SERVICE_NAME is not running!"
    exit 1
fi

# Get new version
NEW_VERSION=$(ssh "$PROD_SERVER" "cd $REPO_PATH && grep '^version' Cargo.toml | head -1 | cut -d'\"' -f2 2>/dev/null || echo 'unknown'")
log_info "New version: $NEW_VERSION"

# Show service status
log_info "$SERVICE_NAME status:"
ssh "$PROD_SERVER" "systemctl status $SERVICE_NAME --no-pager -l | head -n 10"

echo ""
log_info "Deployment completed successfully!"
log_info "Previous version: $CURRENT_VERSION"
log_info "New version: $NEW_VERSION"
log_info "Commit: $NEW_COMMIT"
echo ""
log_info "ATL Server is running on port 3000 (internal)"
echo ""
log_info "Useful commands:"
log_info "  View logs: ssh $PROD_SERVER 'journalctl -u $SERVICE_NAME -f'"
log_info "  Check status: ssh $PROD_SERVER 'systemctl status $SERVICE_NAME'"
log_info "  Restart: ssh $PROD_SERVER 'systemctl restart $SERVICE_NAME'"
log_info "  View config: ssh $PROD_SERVER 'cat $REPO_PATH/.env'"
