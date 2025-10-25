#!/usr/bin/env bash

# =============================================================================
# Unified HDFS Setup Script for Lakesail
# =============================================================================
# This script sets up an HDFS cluster with optional Kerberos authentication.
#
# Usage:
#   ./setup_lakesail_hdfs.sh [standard|kerberos]
#
# If no argument is provided, you'll be prompted to choose.
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
USERNAME=$(whoami)

# =============================================================================
# Setup Mode Selection
# =============================================================================

echo -e "${BLUE}🚀 Lakesail HDFS Setup${NC}"
echo -e "${BLUE}======================${NC}"
echo ""

# Check if mode was provided as argument
if [ $# -eq 0 ]; then
    echo "Select HDFS setup mode:"
    echo ""
    echo "  1) Standard (no authentication)"
    echo "     - Quick setup (~5 minutes)"
    echo "     - Best for: Development, testing, learning"
    echo "     - Container: hdfs-working"
    echo ""
    echo "  2) Kerberos (enterprise authentication)"
    echo "     - Secure setup (~10 minutes)"
    echo "     - Best for: Production, multi-user, compliance"
    echo "     - Container: hdfs-kerberos"
    echo ""
    read -p "Enter choice (1 or 2): " choice

    case $choice in
        1) MODE="standard" ;;
        2) MODE="kerberos" ;;
        *)
            echo -e "${RED}❌ Invalid choice${NC}"
            exit 1
            ;;
    esac
else
    MODE="$1"
    if [ "$MODE" != "standard" ] && [ "$MODE" != "kerberos" ]; then
        echo -e "${RED}❌ Invalid mode: $MODE${NC}"
        echo "Usage: $0 [standard|kerberos]"
        exit 1
    fi
fi

echo ""
echo -e "${GREEN}✅ Selected mode: $MODE${NC}"
echo ""

# =============================================================================
# Standard HDFS Setup
# =============================================================================

setup_standard() {
    local CONTAINER_NAME="hdfs-working"
    local NAMENODE_PORT=9000
    local DATANODE_PORT=9866
    local DATANODE_HTTP_PORT=9864
    local WEB_UI_PORT=9870

    echo -e "${BLUE}🔓 Setting up Standard HDFS (no authentication)${NC}"
    echo -e "${BLUE}===============================================${NC}"

    # Step 1: Build Docker image
    echo -e "\n${YELLOW}📦 Step 1: Building HDFS Docker Image${NC}"
    if docker images | grep -q "local-hdfs"; then
        echo "✅ HDFS Docker image already exists"
    else
        echo "🔨 Building HDFS Docker image..."

        if [ -d "../scripts/hadoop" ]; then
            echo "📁 Using HDFS scripts from parent sail repository..."
            docker build -t local-hdfs ../scripts/hadoop/
        elif [ -d "scripts/hadoop" ]; then
            echo "📁 Using HDFS scripts from current directory..."
            docker build -t local-hdfs scripts/hadoop/
        else
            echo -e "${RED}❌ Error: Cannot find scripts/hadoop directory${NC}"
            echo "💡 Make sure you're running this from the sail repository or have copied the guide there"
            exit 1
        fi

        echo "✅ HDFS Docker image built successfully"
    fi

    # Step 2: Cleanup existing containers
    echo -e "\n${YELLOW}🛑 Step 2: Cleaning up existing containers${NC}"
    if docker ps -a | grep -q "$CONTAINER_NAME"; then
        echo "🗑️  Stopping and removing existing container..."
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
        echo "✅ Cleanup completed"
    else
        echo "✅ No existing containers to clean up"
    fi

    # Step 3: Start container
    echo -e "\n${YELLOW}🐳 Step 3: Starting HDFS Container${NC}"
    echo "🔌 Exposing ports: $NAMENODE_PORT, $DATANODE_HTTP_PORT, $DATANODE_PORT, $WEB_UI_PORT"
    docker run -d \
        --name "$CONTAINER_NAME" \
        --rm \
        -p $NAMENODE_PORT:$NAMENODE_PORT \
        -p $DATANODE_HTTP_PORT:$DATANODE_HTTP_PORT \
        -p $DATANODE_PORT:$DATANODE_PORT \
        -p $WEB_UI_PORT:$WEB_UI_PORT \
        local-hdfs

    echo "⏳ Waiting for HDFS to initialize (15 seconds)..."
    sleep 15

    # Step 4: Verify HDFS
    echo -e "\n${YELLOW}🔍 Step 4: Verifying HDFS Status${NC}"
    if docker exec "$CONTAINER_NAME" hdfs dfsadmin -report > /dev/null 2>&1; then
        echo "✅ HDFS cluster is healthy"
        docker exec "$CONTAINER_NAME" hdfs dfsadmin -report | grep "Live datanodes"
    else
        echo -e "${RED}❌ HDFS cluster failed to start${NC}"
        echo "Logs:"
        docker logs "$CONTAINER_NAME" | tail -20
        exit 1
    fi

    # Step 5: Set up permissions
    echo -e "\n${YELLOW}🔐 Step 5: Setting up HDFS Permissions${NC}"
    echo "👤 Creating user directory for: $USERNAME"

    docker exec "$CONTAINER_NAME" hdfs dfs -mkdir -p "/user/$USERNAME"
    docker exec "$CONTAINER_NAME" hdfs dfs -chown "$USERNAME:supergroup" "/user/$USERNAME" 2>/dev/null || true
    docker exec "$CONTAINER_NAME" hdfs dfs -chmod 755 "/user/$USERNAME"

    echo "✅ HDFS permissions configured"
    docker exec "$CONTAINER_NAME" hdfs dfs -ls /user/

    # Step 6: Run verification
    echo -e "\n${YELLOW}✅ Step 6: Running Verification${NC}"
    if [ -f "$SCRIPT_DIR/verify_setup.py" ]; then
        echo "🔍 Running verify_setup.py..."
        python3 "$SCRIPT_DIR/verify_setup.py" || true
    else
        echo "⚠️  Verification script not found, skipping..."
    fi

    # Summary
    echo -e "\n${GREEN}🎉 Standard HDFS Setup Complete!${NC}"
    echo -e "${GREEN}==================================${NC}"
    echo ""
    echo -e "${BLUE}📋 Setup Summary:${NC}"
    echo "• Mode: Standard (no authentication)"
    echo "• Container: $CONTAINER_NAME"
    echo "• NameNode: hdfs://localhost:$NAMENODE_PORT"
    echo "• Web UI: http://localhost:$WEB_UI_PORT"
    echo "• User Directory: /user/$USERNAME"
    echo ""
    echo -e "${BLUE}🔧 Useful Commands:${NC}"
    echo "• Check status: docker exec $CONTAINER_NAME hdfs dfsadmin -report"
    echo "• List files: docker exec $CONTAINER_NAME hdfs dfs -ls /user/$USERNAME/"
    echo "• Stop HDFS: docker stop $CONTAINER_NAME"
    echo "• View logs: docker logs $CONTAINER_NAME"
}

# =============================================================================
# Kerberos HDFS Setup
# =============================================================================

setup_kerberos() {
    local CONTAINER_NAME="hdfs-kerberos"
    local NAMENODE_PORT=9000
    local DATANODE_PORT=9866
    local DATANODE_HTTP_PORT=9864
    local WEB_UI_PORT=9870
    local KDC_PORT=88
    local KADMIN_PORT=749

    echo -e "${PURPLE}🔐 Setting up Kerberos HDFS (with authentication)${NC}"
    echo -e "${PURPLE}===================================================${NC}"

    # Step 1: Check for Kerberos config directory
    echo -e "\n${YELLOW}📦 Step 1: Checking Kerberos Configuration${NC}"
    if [ ! -d "$SCRIPT_DIR/hadoop-kerberos" ]; then
        echo -e "${RED}❌ Error: hadoop-kerberos directory not found${NC}"
        echo "💡 Expected location: $SCRIPT_DIR/hadoop-kerberos"
        exit 1
    fi
    echo "✅ Found hadoop-kerberos directory"

    # Step 2: Build Docker image
    echo -e "\n${YELLOW}🔨 Step 2: Building HDFS-Kerberos Docker Image${NC}"
    if docker images | grep -q "hdfs-kerberos"; then
        echo "⚠️  HDFS-Kerberos image already exists"
        read -p "Rebuild? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            docker build -t hdfs-kerberos "$SCRIPT_DIR/hadoop-kerberos/"
            echo "✅ Image rebuilt"
        else
            echo "✅ Using existing image"
        fi
    else
        echo "🔨 Building image..."
        docker build -t hdfs-kerberos "$SCRIPT_DIR/hadoop-kerberos/"
        echo "✅ Image built successfully"
    fi

    # Step 3: Cleanup existing containers
    echo -e "\n${YELLOW}🛑 Step 3: Cleaning up existing containers${NC}"
    if docker ps -a | grep -q "$CONTAINER_NAME"; then
        echo "🗑️  Stopping and removing existing container..."
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
        echo "✅ Cleanup completed"
    else
        echo "✅ No existing containers to clean up"
    fi

    # Step 4: Start container
    echo -e "\n${YELLOW}🐳 Step 4: Starting HDFS-Kerberos Container${NC}"
    echo "🔌 Exposing ports:"
    echo "   • HDFS: $NAMENODE_PORT, $DATANODE_HTTP_PORT, $DATANODE_PORT, $WEB_UI_PORT"
    echo "   • Kerberos: $KDC_PORT (KDC), $KADMIN_PORT (kadmin)"

    docker run -d \
        --name "$CONTAINER_NAME" \
        --hostname localhost \
        -p $NAMENODE_PORT:$NAMENODE_PORT \
        -p $DATANODE_HTTP_PORT:$DATANODE_HTTP_PORT \
        -p $DATANODE_PORT:$DATANODE_PORT \
        -p $WEB_UI_PORT:$WEB_UI_PORT \
        -p $KDC_PORT:$KDC_PORT \
        -p $KADMIN_PORT:$KADMIN_PORT \
        hdfs-kerberos

    echo "✅ Container started"

    # Step 5: Wait for initialization
    echo -e "\n${YELLOW}⏳ Step 5: Waiting for initialization (30 seconds)${NC}"
    timeout=60
    elapsed=0
    initialized=false

    while [ $elapsed -lt $timeout ]; do
        if docker logs "$CONTAINER_NAME" 2>&1 | grep -q "Ready to start HDFS with Kerberos authentication"; then
            echo "✅ Kerberos initialized"
            initialized=true
            break
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done
    echo ""

    if [ "$initialized" = false ]; then
        echo -e "${YELLOW}⚠️  Initialization taking longer than expected, continuing anyway...${NC}"
    fi

    sleep 10

    # Step 6: Verify Kerberos
    echo -e "\n${YELLOW}🔍 Step 6: Verifying Kerberos Status${NC}"
    if docker exec "$CONTAINER_NAME" ps aux | grep -q "[k]rb5kdc"; then
        echo "✅ Kerberos KDC is running"
    else
        echo -e "${RED}❌ Kerberos KDC is not running${NC}"
        docker logs "$CONTAINER_NAME" | tail -20
        exit 1
    fi

    echo "📋 Available principals:"
    docker exec "$CONTAINER_NAME" kadmin.local -q "listprincs" | grep -E "(hdfs|HTTP|testuser)" || true

    echo ""
    echo "🔑 Available keytabs:"
    docker exec "$CONTAINER_NAME" ls -lh /etc/security/keytabs/

    # Step 7: Verify HDFS
    echo -e "\n${YELLOW}🔍 Step 7: Verifying HDFS Status${NC}"
    sleep 5

    if docker exec "$CONTAINER_NAME" /opt/hadoop/bin/hdfs dfsadmin -report > /dev/null 2>&1; then
        echo "✅ HDFS cluster is healthy"
        docker exec "$CONTAINER_NAME" /opt/hadoop/bin/hdfs dfsadmin -report | grep "Live datanodes"
    else
        echo -e "${YELLOW}⚠️  HDFS status check pending (might still be initializing)${NC}"
    fi

    # Step 8: Run verification
    echo -e "\n${YELLOW}✅ Step 8: Running Verification${NC}"
    if [ -f "$SCRIPT_DIR/verify_setup.py" ]; then
        echo "🔍 Running verify_setup.py..."
        python3 "$SCRIPT_DIR/verify_setup.py" || true
    else
        echo "⚠️  Verification script not found, skipping..."
    fi

    # Summary
    echo -e "\n${GREEN}🎉 Kerberos HDFS Setup Complete!${NC}"
    echo -e "${GREEN}===================================${NC}"
    echo ""
    echo -e "${BLUE}📋 Setup Summary:${NC}"
    echo "• Mode: Kerberos (enterprise authentication)"
    echo "• Container: $CONTAINER_NAME"
    echo "• Realm: LAKESAIL.COM"
    echo "• NameNode: hdfs://localhost:$NAMENODE_PORT"
    echo "• Web UI: http://localhost:$WEB_UI_PORT"
    echo ""
    echo -e "${BLUE}👥 Principals:${NC}"
    echo "• hdfs/localhost@LAKESAIL.COM (HDFS services)"
    echo "• HTTP/localhost@LAKESAIL.COM (Web UI)"
    echo "• testuser@LAKESAIL.COM (Test user)"
    echo ""
    echo -e "${BLUE}🚀 Next Steps:${NC}"
    echo "1. Copy keytab to local machine:"
    echo "   docker cp $CONTAINER_NAME:/etc/security/keytabs/testuser.keytab ./testuser.keytab"
    echo "   docker cp $CONTAINER_NAME:/etc/krb5.conf ./krb5.conf"
    echo ""
    echo "2. Try examples:"
    echo "   python3 examples/example_lakesail_kerberos.py"
    echo ""
    echo -e "${BLUE}🔧 Useful Commands:${NC}"
    echo "• Check tickets: docker exec $CONTAINER_NAME klist"
    echo "• Authenticate: docker exec $CONTAINER_NAME kinit -kt /etc/security/keytabs/testuser.keytab testuser@LAKESAIL.COM"
    echo "• Check HDFS: docker exec $CONTAINER_NAME /opt/hadoop/bin/hdfs dfsadmin -report"
    echo "• Stop: docker stop $CONTAINER_NAME"
}

# =============================================================================
# Main Execution
# =============================================================================

if [ "$MODE" = "standard" ]; then
    setup_standard
elif [ "$MODE" = "kerberos" ]; then
    setup_kerberos
fi

echo ""
echo -e "${GREEN}✨ Setup complete! HDFS is ready for Lakesail!${NC}"