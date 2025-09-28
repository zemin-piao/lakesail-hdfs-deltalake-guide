#!/usr/bin/env bash

# =============================================================================
# HDFS Setup Script for Lakesail
# =============================================================================
# This script sets up an HDFS cluster for use with Lakesail.
#
# What it does:
# 1. Builds HDFS Docker image
# 2. Starts HDFS cluster with proper port configuration
# 3. Sets up proper HDFS permissions for current user
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
HDFS_CONTAINER_NAME="hdfs-working"
HDFS_NAMENODE_PORT=9000
HDFS_DATANODE_PORT=9866
HDFS_DATANODE_HTTP_PORT=9864
HDFS_WEB_UI_PORT=9870
USERNAME=$(whoami)

echo -e "${BLUE}🚀 Starting HDFS Setup for Lakesail${NC}"
echo -e "${BLUE}===================================${NC}"

# Step 1: Build HDFS Docker Image
echo -e "\n${YELLOW}📦 Step 1: Building HDFS Docker Image${NC}"
if docker images | grep -q "local-hdfs"; then
    echo "✅ HDFS Docker image already exists"
else
    echo "🔨 Building HDFS Docker image..."

    # Check if we're in the guide directory and need to reference parent sail repo
    if [ -d "../scripts/hadoop" ]; then
        echo "📁 Using HDFS scripts from parent sail repository..."
        docker build -t local-hdfs ../scripts/hadoop/
    elif [ -d "scripts/hadoop" ]; then
        echo "📁 Using HDFS scripts from current directory..."
        docker build -t local-hdfs scripts/hadoop/
    else
        echo "❌ Error: Cannot find scripts/hadoop directory"
        echo "💡 Make sure you're running this from the sail repository or have copied the guide there"
        exit 1
    fi

    echo "✅ HDFS Docker image built successfully"
fi

# Step 2: Stop existing container if running
echo -e "\n${YELLOW}🛑 Step 2: Cleaning up existing containers${NC}"
if docker ps -a | grep -q "$HDFS_CONTAINER_NAME"; then
    echo "🗑️ Stopping and removing existing HDFS container..."
    docker stop "$HDFS_CONTAINER_NAME" 2>/dev/null || true
    docker rm "$HDFS_CONTAINER_NAME" 2>/dev/null || true
    echo "✅ Cleanup completed"
else
    echo "✅ No existing containers to clean up"
fi

# Step 3: Start HDFS Container with proper port mapping
echo -e "\n${YELLOW}🐳 Step 3: Starting HDFS Container${NC}"
echo "🔌 Exposing ports: $HDFS_NAMENODE_PORT, $HDFS_DATANODE_HTTP_PORT, $HDFS_DATANODE_PORT, $HDFS_WEB_UI_PORT"
docker run -d \
    --name "$HDFS_CONTAINER_NAME" \
    --rm \
    -p $HDFS_NAMENODE_PORT:$HDFS_NAMENODE_PORT \
    -p $HDFS_DATANODE_HTTP_PORT:$HDFS_DATANODE_HTTP_PORT \
    -p $HDFS_DATANODE_PORT:$HDFS_DATANODE_PORT \
    -p $HDFS_WEB_UI_PORT:$HDFS_WEB_UI_PORT \
    local-hdfs

echo "⏳ Waiting for HDFS to initialize (15 seconds)..."
sleep 15

# Step 4: Verify HDFS is running
echo -e "\n${YELLOW}🔍 Step 4: Verifying HDFS Status${NC}"
if docker exec "$HDFS_CONTAINER_NAME" hdfs dfsadmin -report > /dev/null 2>&1; then
    echo "✅ HDFS cluster is healthy"
    docker exec "$HDFS_CONTAINER_NAME" hdfs dfsadmin -report | grep "Live datanodes"
else
    echo -e "${RED}❌ HDFS cluster failed to start${NC}"
    exit 1
fi

# Step 5: Set up HDFS Permissions
echo -e "\n${YELLOW}🔐 Step 5: Setting up HDFS Permissions${NC}"
echo "👤 Creating user directory for: $USERNAME"

# Create user home directory
docker exec "$HDFS_CONTAINER_NAME" hdfs dfs -mkdir -p "/user/$USERNAME"

# Set ownership (note: this might show a warning but should work)
docker exec "$HDFS_CONTAINER_NAME" hdfs dfs -chown "$USERNAME:supergroup" "/user/$USERNAME" 2>/dev/null || true

# Set permissions
docker exec "$HDFS_CONTAINER_NAME" hdfs dfs -chmod 755 "/user/$USERNAME"

echo "✅ HDFS permissions configured"
docker exec "$HDFS_CONTAINER_NAME" hdfs dfs -ls /user/

echo -e "\n${GREEN}🎉 HDFS Setup Complete!${NC}"
echo -e "${GREEN}=======================${NC}"
echo ""
echo -e "${BLUE}📋 Setup Summary:${NC}"
echo "• HDFS Cluster: ✅ Running and healthy"
echo "• Container Name: $HDFS_CONTAINER_NAME"
echo "• Namenode: hdfs://localhost:$HDFS_NAMENODE_PORT"
echo "• Web UI: http://localhost:$HDFS_WEB_UI_PORT"
echo "• User Directory: /user/$USERNAME (ready for writes)"
echo ""
echo -e "${BLUE}🚀 Next Steps:${NC}"
echo "1. Start Lakesail server:"
echo "   python3 -c \"from pysail.spark import SparkConnectServer; server = SparkConnectServer(port=50051); server.start(background=False)\""
echo ""
echo "2. Connect from PySpark client:"
echo "   from pyspark.sql import SparkSession"
echo "   spark = SparkSession.builder.remote('sc://localhost:50051').getOrCreate()"
echo ""
echo "3. Create data using Lakesail (example):"
echo "   df = spark.range(10).withColumn('name', spark.sql('SELECT concat(\"user_\", id) as name').collect()[0][0])"
echo "   df.write.format('parquet').save('hdfs://localhost:9000/user/$USERNAME/my_data')"
echo ""
echo -e "${BLUE}🔧 Useful Commands:${NC}"
echo "• Check HDFS status: docker exec $HDFS_CONTAINER_NAME hdfs dfsadmin -report"
echo "• List HDFS files: docker exec $HDFS_CONTAINER_NAME hdfs dfs -ls /user/$USERNAME/"
echo "• Stop HDFS: docker stop $HDFS_CONTAINER_NAME"
echo "• View HDFS logs: docker logs $HDFS_CONTAINER_NAME"
echo ""
echo -e "${GREEN}✨ HDFS ready for Lakesail!${NC}"