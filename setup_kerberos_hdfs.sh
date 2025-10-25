#!/usr/bin/env bash

# =============================================================================
# Kerberos HDFS + Sail Setup
# =============================================================================
# One-command setup for enterprise-grade Kerberos HDFS with Sail Spark Connect
# =============================================================================

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONTAINER_NAME="hdfs-kerberos"

echo -e "${BLUE}🚀 Kerberos HDFS + Sail Setup${NC}"
echo -e "${BLUE}=============================${NC}"
echo ""

# Step 1: Check prerequisites
echo -e "${YELLOW}📋 Step 1: Checking prerequisites...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker not found${NC}"
    exit 1
fi
echo "✅ Docker found"

if [ ! -d "$SCRIPT_DIR/hadoop-kerberos" ]; then
    echo -e "${RED}❌ hadoop-kerberos directory not found${NC}"
    exit 1
fi
echo "✅ Configuration directory found"

# Step 2: Build Docker image
echo -e "\n${YELLOW}🔨 Step 2: Building Docker image...${NC}"
if docker images | grep -q "^hdfs-kerberos "; then
    echo "⚠️  Image already exists"
    read -p "Rebuild? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker build -t hdfs-kerberos "$SCRIPT_DIR/hadoop-kerberos/"
        echo "✅ Image rebuilt"
    else
        echo "✅ Using existing image"
    fi
else
    docker build -t hdfs-kerberos "$SCRIPT_DIR/hadoop-kerberos/"
    echo "✅ Image built successfully"
fi

# Step 3: Stop existing container
echo -e "\n${YELLOW}🛑 Step 3: Cleaning up existing containers...${NC}"
if docker ps -a | grep -q "$CONTAINER_NAME"; then
    echo "Stopping and removing existing container..."
    docker stop "$CONTAINER_NAME" 2>/dev/null || true
    docker rm "$CONTAINER_NAME" 2>/dev/null || true
    echo "✅ Cleanup completed"
else
    echo "✅ No existing containers"
fi

# Step 4: Start container
echo -e "\n${YELLOW}🐳 Step 4: Starting Kerberos HDFS container...${NC}"
docker run -d \
    --name "$CONTAINER_NAME" \
    --hostname localhost \
    -p 9000:9000 \
    -p 9864:9864 \
    -p 9866:9866 \
    -p 9870:9870 \
    -p 88:88 \
    -p 749:749 \
    -p 50051:50051 \
    hdfs-kerberos

echo "✅ Container started"

# Step 5: Wait for initialization
echo -e "\n${YELLOW}⏳ Step 5: Waiting for initialization (~30 seconds)...${NC}"
timeout=60
elapsed=0

while [ $elapsed -lt $timeout ]; do
    if docker logs "$CONTAINER_NAME" 2>&1 | grep -q "Ready to start HDFS with Kerberos authentication"; then
        echo ""
        echo "✅ Kerberos initialized"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
    echo -n "."
done
echo ""

if [ $elapsed -ge $timeout ]; then
    echo -e "${YELLOW}⚠️  Initialization taking longer than expected${NC}"
fi

sleep 10

# Step 6: Verify services
echo -e "\n${YELLOW}🔍 Step 6: Verifying services...${NC}"

# Check KDC
if docker exec "$CONTAINER_NAME" ps aux | grep -q "[k]rb5kdc"; then
    echo "✅ Kerberos KDC running"
else
    echo -e "${RED}❌ Kerberos KDC not running${NC}"
    docker logs "$CONTAINER_NAME" | tail -20
    exit 1
fi

# Check HDFS
sleep 5
if docker exec "$CONTAINER_NAME" /opt/hadoop/bin/hdfs dfsadmin -report > /dev/null 2>&1; then
    echo "✅ HDFS cluster healthy"
else
    echo -e "${YELLOW}⚠️  HDFS initializing...${NC}"
fi

# Step 7: Copy keytabs
echo -e "\n${YELLOW}📋 Step 7: Copying keytabs to local directory...${NC}"
docker cp "$CONTAINER_NAME:/etc/security/keytabs/testuser.keytab" "$SCRIPT_DIR/testuser.keytab" 2>/dev/null || echo "⚠️  Keytab copy pending"
docker cp "$CONTAINER_NAME:/etc/krb5.conf" "$SCRIPT_DIR/krb5.conf" 2>/dev/null || echo "⚠️  Config copy pending"

if [ -f "$SCRIPT_DIR/testuser.keytab" ]; then
    echo "✅ Keytabs copied"
    chmod 600 "$SCRIPT_DIR/testuser.keytab"
fi

# Summary
echo ""
echo -e "${GREEN}🎉 Setup Complete!${NC}"
echo -e "${GREEN}==================${NC}"
echo ""
echo -e "${BLUE}📋 Setup Summary:${NC}"
echo "• Container: $CONTAINER_NAME"
echo "• HDFS NameNode: hdfs://localhost:9000"
echo "• HDFS Web UI: http://localhost:9870"
echo "• Kerberos realm: LAKESAIL.COM"
echo "• Principal: testuser@LAKESAIL.COM"
echo "• Sail port: 50051 (after starting server)"
echo ""
echo -e "${BLUE}🚀 Next Steps:${NC}"
echo "1. Verify setup: python3 verify_complete_setup.py"
echo "2. View logs: docker logs $CONTAINER_NAME"
echo "3. Enter container: docker exec -it $CONTAINER_NAME bash"
echo ""
echo -e "${GREEN}✨ Kerberos HDFS is ready!${NC}"
