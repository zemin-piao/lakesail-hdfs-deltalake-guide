#!/bin/bash
# =============================================================================
# HDFS Startup Script with Kerberos Authentication
# =============================================================================
# This script starts HDFS services with Kerberos authentication enabled.
# =============================================================================

set -e

echo "🚀 Starting HDFS with Kerberos Authentication..."

# Configuration
KEYTAB_DIR="/etc/security/keytabs"
HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
REALM="LAKESAIL.COM"

# Auto-detect JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
    if [ -d "/usr/lib/jvm/java-11-openjdk-arm64" ]; then
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
    elif [ -d "/usr/lib/jvm/java-11-openjdk-amd64" ]; then
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    else
        echo "❌ Could not find JAVA_HOME"
        exit 1
    fi
fi

echo "☕ Using JAVA_HOME: $JAVA_HOME"

# Set Java security properties for Kerberos
export HADOOP_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf -Dsun.security.krb5.debug=true"

# Authenticate as HDFS principal before starting services
echo "🔓 Authenticating as HDFS principal..."
kinit -kt $KEYTAB_DIR/hdfs.keytab hdfs/localhost@$REALM

echo "✅ Current Kerberos tickets:"
klist

# Format namenode if needed
if [ ! -d "/hadoop/dfs/name/current" ]; then
    echo "📦 Formatting NameNode..."
    $HADOOP_HOME/bin/hdfs namenode -format -force
    echo "✅ NameNode formatted"
fi

# Start NameNode
echo "🌐 Starting NameNode..."
$HADOOP_HOME/bin/hdfs --daemon start namenode 2>&1 || echo "NameNode start returned: $?"

# Wait for NameNode to start
sleep 10

# Check if NameNode started
if ! ps aux | grep "[N]ameNode" > /dev/null; then
    echo "❌ NameNode failed to start"
    cat /opt/hadoop/logs/hadoop-*-namenode-*.log | tail -50
else
    echo "✅ NameNode started"
fi

# Start DataNode
echo "💾 Starting DataNode..."
$HADOOP_HOME/bin/hdfs --daemon start datanode 2>&1 || echo "DataNode start returned: $?"

# Wait for DataNode to start
sleep 10

# Check if DataNode started
if ! ps aux | grep "[D]ataNode" > /dev/null; then
    echo "❌ DataNode failed to start"
    cat /opt/hadoop/logs/hadoop-*-datanode-*.log | tail -50 2>/dev/null || echo "No datanode logs yet"
else
    echo "✅ DataNode started"
fi

# Check HDFS status
echo "🔍 Checking HDFS status..."
$HADOOP_HOME/bin/hdfs dfsadmin -report 2>&1 || echo "HDFS status check returned: $?"

# Create user directories with proper permissions
echo "📁 Creating user directories..."

# Create directories for different users
for user in hdfs testuser root; do
    echo "Creating directory for $user..."
    $HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/$user 2>/dev/null || echo "Directory /user/$user already exists"
    $HADOOP_HOME/bin/hdfs dfs -chown $user:supergroup /user/$user 2>/dev/null || true
    $HADOOP_HOME/bin/hdfs dfs -chmod 755 /user/$user 2>/dev/null || true
done

# Create tmp directory with world-writable permissions
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /tmp 2>/dev/null || echo "Directory /tmp already exists"
$HADOOP_HOME/bin/hdfs dfs -chmod 1777 /tmp 2>/dev/null || true

echo "✅ User directories created"

# List HDFS contents
echo "📋 HDFS directory structure:"
$HADOOP_HOME/bin/hdfs dfs -ls /

echo ""
echo "🎉 HDFS with Kerberos is running!"
echo ""
echo "📋 Connection Details:"
echo "  • NameNode: hdfs://localhost:9000"
echo "  • Web UI: http://localhost:9870"
echo "  • Authentication: Kerberos (LAKESAIL.COM)"
echo ""
echo "🔑 To authenticate as a user:"
echo "  kinit -kt $KEYTAB_DIR/testuser.keytab testuser@$REALM"
echo ""
echo "📝 To run HDFS commands:"
echo "  hdfs dfs -ls /user/testuser"
echo ""

# Keep container running and monitor HDFS
tail -f /dev/null