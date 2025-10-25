#!/bin/bash
# =============================================================================
# Kerberos Initialization Script for HDFS
# =============================================================================
# This script initializes Kerberos KDC and creates principals and keytabs
# for HDFS services.
# =============================================================================

set -e

echo "🔐 Initializing Kerberos for HDFS..."

# Configuration
REALM="LAKESAIL.COM"
KDC_PASSWORD="lakesail123"
KEYTAB_DIR="/etc/security/keytabs"
HOSTNAME=$(hostname -f)

# Create keytab directory
mkdir -p $KEYTAB_DIR
chmod 755 $KEYTAB_DIR

# Initialize KDC database
echo "📦 Creating KDC database..."
if [ ! -f /var/kerberos/krb5kdc/principal ]; then
    echo -e "$KDC_PASSWORD\n$KDC_PASSWORD" | kdb5_util create -s -r $REALM
    echo "✅ KDC database created"
else
    echo "✅ KDC database already exists"
fi

# Start KDC and kadmin services
echo "🚀 Starting Kerberos services..."
krb5kdc &
kadmind &

# Wait for services to start
sleep 3

# Create admin principal
echo "👤 Creating admin principal..."
echo -e "$KDC_PASSWORD\n$KDC_PASSWORD" | kadmin.local -q "addprinc admin/admin@$REALM" 2>/dev/null || echo "Admin principal already exists"

# Create HDFS service principals
echo "🔑 Creating HDFS service principals..."

# HDFS principal for NameNode and DataNode
kadmin.local -q "addprinc -randkey hdfs/localhost@$REALM" 2>/dev/null || echo "hdfs/localhost principal exists"
kadmin.local -q "addprinc -randkey hdfs/$HOSTNAME@$REALM" 2>/dev/null || echo "hdfs/$HOSTNAME principal exists"

# HTTP principal for web UI
kadmin.local -q "addprinc -randkey HTTP/localhost@$REALM" 2>/dev/null || echo "HTTP/localhost principal exists"
kadmin.local -q "addprinc -randkey HTTP/$HOSTNAME@$REALM" 2>/dev/null || echo "HTTP/$HOSTNAME principal exists"

# Create user principals for testing
echo "👥 Creating user principals..."
kadmin.local -q "addprinc -randkey hdfs@$REALM" 2>/dev/null || echo "hdfs principal exists"
kadmin.local -q "addprinc -randkey testuser@$REALM" 2>/dev/null || echo "testuser principal exists"
echo -e "testpass\ntestpass" | kadmin.local -q "addprinc testuser@$REALM" 2>/dev/null || echo "testuser with password exists"

# Generate keytabs
echo "🎫 Generating keytabs..."

# HDFS keytab
kadmin.local -q "xst -norandkey -k $KEYTAB_DIR/hdfs.keytab hdfs/localhost@$REALM hdfs/$HOSTNAME@$REALM hdfs@$REALM"

# HTTP/SPNEGO keytab
kadmin.local -q "xst -norandkey -k $KEYTAB_DIR/spnego.service.keytab HTTP/localhost@$REALM HTTP/$HOSTNAME@$REALM"

# Test user keytab
kadmin.local -q "xst -norandkey -k $KEYTAB_DIR/testuser.keytab testuser@$REALM"

# Set proper permissions
chmod 644 $KEYTAB_DIR/*.keytab

echo "✅ Keytabs created:"
ls -lh $KEYTAB_DIR/

# Test keytab validity
echo "🧪 Testing keytab validity..."
klist -kt $KEYTAB_DIR/hdfs.keytab
klist -kt $KEYTAB_DIR/spnego.service.keytab

# Authenticate as HDFS principal
echo "🔓 Authenticating as HDFS principal..."
kinit -kt $KEYTAB_DIR/hdfs.keytab hdfs/localhost@$REALM

# Verify authentication
echo "✅ Current Kerberos tickets:"
klist

echo "🎉 Kerberos initialization completed successfully!"
echo ""
echo "📋 Available Principals:"
kadmin.local -q "listprincs" | grep -E "(hdfs|HTTP|testuser)"

echo ""
echo "🔑 Available Keytabs:"
echo "  • HDFS: $KEYTAB_DIR/hdfs.keytab"
echo "  • SPNEGO: $KEYTAB_DIR/spnego.service.keytab"
echo "  • TestUser: $KEYTAB_DIR/testuser.keytab"
echo ""
echo "🚀 Ready to start HDFS with Kerberos authentication!"