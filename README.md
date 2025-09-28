# Lakesail + HDFS + Delta Lake Setup Guide

Complete setup and verification of **Lakesail** (Sail's Spark Connect server) with **HDFS storage** and **Delta Lake format** tables.

## 📋 Prerequisites

- **Git** for cloning repositories
- **Python 3.8+** with uv/pip
- **Docker** for HDFS setup
- **macOS/Linux** environment

## 🚀 Setup Steps

### Step 1: Clone Sail Repository

```bash
# Clone the Sail repository (needed for HDFS Docker setup)
git clone https://github.com/lakehq/sail.git
cd sail
```

### Step 2: Install Dependencies

```bash
# Install required Python packages
uv pip install pyspark
uv pip install pysail

# Verify installation
python3 -c "from pysail.spark import SparkConnectServer; print('✅ pysail installed successfully')"
```

### Step 3: Set Up HDFS Infrastructure

```bash
# Download or copy the lakesail-hdfs-guide to your sail directory
# If you have the guide files, copy them:
# cp -r /path/to/lakesail-hdfs-guide .

# Navigate to the guide directory (should be inside the sail repo)
cd lakesail-hdfs-guide

# Run the automated HDFS setup script (automatically finds ../scripts/hadoop/)
./setup_lakesail_hdfs.sh
```

**What this script does:**
- Builds HDFS Docker image from Sail's Hadoop configuration
- Starts HDFS cluster with proper port mapping (9000, 9864, 9866, 9870)
- Creates user directory in HDFS with proper permissions
- Verifies HDFS cluster health

### Step 4: Start Lakesail Server

```bash
# Start Lakesail Spark Connect server (keep this running)
python3 start_lakesail_server.py
```

**Expected output:**
```
🚀 Starting Lakesail Spark Connect Server...
📡 Server will be available at: sc://localhost:50051
🌐 Press Ctrl+C to stop
✅ Server created successfully
```

### Step 5: Test the Complete Setup

Open a new terminal and run:

```bash
# Complete system verification
python3 verify_complete_setup.py
```

## ✅ Success Criteria

When everything works correctly, you should see:

```
🔍 Complete Lakesail + HDFS + Delta Lake Verification
============================================================

1️⃣ Verifying HDFS Status...
✅ HDFS cluster is healthy
   Live datanodes (1):

2️⃣ Verifying Lakesail Connection...
✅ Connected to Lakesail successfully
   Spark Version: 3.5.3
   Using Spark Connect: True
   Connection URL: sc://localhost:50051
   🎯 Connected to Lakesail on port 50051!

3️⃣ Testing Basic SQL Functionality...
✅ Basic SQL test passed: Row(result=2, status='Lakesail + HDFS Working!')

4️⃣ Testing HDFS Connectivity...
✅ HDFS write/read test passed: 1000 records

5️⃣ Testing Data Creation and Reading...
✅ Data written in 0.123s
✅ Data read back: 1000 records in 0.045s
✅ Query completed: 250 records in 0.067s

6️⃣ Testing Delta Lake Capabilities...
✅ Delta Lake write successful
✅ Delta Lake read successful: 100 records

7️⃣ Testing Advanced SQL Features...
✅ Complex SQL query completed: 10 results in 0.089s

8️⃣ Performance Analysis...
✅ Performance benchmark: 1000 groups processed in 0.156s

9️⃣ Testing Resource Management...
✅ Active sessions: 1
✅ Cache cleared successfully
✅ Available databases: 1

🔍 Final System Verification...
✅ HDFS storage usage:
     128K  /user/your_username/verification_data/

============================================================
🎉 COMPLETE VERIFICATION FINISHED!
============================================================

✅ SUCCESSFUL COMPONENTS:
   🚀 Lakesail Spark Connect Server
   🗄️  HDFS Distributed Storage
   📊 Data Creation and Reading
   🔍 SQL Query Execution
   ⚡ Performance Testing
   🔧 Configuration Management

🎯 YOUR LAKESAIL + HDFS SETUP IS FULLY OPERATIONAL!
```

## 🧪 Additional Testing (Optional)

### Create Sample Data on HDFS
```bash
python3 create_deltalake_hdfs.py
```

**Expected output:**
- Creates employee and transaction datasets
- Writes data to HDFS in Parquet and Delta Lake formats
- Shows successful data creation with record counts

### Read and Analyze Data
```bash
python3 read_deltalake_hdfs.py
```

**Expected output:**
- Reads data from HDFS successfully
- Performs complex SQL analytics (joins, aggregations, window functions)
- Demonstrates cross-table queries and performance metrics

## 🔍 Verification Tools

### Check Lakesail Connection
```bash
python3 verify_sail_connection.py
```

## 🛠️ Troubleshooting

### HDFS Issues
```bash
# Check HDFS status
docker exec hdfs-working hdfs dfsadmin -report

# Check container is running
docker ps | grep hdfs-working
```

### Lakesail Issues
```bash
# Check server is running
lsof -i :50051

# Verify connection
python3 verify_sail_connection.py
```

### Permission Issues
```bash
# Fix HDFS permissions
docker exec hdfs-working hdfs dfs -chmod 777 /user/$(whoami)
```

## 📁 What You Get

After successful setup:

- **🚀 Lakesail server** running on port 50051
- **🗄️ HDFS cluster** with distributed storage
- **📊 Sample data** in Parquet and Delta Lake formats
- **🔍 Working SQL queries** on distributed data
- **⚡ Performance verification** of Lakesail's speed advantages
- **🎯 Production-ready foundation** for data lakehouse architecture

## 🎉 Integration Success

When all steps complete successfully, you have demonstrated:

✅ **Complete integration** between Lakesail, HDFS, and Delta Lake
✅ **High-performance SQL execution** on distributed storage
✅ **Spark Connect compatibility** with modern data tools
✅ **Scalable data lakehouse** architecture ready for production use

**You're now ready to build advanced data pipelines with Lakesail!** 🚀