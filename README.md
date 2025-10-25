# Kerberos HDFS + Sail + Delta Lake Setup

Complete enterprise-grade data lakehouse with **Kerberos HDFS**, **Sail Spark Connect**, and **Delta Lake**.

## 🚀 Quick Start

```bash
# 1. Setup HDFS with Kerberos
./setup_kerberos_hdfs.sh

# 2. Verify everything works
python3 verify_complete_setup.py
```

That's it! You now have a production-ready Kerberos data lakehouse.

---

## 📋 What You Get

- ✅ **Kerberos HDFS**: Enterprise authentication and authorization
- ✅ **Sail Spark Connect**: High-performance query engine on port 50051
- ✅ **Delta Lake**: ACID transactions on distributed storage
- ✅ **Automated Setup**: One command to deploy everything
- ✅ **Comprehensive Verification**: Tests all components together

---

## 🔧 Prerequisites

- **Docker** installed and running
- **Python 3.8+** with pip
- **macOS/Linux** (tested on macOS)

```bash
# Install Python dependencies
pip install pyspark pysail
```

---

## 📖 Detailed Setup

### Step 1: Build and Start Kerberos HDFS

```bash
./setup_kerberos_hdfs.sh
```

**What this does:**
1. Builds Docker image with Hadoop 3.3.6 + Kerberos
2. Starts HDFS with Kerberos authentication (realm: LAKESAIL.COM)
3. Creates principals: `hdfs`, `HTTP`, `testuser@LAKESAIL.COM`
4. Generates keytabs in `/etc/security/keytabs/`
5. Copies keytabs to local directory

**Ports exposed:**
- 9000: HDFS NameNode
- 9870: HDFS Web UI
- 88: Kerberos KDC
- 749: Kerberos kadmin
- 50051: Sail Spark Connect (after starting server)

### Step 2: Verify Complete Setup

```bash
python3 verify_complete_setup.py
```

**What this verifies:**
1. ✅ Kerberos KDC is running
2. ✅ Principals and keytabs are valid
3. ✅ HDFS cluster is healthy with Kerberos auth
4. ✅ Sail server starts successfully
5. ✅ Can write/read data to Kerberos HDFS
6. ✅ Delta Lake operations work
7. ✅ SQL queries execute correctly

**Expected output:**
```
🔍 Complete Setup Verification
============================================================
✅ Kerberos KDC running
✅ Authentication successful
✅ HDFS cluster healthy
✅ Sail server started on port 50051
✅ Delta Lake write/read successful
✅ SQL queries working
🎉 ALL SYSTEMS OPERATIONAL!
```

---

## 💻 Usage

### Connect to Sail from Python

```python
from pyspark.sql import SparkSession

# Connect to Sail server
spark = SparkSession.builder \
    .remote("sc://localhost:50051") \
    .getOrCreate()

# Write to Kerberos HDFS
df = spark.range(1000)
df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/testuser/data")

# Read back
result = spark.read.parquet("hdfs://localhost:9000/user/testuser/data")
print(f"✅ Records: {result.count()}")

# Delta Lake
df.write.format("delta").mode("overwrite").save("hdfs://localhost:9000/user/testuser/delta_table")
```

### Run SQL Queries

```python
# Register table
df.createOrReplaceTempView("my_table")

# Query
result = spark.sql("""
    SELECT id, COUNT(*) as count
    FROM my_table
    GROUP BY id
""")
result.show()
```

---

## 🔍 Architecture

```
┌─────────────────────────────────────────────────┐
│          Docker Container (hdfs-kerberos)       │
│                                                 │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐  │
│  │ Kerberos │   │   HDFS   │   │   Sail   │  │
│  │   KDC    │──→│ NameNode │←──│  Server  │  │
│  │ port 88  │   │ port 9000│   │port 50051│  │
│  └──────────┘   └──────────┘   └──────────┘  │
│                       │                         │
└───────────────────────┼─────────────────────────┘
                        │
                   Delta Lake
              (ACID transactions)
```

**Authentication Flow:**
1. Sail server authenticates with Kerberos (kinit)
2. Gets ticket from KDC
3. Uses ticket to access HDFS
4. Clients connect to Sail (no Kerberos needed on client)

---

## 🛠️ Management

### Start/Stop Services

```bash
# Start container
docker start hdfs-kerberos

# Stop container
docker stop hdfs-kerberos

# View logs
docker logs hdfs-kerberos

# Enter container
docker exec -it hdfs-kerberos bash
```

### HDFS Operations (Inside Container)

```bash
# Enter container
docker exec -it hdfs-kerberos bash

# Authenticate
kinit -kt /etc/security/keytabs/testuser.keytab testuser@LAKESAIL.COM

# HDFS commands
hdfs dfs -ls /user/testuser
hdfs dfs -put localfile /user/testuser/
hdfs dfs -cat /user/testuser/file
```

### Check Kerberos Status

```bash
# Inside container
docker exec hdfs-kerberos klist

# View principals
docker exec hdfs-kerberos kadmin.local -q "listprincs"

# Check KDC is running
docker exec hdfs-kerberos ps aux | grep krb5kdc
```

---

## 🐛 Troubleshooting

### Container won't start
```bash
# Check logs
docker logs hdfs-kerberos

# Restart
docker restart hdfs-kerberos
```

### HDFS operations fail
```bash
# Check HDFS health
docker exec hdfs-kerberos /opt/hadoop/bin/hdfs dfsadmin -report

# Verify authentication
docker exec hdfs-kerberos kinit -kt /etc/security/keytabs/testuser.keytab testuser@LAKESAIL.COM
docker exec hdfs-kerberos klist
```

### Sail server won't start
```bash
# Check if port is in use
lsof -i :50051

# Kill existing process
kill -9 $(lsof -t -i:50051)

# Try manual start (inside container)
docker exec -it hdfs-kerberos bash
kinit -kt /etc/security/keytabs/testuser.keytab testuser@LAKESAIL.COM
python3 -c "from pysail.spark import SparkConnectServer; SparkConnectServer(port=50051).start()"
```

### Kerberos authentication fails
```bash
# Verify keytab
docker exec hdfs-kerberos klist -kt /etc/security/keytabs/testuser.keytab

# Check KDC
docker exec hdfs-kerberos ps aux | grep krb5kdc

# Restart container if needed
docker restart hdfs-kerberos
```

---

## 🧹 Cleanup

```bash
# Stop and remove container
docker stop hdfs-kerberos
docker rm hdfs-kerberos

# Remove image
docker rmi hdfs-kerberos

# Remove local files
rm -f testuser.keytab krb5.conf
```

---

## 📚 File Structure

```
lakesail-hdfs-deltalake-guide/
├── README.md                        # This file
├── setup_kerberos_hdfs.sh          # One-command setup
├── verify_complete_setup.py        # Comprehensive verification
├── hadoop-kerberos/                # Docker configuration
│   ├── Dockerfile                  # Ubuntu + Hadoop + Kerberos
│   ├── config/                     # Kerberos & HDFS configs
│   ├── init-kerberos.sh            # KDC initialization
│   └── start-hdfs-kerberos.sh      # HDFS startup script
└── examples/                       # Usage examples (optional)
    └── example_usage.py            # Sample code
```

---

## 🎓 What This Setup Provides

### Enterprise Features
- **Authentication**: Kerberos SSO integration
- **Authorization**: HDFS permissions and ACLs
- **Encryption**: Secure communication (can add SSL/TLS)
- **Auditing**: HDFS audit logs

### Data Platform Capabilities
- **Spark SQL**: Standard SQL queries via Sail
- **Delta Lake**: ACID transactions, time travel, schema evolution
- **Distributed Storage**: Scalable HDFS backend
- **High Performance**: Optimized query execution

### Production Readiness
- **Automated deployment**: One command setup
- **Comprehensive testing**: Full stack verification
- **Monitoring**: HDFS Web UI at http://localhost:9870
- **Operational commands**: Start/stop/restart services

---

## 🚀 Next Steps

1. **Scale up**: Add more DataNodes for redundancy
2. **Add SSL/TLS**: Encrypt data in transit
3. **Integrate with existing Kerberos**: Use your corporate KDC
4. **Set up monitoring**: Add Prometheus/Grafana
5. **Configure backups**: Implement HDFS snapshot strategy
6. **Deploy to production**: Use orchestration (K8s, Docker Swarm)

---

## 📖 Additional Resources

- **Hadoop Security**: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SecureMode.html
- **Kerberos**: https://web.mit.edu/kerberos/
- **Delta Lake**: https://docs.delta.io/
- **Sail**: https://github.com/lakehq/sail

---

## ✅ Success Criteria

Your setup is working when:

- ✅ `python3 verify_complete_setup.py` passes all checks
- ✅ HDFS Web UI accessible at http://localhost:9870
- ✅ Can connect to Sail on `sc://localhost:50051`
- ✅ Can write/read Delta Lake tables
- ✅ SQL queries execute successfully

**You're now ready for enterprise Spark + HDFS + Delta Lake workloads!** 🎉
