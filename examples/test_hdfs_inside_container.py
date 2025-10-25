#!/usr/bin/env python3
"""
Test HDFS Kerberos - Runs INSIDE the hdfs-kerberos container

This script should be run inside the Docker container where Kerberos authentication works.

Usage:
1. Copy this script to the container:
   docker cp examples/test_hdfs_inside_container.py hdfs-kerberos:/tmp/

2. Enter the container:
   docker exec -it hdfs-kerberos bash

3. Authenticate with Kerberos:
   kinit -kt /etc/security/keytabs/testuser.keytab testuser@LAKESAIL.COM

4. Run the script:
   python3 /tmp/test_hdfs_inside_container.py
"""

from pyspark.sql import SparkSession
import os

def main():
    print("="*60)
    print("Testing Kerberos HDFS from INSIDE Container")
    print("="*60)

    # Set Kerberos config
    os.environ['KRB5_CONFIG'] = '/etc/krb5.conf'

    # Create Spark session with Kerberos
    print("\n🚀 Creating Spark session with Kerberos...")
    spark = SparkSession.builder \
        .appName("KerberosHDFSTest") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.krb5.conf=/etc/krb5.conf") \
        .config("spark.hadoop.hadoop.security.authentication", "kerberos") \
        .config("spark.hadoop.hadoop.security.authorization", "true") \
        .config("spark.kerberos.keytab", "/etc/security/keytabs/testuser.keytab") \
        .config("spark.kerberos.principal", "testuser@LAKESAIL.COM") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    print(f"✅ Spark session created (version: {spark.version})")

    # Test 1: Write to HDFS
    print("\n📝 Test 1: Writing data to Kerberos HDFS...")
    data = [
        (1, "Alice", 85000),
        (2, "Bob", 92000),
        (3, "Charlie", 78000)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary"])

    hdfs_path = "hdfs://localhost:9000/user/testuser/spark_kerberos_test"
    df.write.mode("overwrite").parquet(hdfs_path)
    print(f"✅ Successfully wrote data to {hdfs_path}")

    # Test 2: Read from HDFS
    print("\n📖 Test 2: Reading data from Kerberos HDFS...")
    df_read = spark.read.parquet(hdfs_path)
    print("✅ Successfully read data:")
    df_read.show()

    # Test 3: SQL Query
    print("\n📊 Test 3: Running SQL query...")
    df_read.createOrReplaceTempView("employees")
    result = spark.sql("""
        SELECT
            name,
            salary,
            CASE
                WHEN salary > 80000 THEN 'High'
                ELSE 'Standard'
            END as salary_band
        FROM employees
        ORDER BY salary DESC
    """)
    print("✅ SQL query result:")
    result.show()

    print("\n" + "="*60)
    print("🎉 ALL TESTS PASSED!")
    print("="*60)
    print("\n✅ Kerberos authentication: SUCCESS")
    print("✅ HDFS write operations: SUCCESS")
    print("✅ HDFS read operations: SUCCESS")
    print("✅ SQL queries: SUCCESS")

    spark.stop()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)