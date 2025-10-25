#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
import os

def create_deltalake_tables():
    print("🔌 Connecting to Lakesail server...")

    # Connect to Lakesail
    spark = SparkSession.builder \
        .appName("LakesailDeltaLakeHDFS") \
        .remote("sc://localhost:50051") \
        .getOrCreate()

    print("✅ Connected to Lakesail successfully!")

    # Verify connection details
    print(f"📊 Spark Version: {spark.version}")
    print(f"🔗 Using Spark Connect: {hasattr(spark, '_client')}")

    if hasattr(spark, '_client'):
        url = getattr(spark._client, '_url', 'Unknown')
        print(f"🌐 Connection URL: {url}")

    # Test basic functionality
    print("\n🧮 Testing basic SQL functionality:")
    test_df = spark.sql("SELECT 1 + 1 as result, 'Hello from Lakesail + HDFS!' as message")
    test_df.show()

    # Create employee data
    print("\n📊 Creating employee dataset...")
    employee_data = [
        (1, "Alice Johnson", 28, "Engineering", 85000.0, "New York"),
        (2, "Bob Smith", 32, "Marketing", 72000.0, "San Francisco"),
        (3, "Charlie Brown", 29, "Engineering", 90000.0, "Seattle"),
        (4, "Diana Prince", 35, "Sales", 78000.0, "Chicago"),
        (5, "Eve Wilson", 26, "HR", 65000.0, "Boston"),
        (6, "Frank Miller", 31, "Engineering", 88000.0, "Austin"),
        (7, "Grace Lee", 27, "Marketing", 71000.0, "Denver"),
        (8, "Henry Davis", 33, "Sales", 76000.0, "Miami"),
        (9, "Ivy Chen", 30, "Engineering", 92000.0, "Portland"),
        (10, "Jack Taylor", 34, "HR", 67000.0, "Los Angeles")
    ]

    employee_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("city", StringType(), True)
    ])

    employees_df = spark.createDataFrame(employee_data, employee_schema)
    print("✅ Employee DataFrame created")
    employees_df.show()

    # Create transaction data
    print("\n💰 Creating transaction dataset...")
    transaction_data = [
        (1, 1, 1250.50, "2024-01-15", "expense"),
        (2, 2, 850.75, "2024-01-16", "expense"),
        (3, 1, 2100.00, "2024-01-17", "bonus"),
        (4, 3, 675.25, "2024-01-18", "expense"),
        (5, 4, 1450.80, "2024-01-19", "expense"),
        (6, 2, 920.40, "2024-01-20", "bonus"),
        (7, 5, 1800.00, "2024-01-21", "expense"),
        (8, 3, 1150.60, "2024-01-22", "expense"),
        (9, 6, 890.30, "2024-01-23", "expense"),
        (10, 7, 1675.25, "2024-01-24", "bonus")
    ]

    transaction_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("employee_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("type", StringType(), True)
    ])

    transactions_df = spark.createDataFrame(transaction_data, transaction_schema)
    print("✅ Transaction DataFrame created")
    transactions_df.show()

    # Get current user for HDFS path
    username = os.getenv('USER') or os.getenv('USERNAME') or 'user'
    hdfs_base_path = f"hdfs://localhost:9000/user/{username}"

    print(f"\n💾 Writing data to HDFS at: {hdfs_base_path}")

    # Write employees table as Parquet
    employees_path = f"{hdfs_base_path}/employees_parquet"
    print(f"📁 Writing employees to: {employees_path}")

    employees_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(employees_path)

    print("✅ Employees table written to HDFS")

    # Write transactions table as Parquet
    transactions_path = f"{hdfs_base_path}/transactions_parquet"
    print(f"📁 Writing transactions to: {transactions_path}")

    transactions_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(transactions_path)

    print("✅ Transactions table written to HDFS")

    # Try Delta Lake format (if supported)
    print("\n🔺 Attempting Delta Lake format...")
    try:
        delta_employees_path = f"{hdfs_base_path}/employees_delta"
        employees_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_employees_path)
        print(f"✅ Delta Lake employees table written to: {delta_employees_path}")

        delta_transactions_path = f"{hdfs_base_path}/transactions_delta"
        transactions_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(delta_transactions_path)
        print(f"✅ Delta Lake transactions table written to: {delta_transactions_path}")

    except Exception as e:
        print(f"⚠️  Delta Lake format not available: {e}")
        print("💡 Using Parquet format instead")

    # Create a partitioned table
    print("\n📂 Creating partitioned table...")
    try:
        partitioned_path = f"{hdfs_base_path}/employees_partitioned"
        employees_df.write \
            .format("parquet") \
            .mode("overwrite") \
            .partitionBy("department") \
            .save(partitioned_path)
        print(f"✅ Partitioned table written to: {partitioned_path}")
    except Exception as e:
        print(f"⚠️  Partitioned table creation failed: {e}")

    # Verify written data by listing HDFS contents
    print(f"\n📋 Verifying data written to HDFS...")
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "exec", "hdfs-working", "hdfs", "dfs", "-ls", f"/user/{username}/"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            print("📁 HDFS directory contents:")
            print(result.stdout)
        else:
            print(f"⚠️  Could not list HDFS contents: {result.stderr}")
    except Exception as e:
        print(f"⚠️  Could not verify HDFS contents: {e}")

    print("\n🎉 Data creation completed successfully!")
    print("\n📋 Summary of created tables:")
    print(f"   📊 Employees (Parquet): {employees_path}")
    print(f"   💰 Transactions (Parquet): {transactions_path}")
    print(f"   📂 Employees (Partitioned): {hdfs_base_path}/employees_partitioned")
    print(f"   🔺 Delta tables: Check output above for availability")

    spark.stop()

if __name__ == "__main__":
    try:
        create_deltalake_tables()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()