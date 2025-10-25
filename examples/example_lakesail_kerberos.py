#!/usr/bin/env python3
"""
Example: Using Lakesail with Kerberos-Enabled HDFS

This script demonstrates how to:
1. Connect to Lakesail with Kerberos authentication
2. Write data to secure HDFS
3. Read data back
4. Perform SQL queries on secure data
5. Create Delta Lake tables on Kerberos HDFS

Prerequisites:
- Kerberos HDFS container running (hdfs-kerberos)
- Lakesail server running (optional, can use local Spark)
- Keytab and krb5.conf copied to current directory
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count
import os
import sys

def setup_kerberos_environment():
    """Set up Kerberos environment variables"""
    print("🔐 Setting up Kerberos environment...")

    # Set Kerberos configuration
    krb5_conf_path = os.path.join(os.getcwd(), 'krb5.conf')
    if not os.path.exists(krb5_conf_path):
        print("❌ krb5.conf not found. Run:")
        print("   docker cp hdfs-kerberos:/etc/krb5.conf ./krb5.conf")
        sys.exit(1)

    os.environ['KRB5_CONFIG'] = krb5_conf_path
    print(f"✅ KRB5_CONFIG set to: {krb5_conf_path}")

    # Check keytab exists
    keytab_path = os.path.join(os.getcwd(), 'testuser.keytab')
    if not os.path.exists(keytab_path):
        print("❌ testuser.keytab not found. Run:")
        print("   docker cp hdfs-kerberos:/etc/security/keytabs/testuser.keytab ./testuser.keytab")
        sys.exit(1)

    print(f"✅ Keytab found: {keytab_path}")
    return keytab_path

def create_spark_session_with_kerberos(keytab_path, use_lakesail=False):
    """Create Spark session with Kerberos authentication"""
    print("\n🚀 Creating Spark session with Kerberos...")

    # Get krb5.conf path
    krb5_conf_path = os.environ.get('KRB5_CONFIG', os.path.join(os.getcwd(), 'krb5.conf'))

    # CRITICAL: Set java.security.krb5.conf for the JVM
    # This tells Spark's JVM where to find the Kerberos configuration
    builder = SparkSession.builder \
        .appName("LakesailKerberosExample") \
        .config("spark.driver.extraJavaOptions", f"-Djava.security.krb5.conf={krb5_conf_path}") \
        .config("spark.executor.extraJavaOptions", f"-Djava.security.krb5.conf={krb5_conf_path}") \
        .config("spark.hadoop.hadoop.security.authentication", "kerberos") \
        .config("spark.hadoop.hadoop.security.authorization", "true") \
        .config("spark.kerberos.keytab", keytab_path) \
        .config("spark.kerberos.principal", "testuser@LAKESAIL.COM") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")

    if use_lakesail:
        print("📡 Connecting to Lakesail at sc://localhost:50051")
        spark = builder.remote("sc://localhost:50051").getOrCreate()
    else:
        print("💻 Using local Spark session")
        print(f"🔐 Using Kerberos config: {krb5_conf_path}")
        spark = builder.getOrCreate()

    print(f"✅ Spark session created (version: {spark.version})")
    return spark

def example_1_basic_hdfs_operations(spark):
    """Example 1: Basic HDFS read/write with Kerberos"""
    print("\n" + "="*60)
    print("Example 1: Basic HDFS Operations with Kerberos")
    print("="*60)

    hdfs_path = "hdfs://localhost:9000/user/testuser/example_data"

    # Create sample data
    print("📊 Creating sample dataset...")
    data = [
        (1, "Alice", 85000, "Engineering"),
        (2, "Bob", 92000, "Engineering"),
        (3, "Charlie", 78000, "Sales"),
        (4, "Diana", 81000, "Marketing"),
        (5, "Eve", 95000, "Engineering")
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "department"])

    # Write to secure HDFS
    print(f"💾 Writing data to secure HDFS: {hdfs_path}")
    df.write.mode("overwrite").parquet(hdfs_path)
    print("✅ Data written successfully")

    # Read back
    print("📖 Reading data back from HDFS...")
    df_read = spark.read.parquet(hdfs_path)
    df_read.show()
    print(f"✅ Successfully read {df_read.count()} records")

    return df_read

def example_2_sql_queries(spark, df):
    """Example 2: SQL queries on secure HDFS data"""
    print("\n" + "="*60)
    print("Example 2: SQL Queries on Secure Data")
    print("="*60)

    # Register as temp view
    df.createOrReplaceTempView("employees")

    # Query 1: Average salary by department
    print("📊 Query 1: Average salary by department")
    query1 = """
        SELECT
            department,
            COUNT(*) as employee_count,
            ROUND(AVG(salary), 2) as avg_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """
    result1 = spark.sql(query1)
    result1.show()

    # Query 2: High earners
    print("💰 Query 2: Employees earning above $80K")
    query2 = """
        SELECT name, department, salary
        FROM employees
        WHERE salary > 80000
        ORDER BY salary DESC
    """
    result2 = spark.sql(query2)
    result2.show()

    print("✅ SQL queries completed")

def example_3_delta_lake(spark):
    """Example 3: Delta Lake on Kerberos HDFS"""
    print("\n" + "="*60)
    print("Example 3: Delta Lake with Kerberos HDFS")
    print("="*60)

    delta_path = "hdfs://localhost:9000/user/testuser/delta_employees"

    # Create initial data
    print("📊 Creating initial Delta Lake table...")
    data = [
        (1, "Alice", 85000, "2024-01-15"),
        (2, "Bob", 92000, "2024-01-15"),
        (3, "Charlie", 78000, "2024-01-15")
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "date"])

    # Write as Delta table
    print(f"💾 Writing Delta table to: {delta_path}")
    try:
        df.write.format("delta").mode("overwrite").save(delta_path)
        print("✅ Delta table created")

        # Read Delta table
        print("📖 Reading Delta table...")
        delta_df = spark.read.format("delta").load(delta_path)
        delta_df.show()

        # Append new data
        print("➕ Appending new records...")
        new_data = [(4, "Diana", 81000, "2024-01-16")]
        new_df = spark.createDataFrame(new_data, ["id", "name", "salary", "date"])
        new_df.write.format("delta").mode("append").save(delta_path)

        # Read updated table
        print("📖 Reading updated Delta table...")
        updated_df = spark.read.format("delta").load(delta_path)
        updated_df.show()
        print(f"✅ Delta table now has {updated_df.count()} records")

    except Exception as e:
        print(f"⚠️  Delta Lake operation failed: {e}")
        print("💡 This might be expected if Delta Lake is not configured")

def example_4_complex_analytics(spark):
    """Example 4: Complex analytics on secure HDFS"""
    print("\n" + "="*60)
    print("Example 4: Complex Analytics")
    print("="*60)

    # Create larger dataset
    print("📊 Creating larger dataset for analytics...")
    import random
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
    data = [
        (i, f"Employee_{i}", random.randint(50000, 120000), random.choice(departments))
        for i in range(1, 101)
    ]
    df = spark.createDataFrame(data, ["id", "name", "salary", "department"])

    # Write to HDFS
    analytics_path = "hdfs://localhost:9000/user/testuser/analytics_data"
    print(f"💾 Writing to: {analytics_path}")
    df.write.mode("overwrite").parquet(analytics_path)

    # Complex analytics
    df.createOrReplaceTempView("all_employees")

    print("📈 Running complex analytics...")
    complex_query = """
        SELECT
            department,
            COUNT(*) as total_employees,
            ROUND(AVG(salary), 2) as avg_salary,
            ROUND(STDDEV(salary), 2) as salary_stddev,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary,
            PERCENTILE_APPROX(salary, 0.5) as median_salary
        FROM all_employees
        GROUP BY department
        HAVING COUNT(*) > 5
        ORDER BY avg_salary DESC
    """

    result = spark.sql(complex_query)
    result.show(truncate=False)
    print("✅ Complex analytics completed")

def main():
    """Main execution function"""
    print("="*60)
    print("Lakesail + Kerberos HDFS Example")
    print("="*60)

    # Setup Kerberos
    keytab_path = setup_kerberos_environment()

    # Check if we need to run kinit (for local Spark mode)
    print("\n🔍 Checking Kerberos authentication...")
    try:
        import subprocess
        result = subprocess.run(["klist"], capture_output=True, text=True)
        if result.returncode == 0 and "testuser@LAKESAIL.COM" in result.stdout:
            print("✅ Valid Kerberos ticket found")
        else:
            print("⚠️  No valid Kerberos ticket found")
            print("💡 For local Spark mode, run: kinit -kt ./testuser.keytab testuser@LAKESAIL.COM")
            print("💡 For Lakesail mode, make sure the server was started with kinit")
    except Exception as e:
        print(f"⚠️  Could not check Kerberos ticket: {e}")

    # Create Spark session
    # Set use_lakesail=True if you have Lakesail server running
    # Otherwise it will use local Spark
    use_lakesail = False  # Change to True if Lakesail server is running on port 50051
    spark = create_spark_session_with_kerberos(keytab_path, use_lakesail)

    try:
        # Run examples
        df = example_1_basic_hdfs_operations(spark)
        example_2_sql_queries(spark, df)
        example_3_delta_lake(spark)
        example_4_complex_analytics(spark)

        print("\n" + "="*60)
        print("🎉 All examples completed successfully!")
        print("="*60)
        print("\n📋 Summary:")
        print("✅ Connected to Kerberos-secured HDFS")
        print("✅ Performed basic read/write operations")
        print("✅ Executed SQL queries on secure data")
        print("✅ Created Delta Lake tables (if available)")
        print("✅ Ran complex analytics")
        print("\n💡 Next Steps:")
        print("- Modify the examples for your use case")
        print("- Try with Lakesail server (set use_lakesail=True)")
        print("- Explore Delta Lake ACID operations")
        print("- Scale up to production workloads")

    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🛑 Stopping Spark session...")
        spark.stop()
        print("✅ Cleanup complete")

if __name__ == "__main__":
    main()
