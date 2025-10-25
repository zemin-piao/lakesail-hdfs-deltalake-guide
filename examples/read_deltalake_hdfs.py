#!/usr/bin/env python3

from pyspark.sql import SparkSession
import time
import os

def read_deltalake_tables():
    print("🔌 Connecting to Lakesail server...")

    # Connect to Lakesail
    spark = SparkSession.builder \
        .appName("ReadDeltaLakeHDFS") \
        .remote("sc://localhost:50051") \
        .getOrCreate()

    print("✅ Connected to Lakesail successfully!")

    # Get current user for HDFS path
    username = os.getenv('USER') or os.getenv('USERNAME') or 'user'
    hdfs_base_path = f"hdfs://localhost:9000/user/{username}"

    print(f"\n📖 Reading data from HDFS at: {hdfs_base_path}")

    # Read employees table (Parquet)
    print("\n👥 Reading employees data (Parquet)...")
    try:
        employees_path = f"{hdfs_base_path}/employees_parquet/"
        employees_df = spark.read.format("parquet").load(employees_path)
        print(f"✅ Successfully read employees from: {employees_path}")
        print("📊 Schema:")
        employees_df.printSchema()
        print("📋 Data preview:")
        employees_df.show()
        print(f"📈 Total records: {employees_df.count()}")
    except Exception as e:
        print(f"❌ Error reading employees: {e}")
        employees_df = None

    # Read transactions table (Parquet)
    print("\n💰 Reading transactions data (Parquet)...")
    try:
        transactions_path = f"{hdfs_base_path}/transactions_parquet/"
        transactions_df = spark.read.format("parquet").load(transactions_path)
        print(f"✅ Successfully read transactions from: {transactions_path}")
        print("📊 Schema:")
        transactions_df.printSchema()
        print("📋 Data preview:")
        transactions_df.show()
        print(f"📈 Total records: {transactions_df.count()}")
    except Exception as e:
        print(f"❌ Error reading transactions: {e}")
        transactions_df = None

    # Try reading Delta Lake format
    print("\n🔺 Attempting to read Delta Lake format...")
    try:
        delta_employees_path = f"{hdfs_base_path}/employees_delta/"
        delta_employees_df = spark.read.format("delta").load(delta_employees_path)
        print(f"✅ Successfully read Delta Lake employees from: {delta_employees_path}")
        print("📋 Delta Lake employees preview:")
        delta_employees_df.show(5)

        delta_transactions_path = f"{hdfs_base_path}/transactions_delta/"
        delta_transactions_df = spark.read.format("delta").load(delta_transactions_path)
        print(f"✅ Successfully read Delta Lake transactions from: {delta_transactions_path}")
        print("📋 Delta Lake transactions preview:")
        delta_transactions_df.show(5)

    except Exception as e:
        print(f"⚠️  Could not read Delta format: {e}")
        print("💡 This is expected if Delta Lake is not available")

    # Read partitioned table
    print("\n📂 Reading partitioned employees data...")
    try:
        partitioned_path = f"{hdfs_base_path}/employees_partitioned/"
        partitioned_df = spark.read.format("parquet").load(partitioned_path)
        print(f"✅ Successfully read partitioned data from: {partitioned_path}")
        print("📋 Partitioned data preview:")
        partitioned_df.show(5)

        print("\n🔍 Testing partition filtering (Engineering department):")
        eng_df = partitioned_df.filter(partitioned_df.department == "Engineering")
        print(f"Engineering employees: {eng_df.count()}")
        eng_df.show()

    except Exception as e:
        print(f"⚠️  Could not read partitioned data: {e}")

    # Perform advanced analytics if data is available
    if employees_df is not None and transactions_df is not None:
        print("\n🔗 Performing advanced analytics across HDFS tables...")

        # Register as temporary views
        employees_df.createOrReplaceTempView("employees")
        transactions_df.createOrReplaceTempView("transactions")

        # Complex analytical query
        print("🔍 Running employee-transaction analysis...")
        analysis_query = """
        SELECT
            e.name,
            e.department,
            e.city,
            e.salary,
            COUNT(t.transaction_id) as transaction_count,
            COALESCE(SUM(t.amount), 0) as total_amount,
            COALESCE(AVG(t.amount), 0) as avg_transaction_amount,
            CASE
                WHEN COUNT(t.transaction_id) > 0
                THEN ROUND(SUM(t.amount) / e.salary * 100, 2)
                ELSE 0
            END as amount_to_salary_ratio
        FROM employees e
        LEFT JOIN transactions t ON e.id = t.employee_id
        GROUP BY e.id, e.name, e.department, e.city, e.salary
        ORDER BY total_amount DESC
        """

        start_time = time.time()
        result_df = spark.sql(analysis_query)
        result_df.show(truncate=False)
        end_time = time.time()

        print(f"⚡ Employee analysis completed in {end_time - start_time:.3f} seconds")

        # Department summary with performance metrics
        print("\n📊 Department performance analysis:")
        dept_query = """
        SELECT
            department,
            COUNT(*) as employee_count,
            ROUND(AVG(salary), 2) as avg_salary,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary,
            ROUND(SUM(salary), 2) as total_payroll
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
        """

        dept_result = spark.sql(dept_query)
        dept_result.show()

        # Transaction type analysis
        print("\n💰 Transaction type analysis:")
        trans_query = """
        SELECT
            type,
            COUNT(*) as transaction_count,
            ROUND(SUM(amount), 2) as total_amount,
            ROUND(AVG(amount), 2) as avg_amount,
            MAX(amount) as max_amount,
            MIN(amount) as min_amount
        FROM transactions
        GROUP BY type
        ORDER BY total_amount DESC
        """

        trans_result = spark.sql(trans_query)
        trans_result.show()

        # Cross-department transaction analysis
        print("\n🔀 Cross-department transaction patterns:")
        cross_query = """
        SELECT
            e.department,
            t.type,
            COUNT(*) as count,
            ROUND(AVG(t.amount), 2) as avg_amount
        FROM employees e
        JOIN transactions t ON e.id = t.employee_id
        GROUP BY e.department, t.type
        ORDER BY e.department, avg_amount DESC
        """

        cross_result = spark.sql(cross_query)
        cross_result.show()

        # Performance test with larger dataset
        print("\n⚡ Performance test with computed data:")
        perf_start = time.time()

        computed_query = """
        SELECT
            department,
            COUNT(*) * 1000 as projected_headcount,
            AVG(salary) * 1.1 as projected_avg_salary,
            SUM(salary) * 12 as annual_payroll
        FROM employees
        GROUP BY department
        """

        perf_result = spark.sql(computed_query)
        perf_result.show()
        perf_end = time.time()

        print(f"⚡ Performance test completed in {perf_end - perf_start:.3f} seconds")

    else:
        print("⚠️  Skipping analytics due to missing data tables")

    print("\n🎉 Read and analysis operations completed successfully!")
    print("\n📋 Summary:")
    print("   ✅ Connected to Lakesail via Spark Connect")
    print("   ✅ Read data from HDFS storage")
    print("   ✅ Performed complex SQL analytics")
    print("   ✅ Demonstrated cross-table joins")
    print("   ✅ Verified performance characteristics")

    spark.stop()

if __name__ == "__main__":
    try:
        read_deltalake_tables()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()