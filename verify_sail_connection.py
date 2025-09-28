#!/usr/bin/env python3

from pyspark.sql import SparkSession
import time

def verify_sail_connection():
    """Verify that we're connected to Sail vs regular Spark"""

    print("🔍 Verifying Spark Connect Connection Type")
    print("=" * 50)

    # Connect to Spark
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

    # Method 1: Check Spark version and context
    print(f"📊 Spark Version: {spark.version}")
    print(f"🔗 Spark Master: {spark.sparkContext.master}")
    print(f"📝 App Name: {spark.sparkContext.appName}")

    # Method 2: Check if we're using Spark Connect
    print(f"🌐 Using Spark Connect: {hasattr(spark, '_client')}")

    # Method 3: Check the actual connection details
    if hasattr(spark, '_client'):
        client = spark._client
        print(f"🔌 Client Type: {type(client).__name__}")
        print(f"🏠 Connection URL: {getattr(client, '_url', 'Unknown')}")
        print(f"📡 Channel: {getattr(client, '_channel', 'Unknown')}")

    # Method 4: Run a simple query and check execution plan
    print("\n🧮 Testing Query Execution:")
    df = spark.sql("SELECT 1 + 1 as result, 'Running on Sail!' as message")

    # Show the logical plan (this will show Sail-specific details)
    print("📋 Logical Plan:")
    df.explain(True)

    # Execute and show results
    print("\n✅ Query Results:")
    df.show()

    # Method 5: Check for Sail-specific features or behavior
    print("\n🔬 Additional Diagnostics:")

    # Test a more complex query to see execution engine behavior
    complex_df = spark.range(1000).selectExpr(
        "id",
        "id * 2 as doubled",
        "concat('item_', cast(id as string)) as name"
    )

    print("⚡ Complex Query Execution Time:")
    start_time = time.time()
    count = complex_df.count()
    end_time = time.time()
    print(f"   Records: {count}")
    print(f"   Time: {end_time - start_time:.3f} seconds")

    # Method 6: Check configuration
    print("\n⚙️ Spark Configuration:")
    conf = spark.sparkContext.getConf()
    all_conf = conf.getAll()

    # Look for Sail or Connect specific configurations
    sail_related = [item for item in all_conf if 'connect' in item[0].lower() or 'sail' in item[0].lower()]
    if sail_related:
        print("🔧 Connect/Sail Related Config:")
        for key, value in sail_related:
            print(f"   {key}: {value}")
    else:
        print("📋 Key Configurations:")
        for key, value in all_conf[:5]:  # Show first 5 configs
            print(f"   {key}: {value}")

    print("\n" + "=" * 50)
    print("🎯 Verification Complete!")

    # Summary
    if hasattr(spark, '_client'):
        print("✅ You are using Spark Connect")
        print("🚀 If Sail server is running on port 50051, you're using Sail!")
    else:
        print("⚠️  You are using local Spark (not Connect)")

    spark.stop()

if __name__ == "__main__":
    try:
        verify_sail_connection()
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n💡 Make sure:")
        print("1. Sail server is running on port 50051")
        print("2. You're using the correct connection URL: sc://localhost:50051")
        print("3. No firewall is blocking the connection")