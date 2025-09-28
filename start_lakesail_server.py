#!/usr/bin/env python3

from pysail.spark import SparkConnectServer
import time
import signal
import sys

def signal_handler(sig, frame):
    print("\n🛑 Shutting down Lakesail server...")
    sys.exit(0)

def start_lakesail():
    print("🚀 Starting Lakesail Spark Connect Server...")
    print("📡 Server will be available at: sc://localhost:50051")
    print("🌐 Press Ctrl+C to stop")

    # Handle graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # Create and start server
        server = SparkConnectServer(ip="0.0.0.0", port=50051)
        print("✅ Server created successfully")

        # Start server (blocking)
        server.start(background=False)

    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    start_lakesail()