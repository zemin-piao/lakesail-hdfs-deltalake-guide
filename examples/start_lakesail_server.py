#!/usr/bin/env python3

from pysail.spark import SparkConnectServer
import time
import signal
import sys
import subprocess
import shutil
import os


def signal_handler(sig, frame):
    print("\n🛑 Shutting down Lakesail server...")
    # Add any specific server shutdown logic here if server.start() doesn't handle it
    sys.exit(0)


def run_kerberos_kinit():
    """
    Attempts to authenticate with Kerberos using kinit and a keytab.
    """
    # --- IMPORTANT ---
    #
    # You MUST replace these placeholders with your actual values or set
    # the corresponding environment variables.
    #
    # KRB_KEYTAB_PATH: Absolute path to your keytab file.
    # Example: "/etc/security/keytabs/spark.service.keytab"
    #
    # KRB_PRINCIPAL: Full Kerberos principal associated with the keytab.
    # Example: "spark-user@YOUR-REALM.COM"
    #
    # KRB5_CONFIG_PATH: (Optional) Absolute path to your krb5.conf file.
    # If not set, the system default will be used.
    # Example: "/etc/krb5.conf"
    #
    # --- --- --- --- ---

    keytab_path = os.environ.get("KRB_KEYTAB_PATH", "./testuser.keytab")
    principal = os.environ.get("KRB_PRINCIPAL", "testuser@LAKESAIL.COM")
    krb5_config_path = os.environ.get("KRB5_CONFIG_PATH", "./krb5.conf")
    # Check if kinit command exists
    kinit_path = shutil.which("kinit")
    if not kinit_path:
        print("❌ Error: 'kinit' command not found in system PATH.")
        print("Please ensure Kerberos client utilities are installed.")
        sys.exit(1)

    # Check if keytab file exists
    if not os.path.isfile(keytab_path):
        print(f"❌ Error: Keytab file not found at: {keytab_path}")
        print("Please check the 'KRB_KEYTAB_PATH' environment variable or update the script.")
        sys.exit(1)

    # Prepare environment for subprocess
    kinit_env = os.environ.copy()

    # Check if a custom krb5.conf path is provided and valid
    if krb5_config_path:
        if not os.path.isfile(krb5_config_path):
            print(f"❌ Error: krb5.conf file not found at: {krb5_config_path}")
            print("Please check the 'KRB5_CONFIG_PATH' environment variable or update the script.")
            sys.exit(1)

        print(f"ℹ️ Using custom krb5.conf: {krb5_config_path}")
        kinit_env["KRB5_CONFIG"] = krb5_config_path
    else:
        print("ℹ️ Using system default krb5.conf")

    print(f"🔑 Attempting Kerberos authentication for principal: {principal}")
    print(f"Using keytab: {keytab_path}")

    try:
        # Construct the kinit command
        command = [kinit_path, "-kt", keytab_path, principal]

        # Execute the command
        # - check=True: Raises CalledProcessError if kinit fails
        # - text=True: Decodes stdout/stderr as text
        # - capture_output=True: Captures stdout and stderr
        # - env=kinit_env: Passes the custom environment (with KRB5_CONFIG if set)
        result = subprocess.run(command, check=True, text=True, capture_output=True, env=kinit_env)

        print("✅ Kerberos authentication successful (kinit).")
        if result.stdout:
            print(f"[kinit stdout]: {result.stdout.strip()}")

    except subprocess.CalledProcessError as e:
        print("❌ Kerberos authentication failed (kinit).")
        print(f"Return code: {e.returncode}")
        print(f"[kinit stdout]: {e.stdout.strip()}")
        print(f"[kinit stderr]: {e.stderr.strip()}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ An unexpected error occurred during kinit: {e}")
        sys.exit(1)


def start_lakesail():
    # Handle graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)

    try:
        # --- Run Kerberos kinit first ---
        run_kerberos_kinit()
        # --- --- --- --- --- --- --- ---

        # Verify we have a valid Kerberos ticket
        print("\n🔍 Verifying Kerberos ticket...")
        try:
            result = subprocess.run(["klist"], capture_output=True, text=True, check=True)
            if "testuser@LAKESAIL.COM" in result.stdout:
                print("✅ Valid Kerberos ticket found")
                print(result.stdout)
            else:
                print("⚠️  Warning: Unexpected ticket output")
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"❌ No valid Kerberos ticket: {e}")
            sys.exit(1)

        print("\n🚀 Starting Lakesail Spark Connect Server with Kerberos...")
        print("📡 Server will be available at: sc://localhost:50051")
        print("🔐 HDFS authentication: Kerberos (testuser@LAKESAIL.COM)")
        print("🌐 Press Ctrl+C to stop")

        # Set up Spark configuration for Kerberos
        keytab_path = os.environ.get("KRB_KEYTAB_PATH", "./testuser.keytab")
        principal = os.environ.get("KRB_PRINCIPAL", "testuser@LAKESAIL.COM")
        krb5_config_path = os.environ.get("KRB5_CONFIG_PATH", "./krb5.conf")

        # Set environment variables for Spark
        os.environ["KRB5_CONFIG"] = krb5_config_path

        # Set Spark configs for Kerberos
        spark_conf = {
            "spark.hadoop.hadoop.security.authentication": "kerberos",
            "spark.hadoop.hadoop.security.authorization": "true",
            "spark.kerberos.keytab": keytab_path,
            "spark.kerberos.principal": principal,
            "spark.hadoop.fs.defaultFS": "hdfs://localhost:9000"
        }

        print("\n⚙️  Spark Configuration:")
        for key, value in spark_conf.items():
            print(f"   • {key} = {value}")

        # Create and start server with Kerberos config
        server = SparkConnectServer(
            ip="0.0.0.0",
            port=50051,
            spark_conf=spark_conf
        )
        print("\n✅ Server created successfully")

        # Start server (blocking)
        print("🎯 Starting server (this will block until Ctrl+C)...")
        server.start(background=False)

    except Exception as e:
        print(f"\n❌ Failed to start server: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    start_lakesail()

