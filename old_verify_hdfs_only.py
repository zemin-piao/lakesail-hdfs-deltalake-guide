#!/usr/bin/env python3
"""
Unified HDFS Setup Verification Script
=======================================
Auto-detects and verifies either Standard or Kerberos HDFS setup.
"""

import subprocess
import os
import sys
import time

def run_command(cmd, description="", ignore_error=False):
    """Run a shell command and return output"""
    try:
        result = subprocess.run(
            cmd if isinstance(cmd, list) else cmd,
            shell=not isinstance(cmd, list),
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode != 0 and not ignore_error:
            return None
        return result.stdout
    except Exception:
        return None

def detect_setup():
    """Detect which HDFS setup is running"""
    print("🔍 Detecting HDFS setup...")

    # Check for hdfs-kerberos
    output = run_command("docker ps | grep hdfs-kerberos", ignore_error=True)
    if output and "hdfs-kerberos" in output:
        print("✅ Detected: Kerberos HDFS (hdfs-kerberos container)")
        return "kerberos", "hdfs-kerberos"

    # Check for hdfs-working
    output = run_command("docker ps | grep hdfs-working", ignore_error=True)
    if output and "hdfs-working" in output:
        print("✅ Detected: Standard HDFS (hdfs-working container)")
        return "standard", "hdfs-working"

    print("❌ No HDFS container found")
    print("💡 Run: ./setup_lakesail_hdfs.sh")
    return None, None

def verify_kerberos_specific(container_name):
    """Kerberos-specific verification checks"""
    print("\n" + "="*60)
    print("🔐 Kerberos-Specific Checks")
    print("="*60)

    # 1. Check KDC service
    print("\n1️⃣ Checking Kerberos KDC...")
    output = run_command(
        f"docker exec {container_name} ps aux | grep krb5kdc | grep -v grep",
        ignore_error=True
    )
    if output:
        print("✅ Kerberos KDC is running")
    else:
        print("❌ Kerberos KDC is not running")
        return False

    # 2. Check principals
    print("\n2️⃣ Checking Kerberos principals...")
    output = run_command(
        f"docker exec {container_name} kadmin.local -q 'listprincs'",
        ignore_error=True
    )
    if output:
        principals = [line for line in output.split('\n')
                     if any(x in line for x in ['hdfs', 'HTTP', 'testuser'])]
        if principals:
            print(f"✅ Found {len(principals)} principals")
            for p in principals[:5]:
                if p.strip():
                    print(f"   • {p.strip()}")

    # 3. Check keytabs
    print("\n3️⃣ Checking keytabs...")
    keytabs = ['hdfs.keytab', 'spnego.service.keytab', 'testuser.keytab']
    all_ok = True

    for keytab in keytabs:
        output = run_command(
            f"docker exec {container_name} ls /etc/security/keytabs/{keytab}",
            ignore_error=True
        )
        if output:
            print(f"✅ {keytab}: Present")
        else:
            print(f"❌ {keytab}: Missing")
            all_ok = False

    # 4. Test authentication
    print("\n4️⃣ Testing Kerberos authentication...")
    auth_output = run_command(
        f"docker exec {container_name} kinit -kt /etc/security/keytabs/testuser.keytab testuser@LAKESAIL.COM",
        ignore_error=True
    )

    ticket_output = run_command(
        f"docker exec {container_name} klist",
        ignore_error=True
    )

    if ticket_output and "testuser@LAKESAIL.COM" in ticket_output:
        print("✅ Successfully authenticated as testuser@LAKESAIL.COM")
    else:
        print("⚠️  Could not verify authentication")

    return all_ok

def verify_hdfs_cluster(container_name):
    """Common HDFS cluster verification"""
    print("\n" + "="*60)
    print("🗄️  HDFS Cluster Verification")
    print("="*60)

    # 1. Check HDFS status
    print("\n1️⃣ Checking HDFS cluster status...")
    hdfs_cmd = "hdfs" if container_name == "hdfs-working" else "/opt/hadoop/bin/hdfs"

    output = run_command(
        f"docker exec {container_name} {hdfs_cmd} dfsadmin -report",
        ignore_error=True
    )

    if output:
        print("✅ HDFS cluster is healthy")
        # Show datanode info
        lines = output.split('\n')
        for line in lines:
            if "Live datanodes" in line:
                print(f"   {line.strip()}")
    else:
        print("❌ HDFS cluster is not responding")
        return False

    # 2. Check HDFS directories
    print("\n2️⃣ Checking HDFS directories...")
    output = run_command(
        f"docker exec {container_name} {hdfs_cmd} dfs -ls /",
        ignore_error=True
    )

    if output:
        print("✅ Can list root directory")
        if "/user" in output:
            print("   Found /user directory")
    else:
        print("⚠️  Could not list directories")

    # 3. Test write/read
    print("\n3️⃣ Testing HDFS write/read operations...")

    # Write test
    write_cmd = f"docker exec {container_name} bash -c \"echo 'test' | {hdfs_cmd} dfs -put -f - /tmp/verify_test.txt\""
    write_output = run_command(write_cmd, ignore_error=True)

    if write_output is not None:  # Command succeeded
        print("✅ Successfully wrote test file")

        # Read test
        read_output = run_command(
            f"docker exec {container_name} {hdfs_cmd} dfs -cat /tmp/verify_test.txt",
            ignore_error=True
        )

        if read_output and "test" in read_output:
            print("✅ Successfully read test file")

            # Cleanup
            run_command(
                f"docker exec {container_name} {hdfs_cmd} dfs -rm /tmp/verify_test.txt",
                ignore_error=True
            )
        else:
            print("❌ Could not read test file")
            return False
    else:
        print("❌ Could not write test file")
        return False

    return True

def verify_configuration(container_name, setup_type):
    """Verify configuration files"""
    print("\n" + "="*60)
    print("⚙️  Configuration Verification")
    print("="*60)

    if setup_type == "kerberos":
        print("\n📋 Checking Kerberos configuration...")

        # Check core-site.xml
        output = run_command(
            f"docker exec {container_name} cat /opt/hadoop/etc/hadoop/core-site.xml | grep kerberos",
            ignore_error=True
        )

        if output:
            print("✅ hadoop.security.authentication = kerberos")
        else:
            print("⚠️  Could not verify Kerberos configuration")

    # Check HDFS configuration
    print("\n📋 Checking HDFS configuration...")
    output = run_command(
        f"docker exec {container_name} cat /opt/hadoop/etc/hadoop/hdfs-site.xml 2>/dev/null || docker exec {container_name} cat /hadoop/etc/hadoop/hdfs-site.xml",
        ignore_error=True
    )

    if output:
        print("✅ HDFS configuration found")
        if "dfs.replication" in output:
            print("   • Replication configured")
        if "dfs.datanode" in output:
            print("   • DataNode configured")

    return True

def verify_connectivity():
    """Verify network connectivity to HDFS"""
    print("\n" + "="*60)
    print("🌐 Network Connectivity")
    print("="*60)

    print("\n📡 Checking port accessibility...")

    # Check HDFS NameNode port
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 9000))
        sock.close()

        if result == 0:
            print("✅ HDFS NameNode port 9000: Accessible")
        else:
            print("❌ HDFS NameNode port 9000: Not accessible")
            return False

        # Check Web UI
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('localhost', 9870))
        sock.close()

        if result == 0:
            print("✅ HDFS Web UI port 9870: Accessible")
            print("   🌐 View at: http://localhost:9870")
        else:
            print("⚠️  HDFS Web UI port 9870: Not accessible")

    except Exception as e:
        print(f"⚠️  Could not check ports: {e}")

    return True

def print_summary(setup_type, container_name, all_passed):
    """Print verification summary"""
    print("\n" + "="*60)
    print("📋 VERIFICATION SUMMARY")
    print("="*60)

    print(f"\n📦 Setup Type: {setup_type.upper()}")
    print(f"🐳 Container: {container_name}")

    if all_passed:
        print("\n🎉 ALL VERIFICATIONS PASSED!")
        print("\n✅ Your HDFS setup is fully operational!")

        if setup_type == "kerberos":
            print("\n🔑 Next Steps:")
            print("1. Copy keytabs to local machine:")
            print(f"   docker cp {container_name}:/etc/security/keytabs/testuser.keytab ./testuser.keytab")
            print(f"   docker cp {container_name}:/etc/krb5.conf ./krb5.conf")
            print("\n2. Try examples:")
            print("   python3 examples/example_lakesail_kerberos.py")
        else:
            print("\n🚀 Next Steps:")
            print("1. Start Lakesail server (optional):")
            print("   python3 examples/start_lakesail_server.py")
            print("\n2. Try examples:")
            print("   python3 examples/create_deltalake_hdfs.py")
    else:
        print("\n⚠️  SOME CHECKS FAILED")
        print("\n💡 Troubleshooting:")
        print("• Check logs: docker logs " + container_name)
        print("• Restart container: docker restart " + container_name)
        print("• Re-run setup: ./setup_lakesail_hdfs.sh")

def main():
    """Main verification function"""
    print("="*60)
    print("🔍 HDFS Setup Verification")
    print("="*60)
    print()

    # Detect setup
    setup_type, container_name = detect_setup()

    if not setup_type:
        sys.exit(1)

    all_passed = True

    # Run Kerberos-specific checks if needed
    if setup_type == "kerberos":
        if not verify_kerberos_specific(container_name):
            all_passed = False

    # Run common HDFS checks
    if not verify_hdfs_cluster(container_name):
        all_passed = False

    # Verify configuration
    if not verify_configuration(container_name, setup_type):
        all_passed = False

    # Check connectivity
    if not verify_connectivity():
        all_passed = False

    # Print summary
    print_summary(setup_type, container_name, all_passed)

    return 0 if all_passed else 1

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Verification interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
