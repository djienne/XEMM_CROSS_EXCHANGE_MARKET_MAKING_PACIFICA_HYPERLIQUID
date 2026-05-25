"""
Run bash run_nohup.sh on remote XEMM_rust server

This script:
1. Connects to the remote server via SSH
2. Navigates to XEMM_rust directory
3. Executes 'bash run_nohup.sh'
4. Displays the output

Usage:
    python run_remote.py
"""

import os
import sys
import subprocess
import platform

# Configuration (from deploy.py)
REMOTE_USER = "ubuntu"
REMOTE_HOST = "54.95.246.213"
REMOTE_PATH = "/home/ubuntu/XEMM_rust"
SSH_KEY_NAME = "lighter.pem"


def find_ssh_key():
    """Find SSH key in Windows or WSL filesystem."""
    possible_paths = [
        os.path.expanduser(f"~/{SSH_KEY_NAME}"),  # WSL/Linux home directory
        f"./{SSH_KEY_NAME}",  # Current directory (Windows native)
        os.path.join(os.getcwd(), SSH_KEY_NAME),  # Absolute current dir
        SSH_KEY_NAME,  # Relative path
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return os.path.abspath(path)

    return None


SSH_KEY = find_ssh_key()


def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(text.center(70))
    print("=" * 70 + "\n")


def print_success(text):
    """Print success message."""
    print(f"[OK] {text}")


def print_error(text):
    """Print error message."""
    print(f"[ERROR] {text}")


def print_info(text):
    """Print info message."""
    print(f"[INFO] {text}")


def check_ssh_key():
    """Check if SSH key exists."""
    if SSH_KEY is None:
        print_error(f"SSH key '{SSH_KEY_NAME}' not found in any expected location")
        print("Searched locations:")
        print(f"  - ~/{SSH_KEY_NAME} (WSL/Linux home)")
        print(f"  - ./{SSH_KEY_NAME} (current directory)")
        return False

    print_success(f"Found SSH key: {SSH_KEY}")

    # Set proper permissions (Unix-like systems only)
    if platform.system() != "Windows":
        try:
            os.chmod(SSH_KEY, 0o600)
            print_success("SSH key permissions set to 600")
        except Exception as e:
            print_info(f"Could not set SSH key permissions: {e}")

    return True


def fix_line_endings():
    """Fix line endings on remote run_nohup.sh to ensure Linux compatibility."""
    print_info("Ensuring proper line endings on remote script...")

    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "ConnectTimeout=30",
        "-o", "StrictHostKeyChecking=no",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        f"sed -i 's/\\r$//' {REMOTE_PATH}/run_nohup.sh"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode == 0:
            print_success("Line endings fixed")
            return True
        else:
            print_error(f"Failed to fix line endings: {result.stderr}")
            return False
    except Exception as e:
        print_error(f"Error fixing line endings: {e}")
        return False


def run_remote_script():
    """Run 'bash run_nohup.sh' on remote server."""
    print_info(f"Executing 'bash run_nohup.sh' on remote server...")

    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "ConnectTimeout=30",
        "-o", "StrictHostKeyChecking=no",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        f"cd {REMOTE_PATH} && bash run_nohup.sh"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            encoding='utf-8',
            errors='replace',  # Replace problematic characters instead of crashing
            timeout=120  # 2 minutes timeout
        )

        if result.returncode == 0:
            print_success("Command executed successfully")
            if result.stdout.strip():
                print_header("Command Output")
                # Replace Unicode characters that may not display properly on Windows
                output = result.stdout.strip().replace('OK', '[OK]').replace('WARN', '[WARNING]')
                print(output)
                print("\n" + "=" * 70)
            return True
        else:
            print_error(f"Command failed with return code {result.returncode}")
            if result.stderr:
                print("Error output:")
                stderr_safe = result.stderr.replace('OK', '[OK]').replace('WARN', '[WARNING]')
                print(stderr_safe)
            if result.stdout:
                print("Standard output:")
                stdout_safe = result.stdout.replace('OK', '[OK]').replace('WARN', '[WARNING]')
                print(stdout_safe)
            return False

    except subprocess.TimeoutExpired:
        print_error("SSH command timed out after 120 seconds")
        return False
    except UnicodeEncodeError as e:
        print_error(f"Unicode encoding error - output contains characters not supported by your terminal")
        print_info("The command may have executed successfully. Check logs on remote server.")
        return False
    except Exception as e:
        # Safely convert exception to string, replacing problematic characters
        error_msg = str(e).encode('ascii', errors='replace').decode('ascii')
        print_error(f"Error executing remote command: {error_msg}")
        return False


def main():
    """Main function."""
    print_header("XEMM Remote Script Runner")
    print_info(f"Remote: {REMOTE_USER}@{REMOTE_HOST}:{REMOTE_PATH}")
    print_info(f"Command: bash run_nohup.sh")

    # Step 1: Check SSH key
    if not check_ssh_key():
        sys.exit(1)

    # Step 2: Fix line endings (Windows/Linux compatibility)
    if not fix_line_endings():
        print_error("Failed to fix line endings, but continuing anyway...")

    # Step 3: Run remote script
    if not run_remote_script():
        sys.exit(1)

    print_success("Remote script execution complete")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n")
        print_info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
