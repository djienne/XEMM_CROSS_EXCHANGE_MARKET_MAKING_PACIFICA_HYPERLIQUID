"""
XEMM Bot Deployment Script (Rust Version)

Deploys the bot to remote VPS using scp with tar compression over SSH.
Cross-platform compatible (Windows, Linux, macOS).

Deployment method:
- Creates compressed tar.gz archive locally (with exclusions)
- Transfers single archive file via scp (efficient single-file transfer)
- Extracts archive on remote server
- Falls back to rsync if available (faster incremental updates)

What gets deployed:
- All Rust source code (src/, examples/)
- Configuration files (Cargo.toml, Cargo.lock)
- Docker assets (Dockerfile, docker-compose.yml, .dockerignore)
- Documentation (docs/, *.md, CLAUDE.md, DEPLOYMENT.md)

What gets excluded:
- Git files (.git/, .gitignore)
- Build artifacts (target/) - will be built on remote server via Docker
- Log files (*.log, logs/)
- SSH keys (lighter.pem, *.pem)
- Configuration file (config.json) - CREATE MANUALLY ON REMOTE SERVER
- Environment file (.env) - CREATE MANUALLY ON REMOTE SERVER
- IDE settings (.vscode/, .idea/, .claude/)
- Deployment scripts (deploy.sh, deploy.py)
- Temporary files (nul, *.swp, *.swo, .DS_Store)
- Python dashboard (dashboard_python/) - not needed for bot

CRITICAL: After deployment, you MUST:
1. Create config.json with bot parameters on remote server
2. Create .env file with API credentials on remote server
3. Build and run with Docker: docker compose build && docker compose up -d
4. Use graceful shutdown (Ctrl+C or docker compose stop) - NEVER kill -9

Usage:
    python deploy.py
"""

import os
import sys
import subprocess
import platform
from pathlib import Path

# Configuration
REMOTE_USER = "ubuntu"
REMOTE_HOST = "54.95.246.213"
REMOTE_PATH = "/home/ubuntu/XEMM_rust"
LOCAL_PATH = "."
SSH_KEY_NAME = "lighter.pem"  # SSH key filename

# Find SSH key in multiple locations (cross-platform: Windows + WSL)
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

# Files that must always be shipped even if they match an exclusion rule
INCLUDE_ALWAYS = {
    "Dockerfile",
    "docker-compose.yml",
}

# Files and directories to exclude from deployment
EXCLUDE_PATTERNS = [
    '.git/',
    '.gitignore',
    'target/',  # Rust build artifacts - will be built via Docker on remote
    '*.log',
    'logs/',
    'logs_remote/',  # Don't overwrite remote logs
    'dashboard_python/',  # Python dashboard not needed for bot
    '.vscode/',
    '.idea/',
    '.claude/',  # Claude Code settings
    '*.pem',  # SSH keys
    'lighter.pem',
    'config.json',  # NEVER deploy config - create manually on remote server
    '.env',  # NEVER deploy credentials - create manually on remote server
    '.env.local',
    '.env.*.local',
    'nul',  # Windows null device file
    '*.swp',  # Vim swap files
    '*.swo',
    '.DS_Store',  # macOS
    'Thumbs.db',  # Windows
    'deploy.sh',
    'deploy.py',
    '**/*.pyc',  # Python bytecode
    '__pycache__/',  # Python cache
]


def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 60)
    print(text.center(60))
    print("=" * 60 + "\n")


def print_success(text):
    """Print success message."""
    print(f"[OK] {text}")


def print_error(text):
    """Print error message."""
    print(f"[ERROR] {text}")


def print_warning(text):
    """Print warning message."""
    print(f"[WARNING] {text}")


def print_info(text):
    """Print info message."""
    print(f"[INFO] {text}")


def check_ssh_key():
    """Check if SSH key exists and set proper permissions."""
    if SSH_KEY is None:
        print_error(f"SSH key '{SSH_KEY_NAME}' not found in any expected location")
        print("Searched locations:")
        print(f"  - ~/{SSH_KEY_NAME} (WSL/Linux home)")
        print(f"  - ./{SSH_KEY_NAME} (current directory)")
        print(f"Please ensure {SSH_KEY_NAME} is in one of these locations")
        return False

    ssh_key_path = Path(SSH_KEY)
    print_success(f"Found SSH key: {SSH_KEY}")

    # Set proper permissions (Unix-like systems only)
    if platform.system() != "Windows":
        try:
            os.chmod(ssh_key_path, 0o600)
            print_success(f"SSH key permissions set to 600")
        except Exception as e:
            print_warning(f"Could not set SSH key permissions: {e}")

    return True


def test_ssh_connection():
    """Test SSH connection to remote server."""
    print_info("Testing SSH connection (may take up to 60 seconds)...")

    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "ConnectTimeout=30",
        "-o", "StrictHostKeyChecking=no",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        "echo 'Connection successful'"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            print_success("SSH connection successful")
            return True
        else:
            print_error("Cannot connect to remote server")
            print(f"Host: {REMOTE_USER}@{REMOTE_HOST}")
            print(f"Key: {SSH_KEY}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print_error("SSH connection timed out after 60 seconds")
        return False
    except FileNotFoundError:
        print_error("SSH client not found. Please install OpenSSH.")
        return False
    except Exception as e:
        print_error(f"SSH test failed: {e}")
        return False


def create_remote_directory():
    """Create remote directory if it doesn't exist."""
    print_info("Ensuring remote directory exists...")

    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "ConnectTimeout=30",
        "-o", "StrictHostKeyChecking=no",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        f"mkdir -p {REMOTE_PATH}"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print_success(f"Remote directory ready: {REMOTE_PATH}")
            return True
        else:
            print_error(f"Failed to create remote directory: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print_error("Remote directory creation timed out after 60 seconds")
        return False
    except Exception as e:
        print_error(f"Error creating remote directory: {e}")
        return False


def check_rsync_available():
    """Check if rsync is available (disabled on Windows)."""
    # Disable rsync on Windows - even if available, it doesn't work reliably
    if platform.system() == "Windows":
        return False

    try:
        result = subprocess.run(
            ["rsync", "--version"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def deploy_with_rsync():
    """Deploy using rsync (faster, incremental)."""
    print_info("Deploying using rsync (incremental sync)...")

    # Build exclude arguments
    exclude_args = []
    for pattern in EXCLUDE_PATTERNS:
        exclude_args.extend(["--exclude", pattern])

    cmd = [
        "rsync",
        "-avz",
        "--progress",
        "--delete",
        "-e", f"ssh -i {SSH_KEY} -o StrictHostKeyChecking=no -o ConnectTimeout=30",
        *exclude_args,
        f"{LOCAL_PATH}/",
        f"{REMOTE_USER}@{REMOTE_HOST}:{REMOTE_PATH}/"
    ]

    try:
        result = subprocess.run(cmd, timeout=300)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print_error("Rsync deployment timed out after 5 minutes")
        return False
    except Exception as e:
        print_error(f"Rsync deployment failed: {e}")
        return False


def deploy_with_scp():
    """Deploy using scp with tar compression (efficient single-file transfer)."""
    print_info("Deploying using scp with tar compression...")

    import tempfile
    import tarfile

    # Create temporary tar file
    with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tmp_tar:
        tar_path = tmp_tar.name

    try:
        print_info("Creating compressed archive...")

        # Create tar archive with exclusions
        def filter_exclude(tarinfo):
            """Filter function to exclude files matching patterns."""
            # Get relative path
            path_str = tarinfo.name
            base_name = os.path.basename(path_str)

            # Never exclude files explicitly marked for inclusion
            if base_name in INCLUDE_ALWAYS:
                return tarinfo

            for pattern in EXCLUDE_PATTERNS:
                if pattern.endswith('/'):
                    # Directory pattern - check if directory name matches
                    dir_name = pattern.rstrip('/')
                    if dir_name in path_str.split('/'):
                        return None
                elif '*' in pattern:
                    # Wildcard pattern
                    import fnmatch
                    if fnmatch.fnmatch(path_str, pattern) or fnmatch.fnmatch(base_name, pattern):
                        return None
                else:
                    # Exact filename match
                    if pattern == base_name or pattern in path_str:
                        return None

            return tarinfo

        # Create the archive
        with tarfile.open(tar_path, 'w:gz') as tar:
            tar.add(LOCAL_PATH, arcname='xemm_rust', filter=filter_exclude)

        # Get archive size
        archive_size_mb = os.path.getsize(tar_path) / (1024 * 1024)
        print_success(f"Archive created: {archive_size_mb:.2f} MB")

        # Transfer archive to remote
        print_info("Transferring archive to remote server...")

        scp_cmd = [
            "scp",
            "-i", SSH_KEY,
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=30",
            tar_path,
            f"{REMOTE_USER}@{REMOTE_HOST}:/tmp/xemm_deploy.tar.gz"
        ]

        result = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print_error(f"Failed to transfer archive: {result.stderr}")
            return False

        print_success("Archive transferred successfully")

        # Extract archive on remote server
        print_info("Extracting archive on remote server...")

        extract_cmd = [
            "ssh",
            "-i", SSH_KEY,
            "-o", "StrictHostKeyChecking=no",
            "-o", "ConnectTimeout=30",
            f"{REMOTE_USER}@{REMOTE_HOST}",
            f"cd {REMOTE_PATH} && tar -xzf /tmp/xemm_deploy.tar.gz --strip-components=1 && rm /tmp/xemm_deploy.tar.gz"
        ]

        result = subprocess.run(extract_cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            print_error(f"Failed to extract archive: {result.stderr}")
            return False

        print_success("Deployment completed successfully")
        return True

    except Exception as e:
        print_error(f"SCP deployment failed: {e}")
        return False
    finally:
        # Clean up local tar file
        try:
            if os.path.exists(tar_path):
                os.unlink(tar_path)
        except:
            pass


def fix_line_endings():
    """Fix line endings on shell scripts to ensure Linux compatibility."""
    print_info("Fixing line endings on shell scripts for Linux compatibility...")

    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "ConnectTimeout=30",
        "-o", "StrictHostKeyChecking=no",
        f"{REMOTE_USER}@{REMOTE_HOST}",
        f"cd {REMOTE_PATH} && find . -name '*.sh' -type f -exec sed -i 's/\\r$//' {{}} +"
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            print_success("Line endings fixed on all .sh files")
            return True
        else:
            print_warning(f"Could not fix line endings: {result.stderr}")
            return False
    except Exception as e:
        print_warning(f"Error fixing line endings: {e}")
        return False


def display_next_steps():
    """Display next steps after successful deployment."""
    print("\n" + "=" * 60)
    print("Deployment completed successfully!".center(60))
    print("=" * 60 + "\n")

    print("Next steps:\n")
    print(f"1. SSH into the server:")
    print(f"   ssh -i {SSH_KEY} {REMOTE_USER}@{REMOTE_HOST}\n")

    print(f"2. Navigate to the bot directory:")
    print(f"   cd {REMOTE_PATH}\n")

    print("3. Create config.json with bot parameters:")
    print("   nano config.json")
    print("   (Copy contents from your local config.json file)\n")

    print("4. Create .env file with your credentials:")
    print("   nano .env")
    print("   (Copy contents from your local .env file)")
    print("   Required variables:")
    print("     PACIFICA_API_KEY, PACIFICA_SECRET_KEY, PACIFICA_ACCOUNT")
    print("     HL_WALLET, HL_PRIVATE_KEY\n")

    print("5. Verify configuration:")
    print("   cat config.json")
    print("   cat .env\n")

    print("6. Build and run with Docker (recommended):")
    print("   docker compose build")
    print("   docker compose up -d")
    print("   docker compose logs -f\n")

    print("7. To stop gracefully (CRITICAL - cancels all orders):")
    print("   docker compose stop\n")

    print("=" * 60)
    print("IMPORTANT SAFETY NOTES:")
    print("=" * 60)
    print("• config.json is NOT deployed - create it manually on remote server")
    print("• .env file is NOT deployed for security - create it manually")
    print("• NEVER use 'kill -9' or 'docker kill' - orders won't be cancelled")
    print("• Always use graceful shutdown (Ctrl+C or docker compose stop)")
    print("• Bot exits after one successful arbitrage cycle")
    print("• Check bot logs regularly: docker compose logs -f")


def main():
    """Main deployment function."""
    print_header("XEMM Bot (Rust) - Remote Deployment Script")

    # Step 1: Check SSH key
    if not check_ssh_key():
        sys.exit(1)

    # Step 2: Test SSH connection
    if not test_ssh_connection():
        sys.exit(1)

    # Step 3: Create remote directory
    if not create_remote_directory():
        sys.exit(1)

    # Step 4: Display deployment details
    print("\nDeployment Details:")
    print(f"  Local path:  {LOCAL_PATH}")
    print(f"  Remote host: {REMOTE_USER}@{REMOTE_HOST}")
    print(f"  Remote path: {REMOTE_PATH}")
    print(f"  SSH key:     {SSH_KEY}")
    print()

    # Check if config.json and .env exist locally and remind user
    config_file = Path("config.json")
    env_file = Path(".env")

    if config_file.exists():
        print_warning("config.json will NOT be deployed (prevents overwriting remote config)")
        print_info("You must create config.json manually on the remote server")
    else:
        print_warning("config.json not found locally")

    if env_file.exists():
        print_warning(".env file will NOT be deployed (security best practice)")
        print_info("You must create .env manually on the remote server with your credentials")
    else:
        print_warning(".env file not found locally")
        print_info("Remember to create .env on the remote server before running the bot")
    print()

    # Step 5: Deploy (auto-confirmed)
    success = False

    # Use scp with tar compression (efficient single-file transfer)
    # Falls back to rsync if available for incremental updates
    if check_rsync_available():
        print_info("rsync detected - using for incremental sync")
        success = deploy_with_rsync()
    else:
        success = deploy_with_scp()

    # Step 6: Fix line endings (Windows/Linux compatibility)
    if success:
        fix_line_endings()

    # Step 7: Display results
    if success:
        display_next_steps()
    else:
        print("\n" + "=" * 60)
        print("Deployment failed!".center(60))
        print("=" * 60 + "\n")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n")
        print_warning("Deployment interrupted by user")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
