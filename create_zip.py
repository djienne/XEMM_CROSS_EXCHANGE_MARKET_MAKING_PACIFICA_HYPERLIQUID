#!/usr/bin/env python3
"""
Create a portable zip archive of the XEMM Rust project.
Excludes build artifacts, temporary files, and sensitive data.
"""

import os
import zipfile
from pathlib import Path

# Directories to exclude
EXCLUDE_DIRS = {
    'target',           # Rust build artifacts
    '__pycache__',      # Python bytecode
    '.git',             # Git repository data
    'node_modules',     # Node.js dependencies (if any)
    '.vscode',          # IDE settings
    '.idea',            # JetBrains IDE settings
}

# File patterns to exclude
EXCLUDE_PATTERNS = {
    '*.pyc',           # Python bytecode
    '*.pyo',           # Python optimized bytecode
    '*.pdb',           # Windows debug symbols
    '*.log',           # Log files
    # NOTE: .env and *.pem are INCLUDED per user request (contain secrets)
    # '.env',          # Environment variables (secrets)
    # '.env.*',        # Environment variable variants
    # '*.pem',         # Private keys
    '*.key',           # Private keys (except .pem)
    '*.p12',           # Certificate files
    '*.pfx',           # Certificate files
    'NUL',             # Windows null file
    '.DS_Store',       # macOS metadata
    'Thumbs.db',       # Windows metadata
    '*.swp',           # Vim swap files
    '*.swo',           # Vim swap files
    '*~',              # Backup files
}

def should_exclude_file(file_path: Path) -> bool:
    """Check if a file should be excluded from the archive."""
    # Check if any parent directory should be excluded
    for parent in file_path.parents:
        if parent.name in EXCLUDE_DIRS:
            return True

    # Check if filename matches exclude patterns
    for pattern in EXCLUDE_PATTERNS:
        if file_path.match(pattern):
            return True

    return False

def create_project_zip():
    """Create a zip archive of the project."""
    project_root = Path(__file__).parent
    zip_filename = 'xemm_rust.zip'
    zip_path = project_root / zip_filename

    # Remove old zip if it exists
    if zip_path.exists():
        print(f"Removing existing archive: {zip_filename}")
        zip_path.unlink()
        print()

    print(f"Creating portable archive: {zip_filename}")
    print(f"Project root: {project_root}")
    print()

    files_added = 0
    files_excluded = 0
    total_size = 0

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through all files in the project
        for root, dirs, files in os.walk(project_root):
            root_path = Path(root)

            # Remove excluded directories from dirs to prevent os.walk from entering them
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

            for file in files:
                file_path = root_path / file

                # Skip the zip file we're creating
                if file_path.name == 'xemm_rust.zip':
                    continue

                # Check if file should be excluded
                if should_exclude_file(file_path):
                    files_excluded += 1
                    continue

                # Add file to zip with relative path
                arcname = file_path.relative_to(project_root)
                zipf.write(file_path, arcname)

                file_size = file_path.stat().st_size
                total_size += file_size
                files_added += 1

                # Print progress for larger files
                if file_size > 100_000:  # > 100KB
                    print(f"  Added: {arcname} ({file_size:,} bytes)")

    zip_size = zip_path.stat().st_size

    print()
    print("=" * 60)
    print(f"Archive created successfully: {zip_filename}")
    print(f"Files added: {files_added}")
    print(f"Files excluded: {files_excluded}")
    print(f"Total uncompressed size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
    print(f"Archive size: {zip_size:,} bytes ({zip_size / 1024 / 1024:.2f} MB)")
    print(f"Compression ratio: {(1 - zip_size / total_size) * 100:.1f}%")
    print("=" * 60)
    print()
    print("Excluded patterns:")
    print(f"  Directories: {', '.join(sorted(EXCLUDE_DIRS))}")
    print(f"  File patterns: {', '.join(sorted(EXCLUDE_PATTERNS))}")
    print()

if __name__ == '__main__':
    create_project_zip()
