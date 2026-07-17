#!/usr/bin/env bash
# =============================================================================
# CardShield — Build Script
# Packages all project files into a distributable compressed archive.
#
# Usage:
#   chmod +x build.sh
#   ./build.sh
#
# Output:
#   cardshield_project.tar.gz  (in the parent directory of this script)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="cardshield"
OUTPUT_NAME="cardshield_project"

echo "=================================================="
echo "  CardShield — Project Packager"
echo "=================================================="
echo ""

# Move to parent directory so the archive includes the cardshield/ folder
cd "$(dirname "$SCRIPT_DIR")"

ARCHIVE="${OUTPUT_NAME}.tar.gz"

echo "[1/3] Creating archive: ${ARCHIVE}"
tar \
  --exclude="*/__pycache__" \
  --exclude="*.pyc" \
  --exclude="*.pyo" \
  --exclude=".DS_Store" \
  --exclude="*.egg-info" \
  --exclude=".env" \
  -czf "${ARCHIVE}" \
  "${PROJECT_NAME}/"

echo "[2/3] Verifying archive contents…"
tar -tzf "${ARCHIVE}" | head -60

echo ""
echo "[3/3] Done!"
echo ""
echo "  Archive : $(pwd)/${ARCHIVE}"
echo "  Size    : $(du -sh "${ARCHIVE}" | cut -f1)"
echo ""
echo "  To extract:"
echo "    tar -xzf ${ARCHIVE}"
echo ""
echo "  Next steps:"
echo "    1. cd ${PROJECT_NAME}"
echo "    2. cp .env.example .env && edit .env"
echo "    3. docker-compose up -d"
echo "    4. See README.md for full startup guide"
echo "=================================================="
