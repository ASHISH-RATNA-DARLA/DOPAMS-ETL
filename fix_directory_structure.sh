#!/bin/bash
# Fix directory structure mismatch on NFS mount

set -e  # Exit on any error

echo "======================================================================="
echo "ETL DIRECTORY STRUCTURE FIX"
echo "======================================================================="
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

NFS_MOUNT="/mnt/shared-etl-files"

# Step 1: Verify NFS mount is accessible
echo -e "${YELLOW}[1/5] Checking NFS mount...${NC}"
if [ -d "$NFS_MOUNT" ]; then
    echo -e "${GREEN}✓ Mount point exists: $NFS_MOUNT${NC}"
else
    echo -e "${RED}✗ ERROR: Mount point does not exist: $NFS_MOUNT${NC}"
    exit 1
fi

# Step 2: Check current directory structure
echo
echo -e "${YELLOW}[2/5] Current directory structure:${NC}"
ls -la "$NFS_MOUNT" | grep "^d" | awk '{print "  " $9}'

# Step 3: Check for missing directories
echo
echo -e "${YELLOW}[3/5] Checking for missing directories...${NC}"

MISSING_DIRS=()

if [ ! -d "$NFS_MOUNT/mo_seizures" ]; then
    echo -e "${RED}✗ mo_seizures/ missing (410 DB records expect this)${NC}"
    MISSING_DIRS+=("$NFS_MOUNT/mo_seizures")
else
    echo -e "${GREEN}✓ mo_seizures/ exists${NC}"
fi

if [ ! -d "$NFS_MOUNT/fsl_case_property" ]; then
    echo -e "${RED}✗ fsl_case_property/ missing (1 DB record expects this)${NC}"
    MISSING_DIRS+=("$NFS_MOUNT/fsl_case_property")
else
    echo -e "${GREEN}✓ fsl_case_property/ exists${NC}"
fi

# Step 4: Create missing directories
if [ ${#MISSING_DIRS[@]} -eq 0 ]; then
    echo
    echo -e "${GREEN}All directories exist!${NC}"
else
    echo
    echo -e "${YELLOW}[4/5] Creating missing directories...${NC}"
    for dir in "${MISSING_DIRS[@]}"; do
        echo "  Creating: $dir"
        mkdir -p "$dir" 2>/dev/null || sudo mkdir -p "$dir"
        chmod 777 "$dir" 2>/dev/null || sudo chmod 777 "$dir"
        echo -e "${GREEN}  ✓ Created${NC}"
    done
fi

# Step 5: Verify all expected directories exist and have correct permissions
echo
echo -e "${YELLOW}[5/5] Final verification:${NC}"

EXPECTED_DIRS=(
    "crimes"
    "person/media"
    "person/identitydetails"
    "property"
    "interrogations/media"
    "interrogations/interrogationreport"
    "interrogations/dopamsdata"
    "mo_seizures"
    "chargesheets"
    "fsl_case_property"
)

ALL_OK=true
for dir in "${EXPECTED_DIRS[@]}"; do
    FULL_PATH="$NFS_MOUNT/$dir"
    if [ -d "$FULL_PATH" ]; then
        FILE_COUNT=$(ls -q "$FULL_PATH" 2>/dev/null | wc -l)
        echo -e "${GREEN}✓ $dir${NC} ($FILE_COUNT files)"
    else
        echo -e "${RED}✗ $dir (MISSING)${NC}"
        ALL_OK=false
    fi
done

echo
echo "======================================================================="
if [ "$ALL_OK" = true ]; then
    echo -e "${GREEN}✓ Directory structure is now correct!${NC}"
    echo "======================================================================="
    echo
    echo "Next steps:"
    echo "1. Run diagnostic: python3 etl-files/diagnose_missing_files.py"
    echo "2. Monitor progress for 48-72 hours"
    echo "3. When 60%+ files available: Re-run Order 29"
    echo
else
    echo -e "${RED}✗ Some directories still missing${NC}"
    echo "======================================================================="
    exit 1
fi
