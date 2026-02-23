#!/bin/bash
# Package test runner files for server deployment

echo "📦 Packaging test runner for server deployment..."
echo ""

# Create package directory
PACKAGE_DIR="test_runner_package"
mkdir -p "$PACKAGE_DIR/scripts"

# Copy required files
echo "Copying files..."
cp scripts/test_runner.py "$PACKAGE_DIR/scripts/"
cp scripts/analyze_results.py "$PACKAGE_DIR/scripts/"
cp questions.txt "$PACKAGE_DIR/"

# Create README
cat > "$PACKAGE_DIR/README.md" << 'EOF'
# Test Runner Package for Server

## Files Included
- scripts/test_runner.py - Main test runner
- scripts/analyze_results.py - Results analyzer
- questions.txt - Test questions

## Setup on Server

1. Extract this package on your server
2. Ensure Python 3 and `requests` library are installed
3. Run tests:

```bash
python3 scripts/test_runner.py \
    --api-url http://localhost:5008 \
    --log-file /path/to/app.log \
    --limit 10
```

## Notes
- CSV export files (postgres_exports/, mongo_exports/) are NOT needed for testing
- They're only needed if you want to import test data separately
- Test runner only needs network access to chatbot API and log file
EOF

# Create tarball
echo "Creating tarball..."
tar -czf test_runner_package.tar.gz "$PACKAGE_DIR/"

# Show package contents
echo ""
echo "✅ Package created: test_runner_package.tar.gz"
echo ""
echo "Package contents:"
tar -tzf test_runner_package.tar.gz | head -10
echo "..."
echo ""
echo "📦 Package size:"
du -h test_runner_package.tar.gz
echo ""
echo "To deploy:"
echo "  scp test_runner_package.tar.gz user@server:/path/to/destination/"
echo "  # On server: tar -xzf test_runner_package.tar.gz"

