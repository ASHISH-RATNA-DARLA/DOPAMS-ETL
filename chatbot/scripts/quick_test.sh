#!/bin/bash
# Quick test script - runs a small batch of tests

echo "🚀 DOPAMAS Chatbot Quick Test"
echo "=============================="
echo ""

# Check if chatbot is running
echo "Checking if chatbot is running..."
if curl -s http://localhost:5008/api/health > /dev/null; then
    echo "✅ Chatbot is running"
else
    echo "❌ Chatbot is not running! Please start it first."
    exit 1
fi

echo ""
echo "Running first 10 questions as a test..."
echo ""

# Run test with limit
python scripts/test_runner.py --limit 10

echo ""
echo "Test complete! Check test_results/ for output."
echo ""
echo "To analyze results:"
echo "  python scripts/analyze_results.py test_results/test_results_*.json"

