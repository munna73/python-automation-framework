#!/bin/bash

# Test Automation Framework - Test Runner with HTML Reports
# Usage: ./run_tests.sh [options]

echo "🚀 Test Automation Framework - HTML Report Runner"
echo "=================================================="

# Default values
FEATURES_DIR="features/"
TAGS=""
TITLE="Database Test Automation Report"
OUTPUT_DIR="output/reports"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --features)
            FEATURES_DIR="$2"
            shift 2
            ;;
        --tags)
            TAGS="--tags $2"
            shift 2
            ;;
        --title)
            TITLE="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --features DIR    Features directory or specific .feature files"
            echo "  --tags TAGS       Tags to include/exclude (e.g., '@database')"
            echo "  --title TITLE     Report title"
            echo "  --output DIR      Output directory for reports"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                                    # Run all tests"
            echo "  $0 --features features/database/     # Run database tests only"
            echo "  $0 --tags '@database'                # Run tests tagged with @database"
            echo "  $0 --title 'My Test Report'          # Custom report title"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Create output directories
mkdir -p "$OUTPUT_DIR"
mkdir -p output/junit

# Set environment variables for better test execution
export PYTHONPATH=".:$PYTHONPATH"

echo "📋 Configuration:"
echo "   Features: $FEATURES_DIR"
echo "   Tags: ${TAGS:-'(all tests)'}"
echo "   Title: $TITLE"
echo "   Output: $OUTPUT_DIR"
echo ""

# Run tests with Python script
echo "🔄 Starting test execution..."
python3 scripts/run_tests_with_reports.py $FEATURES_DIR $TAGS --title "$TITLE" --output "$OUTPUT_DIR"

# Capture exit code
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Test execution completed successfully!"
    echo "📊 Check the HTML report in: $OUTPUT_DIR/"
else
    echo "⚠️ Test execution completed with failures."
    echo "📊 Check the HTML report for details: $OUTPUT_DIR/"
fi

echo ""
echo "📁 Generated Reports:"
echo "   - HTML Report: $OUTPUT_DIR/test_report_*.html"
echo "   - JSON Results: $OUTPUT_DIR/behave_results_*.json"
echo "   - JUnit XML: output/junit/"
echo "   - Application Logs: logs/test_automation.log"

exit $EXIT_CODE