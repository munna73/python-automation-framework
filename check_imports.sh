#!/bin/bash

# This script checks all Python files for valid local and installed package imports.
# It simulates a Python interpreter's behavior to catch ImportErrors.

# --- Configuration ---
# Define the output report file
REPORT_FILE="import_analysis_report.txt"
# Get the project root directory
PROJECT_ROOT=$(pwd)
# A simple list of common Python import errors to look for
IMPORT_ERRORS=("ImportError" "ModuleNotFoundError")

# Clear the previous report
> "$REPORT_FILE"

echo "======================================================" >> "$REPORT_FILE"
echo "  Python Import Analysis Report" >> "$REPORT_FILE"
echo "  Timestamp: $(date)" >> "$REPORT_FILE"
echo "======================================================" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "Starting analysis of Python imports in the project..."
echo "Report will be saved to $REPORT_FILE"
echo ""

# Find all Python files, excluding the virtual environment
PYTHON_FILES=$(find "$PROJECT_ROOT" -name "*.py" -not -path "$PROJECT_ROOT/path/to/venv/*")

if [ -z "$PYTHON_FILES" ]; then
    echo "No Python files found to analyze. Exiting." >> "$REPORT_FILE"
    echo "No Python files found to analyze. Exiting."
    exit 0
fi

ISSUE_FOUND=false

# --- Loop through each Python file to test its imports ---
for FILE in $PYTHON_FILES; do
    # Extract import statements from the file, excluding __future__ imports
    IMPORTS=$(grep -E '^(from|import) ' "$FILE" | grep -v '__future__')

    if [ -z "$IMPORTS" ]; then
        continue # No imports found in this file, skip to the next
    fi

    # Create a temporary file to hold the test script for this file
    TEMP_PY_FILE=$(mktemp)
    echo "$IMPORTS" >> "$TEMP_PY_FILE"

    # Use the python executable from the venv if available, otherwise use default
    PYTHON_CMD="python"
    if [ -f "$PROJECT_ROOT/path/to/venv/bin/python" ]; then
        PYTHON_CMD="$PROJECT_ROOT/path/to/venv/bin/python"
    fi

    # Run the temporary Python script with imports.
    # We set PYTHONPATH to the project root so Python can find local modules.
    # The output and errors are redirected to the report file for analysis.
    if ! PYTHONPATH="$PROJECT_ROOT" "$PYTHON_CMD" -c "exec(open('$TEMP_PY_FILE').read())" 2> /tmp/import_error_output; then
        
        # Check if the error is a known import error
        ERROR_MESSAGE=$(cat /tmp/import_error_output)
        IS_IMPORT_ERROR=false
        for ERROR_TYPE in "${IMPORT_ERRORS[@]}"; do
            if [[ "$ERROR_MESSAGE" =~ "$ERROR_TYPE" ]]; then
                IS_IMPORT_ERROR=true
                break
            fi
        done

        if [ "$IS_IMPORT_ERROR" = true ]; then
            echo "--------------------------------------------------------" >> "$REPORT_FILE"
            echo "ISSUE FOUND in: $FILE" >> "$REPORT_FILE"
            echo "  Error details:" >> "$REPORT_FILE"
            cat /tmp/import_error_output >> "$REPORT_FILE"
            echo "" >> "$REPORT_FILE"
            echo "  Suggestions for fix:" >> "$REPORT_FILE"
            echo "  - Ensure the imported module/package exists at the specified path." >> "$REPORT_FILE"
            echo "  - Check for typos in the import statement." >> "$REPORT_FILE"
            echo "  - Verify that the imported file or directory has an '__init__.py' file to be treated as a package." >> "$REPORT_FILE"
            echo "  - Run 'pip install -r requirements.txt' to install any missing third-party dependencies." >> "$REPORT_FILE"
            echo "--------------------------------------------------------" >> "$REPORT_FILE"
            echo "" >> "$REPORT_FILE"
            ISSUE_FOUND=true
        fi
    fi

    # Clean up the temporary file
    rm "$TEMP_PY_FILE"
    rm /tmp/import_error_output
done

echo "======================================================" >> "$REPORT_FILE"
if [ "$ISSUE_FOUND" = true ]; then
    echo "  Import analysis completed with ERRORS." >> "$REPORT_FILE"
    echo "  Please check '$REPORT_FILE' for details." >> "$REPORT_FILE"
    echo ""
    echo "Import analysis completed with ERRORS. Please check '$REPORT_FILE' for details."
else
    echo "  Import analysis completed successfully. No import issues found." >> "$REPORT_FILE"
    echo "  Check '$REPORT_FILE' for confirmation." >> "$REPORT_FILE"
    echo ""
    echo "Import analysis completed successfully. No import issues found."
fi
echo "======================================================" >> "$REPORT_FILE"
