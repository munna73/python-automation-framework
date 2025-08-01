#!/bin/bash
# Script to fix pip install error due to path-based .whl file

source venv/bin/activate

# Re-freeze clean requirements
pip freeze | grep -vE '^(--|file:|/)' > requirements.txt

# Install clean packages
pip install -r requirements.txt

echo "✅ Fixed requirements.txt and installed packages successfully"
