#!/bin/bash

# Activate existing virtual environment
source venv/bin/activate

# Freeze current environment into requirements.txt
pip freeze > requirements.txt

echo "📄 requirements.txt updated with current packages"
