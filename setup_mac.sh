#!/bin/bash

#################################################################################
# Setup script for Python Test Automation Framework on macOS
# This script installs all necessary system and Python dependencies
#################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo -e "\n${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if running on macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
    print_error "This script is designed for macOS only!"
    exit 1
fi

print_header "🍎 Python Test Automation Framework Setup for macOS"

# Check for Homebrew
print_header "Checking Homebrew Installation"
if ! command -v brew &> /dev/null; then
    print_warning "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    
    # Add Homebrew to PATH for Apple Silicon Macs
    if [[ -f "/opt/homebrew/bin/brew" ]]; then
        echo 'eval "$(/opt/homebrew/bin/brew shellenv)"' >> ~/.zprofile
        eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
else
    print_success "Homebrew is installed"
    brew update
fi

# Install System Dependencies
print_header "Installing System Dependencies"

# Core dependencies
print_info "Installing Python and core tools..."
brew install python@3.9 python@3.10 python@3.11 || true
brew install git tree make wget curl jq || true

# Database dependencies
print_info "Installing database dependencies..."
brew install postgresql@14 || true
brew install mysql || true
brew install mongodb-community || true
brew install libpq || true  # PostgreSQL client library

# Additional dependencies for Python packages
print_info "Installing build dependencies..."
brew install openssl readline sqlite3 xz zlib || true
brew install pkg-config || true

# AWS CLI (useful for AWS testing)
print_info "Installing AWS CLI..."
brew install awscli || true

# Message Queue dependencies
print_info "Installing message queue tools..."
brew install rabbitmq || true

print_success "System dependencies installed"

# Install Oracle Instant Client (optional, with error handling)
print_header "Installing Oracle Instant Client (Optional)"
if ! brew list instantclient-basic &> /dev/null; then
    print_info "Installing Oracle Instant Client..."
    brew tap InstantClientTap/instantclient || true
    brew install instantclient-basic || print_warning "Oracle client installation failed - continuing without it"
else
    print_success "Oracle Instant Client already installed"
fi

# Setup Python Environment
print_header "Setting Up Python Environment"

# Determine Python version
if command -v python3.9 &> /dev/null; then
    PYTHON_CMD="python3.9"
elif command -v python3.10 &> /dev/null; then
    PYTHON_CMD="python3.10"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
else
    print_error "Python 3 not found!"
    exit 1
fi

print_info "Using Python: $($PYTHON_CMD --version)"

# Create virtual environment
print_info "Creating virtual environment..."
$PYTHON_CMD -m venv venv

# Activate virtual environment
print_info "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
print_info "Upgrading pip..."
pip install --upgrade pip setuptools wheel

# Install Python packages
print_header "Installing Python Packages"

# Check if requirements.txt exists
if [ -f "requirements.txt" ]; then
    print_info "Installing packages from requirements.txt..."
    pip install -r requirements.txt
    print_success "Python packages installed"
else
    print_warning "requirements.txt not found. Installing common packages..."
    
    # Install essential packages manually
    pip install behave==1.2.6
    pip install requests==2.31.0
    pip install boto3==1.34.14
    pip install pymongo==4.6.1
    pip install psycopg2-binary==2.9.9
    pip install pandas==2.1.4
    pip install python-dotenv==1.0.0
    pip install PyYAML==6.0.1
    pip install colorlog==6.8.0
    pip install pytest==7.4.3
    pip install allure-behave==2.13.2
    
    # Create requirements.txt
    pip freeze > requirements.txt
    print_success "Created requirements.txt with installed packages"
fi

# Setup environment variables for Oracle
print_header "Setting Up Environment Variables"

# Add to shell profile
SHELL_PROFILE="$HOME/.zshrc"
if [ ! -f "$SHELL_PROFILE" ]; then
    SHELL_PROFILE="$HOME/.bash_profile"
fi

print_info "Adding environment variables to $SHELL_PROFILE..."

# Check if already added
if ! grep -q "DYLD_LIBRARY_PATH" "$SHELL_PROFILE"; then
    cat >> "$SHELL_PROFILE" << 'EOL'

# Oracle Instant Client
export DYLD_LIBRARY_PATH=/usr/local/lib:$DYLD_LIBRARY_PATH

# PostgreSQL
export PATH="/usr/local/opt/postgresql@14/bin:$PATH"

# Python Test Automation
alias activate-test='source venv/bin/activate'
EOL
    print_success "Environment variables added"
else
    print_info "Environment variables already configured"
fi

# Create project structure if it doesn't exist
print_header "Verifying Project Structure"

directories=(
    "api"
    "aws" 
    "config"
    "data/input"
    "data/schemas"
    "db"
    "features/api"
    "features/aws"
    "features/database/sql"
    "features/database/nosql"
    "features/database/cross_database"
    "features/mq"
    "features/steps"
    "logs/api"
    "logs/application"
    "logs/database"
    "logs/mq"
    "logs/test_execution"
    "mq"
    "output/exports"
    "output/junit"
    "output/reports"
    "scripts"
    "tests/unit"
    "utils"
)

for dir in "${directories[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        touch "$dir/.gitkeep"
        print_info "Created directory: $dir"
    fi
done

# Create .env file if it doesn't exist
if [ ! -f ".env" ] && [ -f ".env.template" ]; then
    cp .env.template .env
    print_success "Created .env file from template"
    print_warning "Please update .env with your actual credentials"
elif [ ! -f ".env" ]; then
    print_warning "No .env.template found. Creating basic .env file..."
    cat > .env << 'EOL'
# Database Passwords
DEV_ORACLE_PWD=your_password
DEV_POSTGRES_PWD=your_password
DEV_MONGODB_PWD=your_password

# AWS Credentials
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1

# API Configuration
API_BASE_URL=http://localhost:8080
API_TOKEN=your_token
EOL
    print_success "Created basic .env file"
    print_warning "Please update .env with your actual credentials"
fi

# Create Makefile if it doesn't exist
if [ ! -f "Makefile" ]; then
    print_info "Creating Makefile..."
    cat > Makefile << 'EOL'
.PHONY: help setup test clean

help:
	@echo "Available commands:"
	@echo "  make setup  - Install dependencies"
	@echo "  make test   - Run all tests"
	@echo "  make clean  - Clean up files"

setup:
	pip install -r requirements.txt

test:
	behave

clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +
EOL
    print_success "Created Makefile"
fi

# Start services
print_header "Starting Services (Optional)"

print_info "Starting PostgreSQL..."
brew services start postgresql@14 || print_warning "PostgreSQL start failed"

print_info "Starting MongoDB..."
brew services start mongodb-community || print_warning "MongoDB start failed"

print_info "Starting RabbitMQ..."
brew services start rabbitmq || print_warning "RabbitMQ start failed"

# Verify installations
print_header "Verifying Installations"

# Check Python packages
print_info "Checking Python packages..."
python -c "import behave; print(f'✅ Behave {behave.__version__}')" || print_error "Behave not installed"
python -c "import requests; print('✅ Requests installed')" || print_error "Requests not installed"
python -c "import boto3; print('✅ Boto3 installed')" || print_error "Boto3 not installed"
python -c "import pymongo; print('✅ PyMongo installed')" || print_error "PyMongo not installed"
python -c "import psycopg2; print('✅ psycopg2 installed')" || print_error "psycopg2 not installed"

# Check system services
print_info "Checking services..."
brew services list | grep -E "(postgresql|mongodb|rabbitmq)"

# Final instructions
print_header "✨ Setup Complete!"

echo -e "${GREEN}Your Python test automation framework is ready!${NC}\n"
echo "Next steps:"
echo "1. Activate the virtual environment:"
echo "   ${BLUE}source venv/bin/activate${NC}"
echo ""
echo "2. Update your credentials in .env file:"
echo "   ${BLUE}nano .env${NC}"
echo ""
echo "3. Run a quick test:"
echo "   ${BLUE}behave --tags=@smoke --dry-run${NC}"
echo ""
echo "4. View all available make commands:"
echo "   ${BLUE}make help${NC}"
echo ""
echo "Useful aliases added:"
echo "  ${BLUE}activate-test${NC} - Activate the test virtual environment"
echo ""
echo "Services status:"
echo "  PostgreSQL: ${BLUE}brew services info postgresql@14${NC}"
echo "  MongoDB: ${BLUE}brew services info mongodb-community${NC}"
echo "  RabbitMQ: ${BLUE}brew services info rabbitmq${NC}"
echo ""
print_warning "Remember to source your shell profile or restart terminal:"
echo "  ${BLUE}source $SHELL_PROFILE${NC}"

# Create a quick test script
cat > quick_test.sh << 'EOL'
#!/bin/bash
source venv/bin/activate
echo "Running quick validation..."
python -c "print('✅ Python environment OK')"
python -c "import behave; print('✅ Behave installed')"
python -c "from db.database_connector import *; print('✅ Database modules OK')"
python -c "from api.rest_client import *; print('✅ API modules OK')"
behave --version
echo "✅ All checks passed!"
EOL
chmod +x quick_test.sh

print_success "Created quick_test.sh - Run ./quick_test.sh to verify setup"