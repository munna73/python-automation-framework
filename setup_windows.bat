@echo off
REM Windows Setup Script for Test Automation Framework

echo.
echo 🛠️ Setting up Test Automation Framework for Windows
echo ====================================================

REM Check if Python is installed
echo 🔍 Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please install Python first:
    echo    1. Go to https://python.org/downloads/
    echo    2. Download Python 3.8 or later
    echo    3. During installation, check "Add Python to PATH"
    echo    4. Run this setup script again
    pause
    exit /b 1
)

python --version
echo ✅ Python is available

REM Check if pip is available
echo.
echo 🔍 Checking pip installation...
pip --version >nul 2>&1
if errorlevel 1 (
    echo ❌ pip not found. Installing pip...
    python -m ensurepip --upgrade
)
echo ✅ pip is available

REM Create virtual environment (recommended for Windows)
echo.
echo 🔧 Setting up virtual environment...
if not exist "venv" (
    echo Creating virtual environment...
    python -m venv venv
    echo ✅ Virtual environment created
) else (
    echo ✅ Virtual environment already exists
)

REM Activate virtual environment
echo.
echo 🔧 Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ❌ Failed to activate virtual environment
    exit /b 1
)
echo ✅ Virtual environment activated

REM Upgrade pip
echo.
echo 🔧 Upgrading pip...
python -m pip install --upgrade pip

REM Install requirements
echo.
echo 📦 Installing Python dependencies...
if exist "requirements.txt" (
    pip install -r requirements.txt
    echo ✅ Requirements installed
) else (
    echo ❌ requirements.txt not found
    exit /b 1
)

REM Create necessary directories
echo.
echo 📁 Creating directory structure...
if not exist "logs" mkdir logs
if not exist "output" mkdir output
if not exist "output\reports" mkdir output\reports
if not exist "output\junit" mkdir output\junit
if not exist "output\exports" mkdir output\exports
echo ✅ Directory structure created

REM Set up configuration
echo.
echo ⚙️ Setting up configuration...
if not exist "config\config.ini" (
    if exist "config\sampleconfig.ini" (
        copy "config\sampleconfig.ini" "config\config.ini"
        echo ✅ Configuration file created from sample
        echo ⚠️ Please edit config\config.ini with your database settings
    ) else (
        echo ❌ No sample configuration found
    )
) else (
    echo ✅ Configuration file already exists
)

REM Test the setup
echo.
echo 🧪 Testing framework setup...
python -c "
import sys
print('Python version:', sys.version)

# Test imports
try:
    import behave
    print('✅ Behave imported successfully')
except ImportError as e:
    print('❌ Behave import failed:', e)

try:
    import pandas as pd
    print('✅ Pandas imported successfully')
except ImportError as e:
    print('❌ Pandas import failed:', e)

try:
    from utils.logger import logger
    logger.info('Testing logger functionality')
    print('✅ Logger working correctly')
except Exception as e:
    print('❌ Logger test failed:', e)

try:
    from utils.html_reporter import HTMLReportGenerator
    print('✅ HTML Reporter available')
except Exception as e:
    print('❌ HTML Reporter test failed:', e)
"

echo.
echo 🎉 Setup completed!
echo.
echo 📋 Next Steps:
echo    1. Edit config\config.ini with your database connection details
echo    2. Set environment variables for database passwords:
echo       set DEV_ORACLE_PWD=your_oracle_password
echo       set DEV_POSTGRES_PWD=your_postgres_password
echo    3. Run tests: run_tests.bat
echo.
echo 💡 To activate virtual environment manually:
echo    venv\Scripts\activate.bat
echo.
echo 📖 For more information, see HTML_REPORTING.md

pause