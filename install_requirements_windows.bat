@echo off
REM Install Python requirements for Windows

echo.
echo 📦 Installing Python Requirements for Windows
echo ============================================

REM Check if we're in virtual environment
if defined VIRTUAL_ENV (
    echo ✅ Virtual environment active: %VIRTUAL_ENV%
) else (
    echo ⚠️ No virtual environment detected. Recommended to use:
    echo    python -m venv venv
    echo    venv\Scripts\activate.bat
    echo.
    echo Continue anyway? [Y/N]
    set /p choice=
    if /i "%choice%" neq "y" exit /b 1
)

REM Upgrade pip first
echo.
echo 🔧 Upgrading pip...
python -m pip install --upgrade pip
if errorlevel 1 (
    echo ❌ Failed to upgrade pip
    exit /b 1
)

REM Install core requirements
echo.
echo 📦 Installing core requirements...
pip install behave==1.2.6
pip install jsonschema==4.17.3
pip install pandas==2.0.3
pip install requests==2.31.0
pip install SQLAlchemy==2.0.19
pip install PyYAML==6.0.1

REM Install Windows-specific database drivers
echo.
echo 🔧 Installing database drivers for Windows...

REM PostgreSQL driver
pip install psycopg2-binary
if errorlevel 1 (
    echo ⚠️ psycopg2-binary failed, trying psycopg2...
    pip install psycopg2
)

REM Oracle driver (if needed)
echo.
echo Installing Oracle driver (optional)...
pip install cx_Oracle
if errorlevel 1 (
    echo ⚠️ Oracle driver installation failed
    echo    This is optional unless you're testing Oracle databases
    echo    You may need to install Oracle Instant Client first
)

REM MongoDB driver (if needed)
echo.
echo Installing MongoDB driver (optional)...
pip install pymongo
if errorlevel 1 (
    echo ⚠️ MongoDB driver installation failed
)

REM Message Queue drivers (optional)
echo.
echo Installing MQ drivers (optional)...
pip install kafka-python
if errorlevel 1 (
    echo ⚠️ Kafka driver installation failed
)

REM Try to install IBM MQ (often fails on Windows)
pip install pymqi
if errorlevel 1 (
    echo ⚠️ IBM MQ driver installation failed
    echo    This is optional unless you're testing IBM MQ
)

REM AWS SDK (if needed)
echo.
echo Installing AWS SDK (optional)...
pip install boto3
if errorlevel 1 (
    echo ⚠️ AWS SDK installation failed
)

echo.
echo ✅ Core requirements installation completed!
echo.
echo 🧪 Testing imports...
python -c "
import sys
print('Testing Python imports...')

# Test core imports
modules = [
    ('behave', 'Behave BDD framework'),
    ('pandas', 'Data analysis library'),
    ('requests', 'HTTP requests library'),
    ('json', 'JSON handling'),
    ('sqlalchemy', 'Database toolkit')
]

for module, desc in modules:
    try:
        __import__(module)
        print(f'✅ {module} - {desc}')
    except ImportError as e:
        print(f'❌ {module} - {desc}: {e}')

# Test optional imports
optional_modules = [
    ('psycopg2', 'PostgreSQL driver'),
    ('cx_Oracle', 'Oracle driver'), 
    ('pymongo', 'MongoDB driver'),
    ('boto3', 'AWS SDK')
]

print('\nOptional modules:')
for module, desc in optional_modules:
    try:
        __import__(module)
        print(f'✅ {module} - {desc}')
    except ImportError:
        print(f'⚠️ {module} - {desc}: Not installed (optional)')

print('\n✅ Import test completed!')
"

echo.
echo 📋 Next Steps:
echo    1. Configure your database connections in config\config.ini
echo    2. Set environment variables for passwords:
echo       set DEV_ORACLE_PWD=your_password
echo       set DEV_POSTGRES_PWD=your_password
echo    3. Run tests: run_tests.bat
echo.

pause