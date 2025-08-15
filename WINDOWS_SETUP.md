# Windows Setup Guide

## Prerequisites

### 1. **Python Installation**
- Download Python 3.8 or later from [python.org](https://python.org/downloads/)
- **Important**: During installation, check ☑️ "Add Python to PATH"
- Verify installation:
  ```cmd
  python --version
  pip --version
  ```

### 2. **Git (Optional but Recommended)**
- Download from [git-scm.com](https://git-scm.com/download/win)
- Or use GitHub Desktop for GUI

### 3. **Database Drivers (If using database features)**
- **Oracle**: Download Oracle Instant Client
- **PostgreSQL**: Install psycopg2 (included in requirements)
- **SQL Server**: Install pyodbc

## Quick Setup

### Option 1: Automated Setup (Recommended)
```cmd
# Run the automated setup script
setup_windows.bat
```

### Option 2: Manual Setup
```cmd
# 1. Create virtual environment
python -m venv venv

# 2. Activate virtual environment
venv\Scripts\activate.bat

# 3. Upgrade pip
python -m pip install --upgrade pip

# 4. Install dependencies
pip install -r requirements.txt

# 5. Create directories
mkdir logs output output\reports output\junit
```

## Configuration

### 1. **Database Configuration**
Edit `config\config.ini`:
```ini
[DEFAULT]
log_level = INFO

[DEV_ORACLE]
host = your-oracle-host
port = 1521
service_name = your_service
username = your_username
password = DEV_ORACLE_PWD

[DEV_POSTGRES]
host = your-postgres-host
port = 5432
database = your_database
username = your_username
password = DEV_POSTGRES_PWD
```

### 2. **Environment Variables**
Set database passwords as environment variables:

**Command Prompt:**
```cmd
set DEV_ORACLE_PWD=your_oracle_password
set DEV_POSTGRES_PWD=your_postgres_password
```

**PowerShell:**
```powershell
$env:DEV_ORACLE_PWD="your_oracle_password"
$env:DEV_POSTGRES_PWD="your_postgres_password"
```

**Permanent (System Environment Variables):**
1. Right-click "This PC" → Properties → Advanced System Settings
2. Click "Environment Variables"
3. Add new variables under "User variables"

## Running Tests

### 1. **Simple Execution**
```cmd
# Run all tests
run_tests.bat

# Run specific features
run_tests.bat --features features\database\

# Run with tags
run_tests.bat --tags "@database"

# Custom report title
run_tests.bat --title "Windows Test Report"
```

### 2. **Python Script Execution**
```cmd
# Activate virtual environment first
venv\Scripts\activate.bat

# Run tests with Python script
python scripts\run_tests_with_reports.py features\ --title "Windows Tests"
```

### 3. **Direct Behave Execution**
```cmd
# Install behave first if not done
pip install behave

# Run behave directly
behave features\

# Generate HTML report from JSON
python utils\html_reporter.py output\reports\behave_results.json output\reports\my_report.html
```

## Windows-Specific Considerations

### 1. **Path Separators**
- Use backslashes (`\`) in batch files
- Use forward slashes (`/`) in Python code (Python handles conversion)
- File paths in config files can use either format

### 2. **Virtual Environment**
```cmd
# Activation
venv\Scripts\activate.bat

# Deactivation
deactivate
```

### 3. **File Permissions**
- Ensure scripts have execution permissions
- Run Command Prompt as Administrator if needed

### 4. **Database Drivers**

**Oracle:**
```cmd
# Download Oracle Instant Client
# Add to PATH or set ORACLE_HOME
pip install cx_Oracle
```

**SQL Server:**
```cmd
pip install pyodbc
# Or for SQL Server specifically:
pip install mssql-cli
```

## Troubleshooting

### 1. **Python Not Found**
```cmd
# Add Python to PATH manually
set PATH=%PATH%;C:\Python39;C:\Python39\Scripts

# Or reinstall Python with "Add to PATH" checked
```

### 2. **Virtual Environment Issues**
```cmd
# Delete and recreate venv
rmdir /s venv
python -m venv venv
venv\Scripts\activate.bat
pip install -r requirements.txt
```

### 3. **Permission Errors**
```cmd
# Run Command Prompt as Administrator
# Or check Windows Defender/Antivirus settings
```

### 4. **Import Errors**
```cmd
# Check PYTHONPATH
set PYTHONPATH=.;%PYTHONPATH%

# Or add to the script
set PYTHONPATH=%cd%;%PYTHONPATH%
```

### 5. **Database Connection Issues**
```cmd
# Test database connectivity
telnet your-database-host 1521  # Oracle
telnet your-database-host 5432  # PostgreSQL

# Check firewall settings
# Verify VPN connection if needed
```

## PowerShell Alternative

If you prefer PowerShell, here's the equivalent commands:

### Setup:
```powershell
# Create virtual environment
python -m venv venv

# Activate
.\venv\Scripts\Activate.ps1

# If execution policy error:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Install requirements
pip install -r requirements.txt
```

### Running Tests:
```powershell
# Run tests
python scripts\run_tests_with_reports.py features\ --title "PowerShell Test Report"

# Set environment variables
$env:DEV_ORACLE_PWD="your_password"
$env:DEV_POSTGRES_PWD="your_password"
```

## CI/CD Integration (Windows)

### GitHub Actions:
```yaml
name: Windows Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: windows-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run tests
      env:
        DEV_ORACLE_PWD: ${{ secrets.DEV_ORACLE_PWD }}
        DEV_POSTGRES_PWD: ${{ secrets.DEV_POSTGRES_PWD }}
      run: |
        python scripts\run_tests_with_reports.py features\ --title "Windows CI Tests"
    
    - name: Upload HTML Report
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: test-report-windows
        path: output\reports\test_report_*.html
```

### Jenkins (Windows):
```groovy
pipeline {
    agent { 
        label 'windows' 
    }
    environment {
        DEV_ORACLE_PWD = credentials('dev-oracle-password')
        DEV_POSTGRES_PWD = credentials('dev-postgres-password')
    }
    stages {
        stage('Setup') {
            steps {
                bat 'python -m pip install --upgrade pip'
                bat 'pip install -r requirements.txt'
            }
        }
        stage('Test') {
            steps {
                bat 'run_tests.bat --title "Jenkins Windows Tests"'
            }
        }
    }
    post {
        always {
            publishHTML([
                allowMissing: false,
                alwaysLinkToLastBuild: true,
                keepAll: true,
                reportDir: 'output\\reports',
                reportFiles: 'test_report_*.html',
                reportName: 'Test Report'
            ])
        }
    }
}
```

## Performance Tips

1. **Use SSD**: Store project on SSD for faster execution
2. **Exclude from Antivirus**: Add project folder to antivirus exclusions
3. **Close Unnecessary Apps**: Free up system resources
4. **Use Local Databases**: Reduce network latency when possible

## IDE Integration

### VS Code:
- Install Python extension
- Configure Python interpreter: `Ctrl+Shift+P` → "Python: Select Interpreter"
- Choose `venv\Scripts\python.exe`

### PyCharm:
- Open project folder
- Configure Python interpreter to use virtual environment
- Set working directory to project root

This setup ensures the framework runs smoothly on Windows with all HTML reporting features working correctly!