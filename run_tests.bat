@echo off
REM Test Automation Framework - Windows Test Runner with HTML Reports
REM Usage: run_tests.bat [options]

echo.
echo 🚀 Test Automation Framework - HTML Report Runner (Windows)
echo ===========================================================

REM Default values
set "FEATURES_DIR=features\"
set "TAGS="
set "TITLE=Database Test Automation Report"
set "OUTPUT_DIR=output\reports"

REM Parse command line arguments
:parse_args
if "%~1"=="" goto end_parse
if "%~1"=="--features" (
    set "FEATURES_DIR=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="--tags" (
    if "%TAGS%"=="" (
        set "TAGS=--tags=%~2"
    ) else (
        set "TAGS=%TAGS% --tags=%~2"
    )
    shift
    shift
    goto parse_args
)
if "%~1"=="--title" (
    set "TITLE=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="--output" (
    set "OUTPUT_DIR=%~2"
    shift
    shift
    goto parse_args
)
if "%~1"=="--help" goto show_help
if "%~1"=="-h" goto show_help

REM Handle bare tag arguments (e.g., @sometag)
if "%~1:~0,1%"=="@" (
    if "%TAGS%"=="" (
        set "TAGS=--tags=%~1"
    ) else (
        set "TAGS=%TAGS% --tags=%~1"
    )
    shift
    goto parse_args
)

echo Unknown option: %~1
echo Use --help for usage information
exit /b 1

:show_help
echo Usage: %0 [OPTIONS]
echo.
echo Options:
echo   --features DIR    Features directory or specific .feature files
echo   --tags TAGS       Tags to include/exclude (supports Behave tag expressions)
echo   --title TITLE     Report title
echo   --output DIR      Output directory for reports
echo   --help, -h        Show this help message
echo.
echo Tag Expressions (Behave syntax):
echo   @tag1             # Run scenarios with @tag1
echo   @tag1 @tag2       # Run scenarios with BOTH @tag1 AND @tag2
echo   "@tag1 or @tag2"  # Run scenarios with @tag1 OR @tag2
echo   "@tag1 and @tag2" # Run scenarios with @tag1 AND @tag2 (explicit)
echo   "not @skip"       # Run scenarios WITHOUT @skip tag
echo   "@db and not @slow" # Complex: @db tagged but not @slow
echo.
echo Examples:
echo   %0                                    # Run all tests
echo   %0 --features features\database\     # Run database tests only
echo   %0 --tags "@database"                # Run tests tagged with @database
echo   %0 @database                         # Run tests tagged with @database (shorthand)
echo   %0 @database @smoke                  # Run tests with multiple tags (AND condition)
echo   %0 --tags "@database or @smoke"      # Multiple tags OR condition (use 'or' keyword)
echo   %0 --tags "@database and @smoke"     # Multiple tags AND condition (use 'and' keyword)
echo   %0 --tags "not @skip"                # Exclude tests with @skip tag
echo   %0 --title "My Test Report"          # Custom report title
exit /b 0

:end_parse

REM Create output directories
if not exist "%OUTPUT_DIR%" mkdir "%OUTPUT_DIR%"
if not exist "output\junit" mkdir "output\junit"

REM Set environment variables for better test execution
set "PYTHONPATH=.;%PYTHONPATH%"

echo 📋 Configuration:
echo    Features: %FEATURES_DIR%
if "%TAGS%"=="" (
    echo    Tags: (all tests)
) else (
    echo    Tags: %TAGS%
)
echo    Title: %TITLE%
echo    Output: %OUTPUT_DIR%
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please install Python and add it to PATH
    echo    Download from: https://python.org/downloads/
    exit /b 1
)

REM Run tests with Python script
echo 🔄 Starting test execution...
if "%TAGS%"=="" (
    python scripts\run_tests_with_reports.py %FEATURES_DIR% --title "%TITLE%" --output "%OUTPUT_DIR%"
) else (
    python scripts\run_tests_with_reports.py %FEATURES_DIR% %TAGS% --title "%TITLE%" --output "%OUTPUT_DIR%"
)

REM Capture exit code
set EXIT_CODE=%errorlevel%

echo.
if %EXIT_CODE% equ 0 (
    echo ✅ Test execution completed successfully!
    echo 📊 Check the HTML report in: %OUTPUT_DIR%\
) else (
    echo ⚠️ Test execution completed with failures.
    echo 📊 Check the HTML report for details: %OUTPUT_DIR%\
)

echo.
echo 📁 Generated Reports:
echo    - HTML Report: %OUTPUT_DIR%\test_report_*.html
echo    - JSON Results: %OUTPUT_DIR%\behave_results_*.json
echo    - JUnit XML: output\junit\
echo    - Application Logs: logs\test_automation.log

exit /b %EXIT_CODE%