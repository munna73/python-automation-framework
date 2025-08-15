# Test Automation Framework - PowerShell Test Runner with HTML Reports
# Usage: .\run_tests.ps1 [options]

param(
    [string]$Features = "features\",
    [string]$Tags = "",
    [string]$Title = "Database Test Automation Report", 
    [string]$Output = "output\reports",
    [switch]$Help
)

if ($Help) {
    Write-Host ""
    Write-Host "🚀 Test Automation Framework - PowerShell Runner"
    Write-Host "================================================="
    Write-Host ""
    Write-Host "Usage: .\run_tests.ps1 [OPTIONS]"
    Write-Host ""
    Write-Host "Parameters:"
    Write-Host "  -Features DIR     Features directory or specific .feature files"
    Write-Host "  -Tags TAGS        Tags to include/exclude (e.g., '@database')"
    Write-Host "  -Title TITLE      Report title"
    Write-Host "  -Output DIR       Output directory for reports"
    Write-Host "  -Help            Show this help message"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\run_tests.ps1                                    # Run all tests"
    Write-Host "  .\run_tests.ps1 -Features features\database\       # Run database tests only"
    Write-Host "  .\run_tests.ps1 -Tags '@database'                  # Run tests tagged with @database"
    Write-Host "  .\run_tests.ps1 -Title 'My Test Report'            # Custom report title"
    Write-Host ""
    exit 0
}

Write-Host ""
Write-Host "🚀 Test Automation Framework - HTML Report Runner (PowerShell)" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

# Create output directories
if (!(Test-Path $Output)) {
    New-Item -ItemType Directory -Path $Output -Force | Out-Null
}
if (!(Test-Path "output\junit")) {
    New-Item -ItemType Directory -Path "output\junit" -Force | Out-Null
}

# Set environment variables for better test execution
$env:PYTHONPATH = ".;$($env:PYTHONPATH)"

Write-Host ""
Write-Host "📋 Configuration:" -ForegroundColor Yellow
Write-Host "   Features: $Features" -ForegroundColor White
Write-Host "   Tags: $(if ($Tags) {$Tags} else {'(all tests)'})" -ForegroundColor White
Write-Host "   Title: $Title" -ForegroundColor White
Write-Host "   Output: $Output" -ForegroundColor White
Write-Host ""

# Check if Python is available
try {
    $pythonVersion = python --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Python not found"
    }
    Write-Host "✅ Python available: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python not found. Please install Python and add it to PATH" -ForegroundColor Red
    Write-Host "   Download from: https://python.org/downloads/" -ForegroundColor Yellow
    exit 1
}

# Build command arguments
$arguments = @($Features)
if ($Tags) {
    $arguments += "--tags"
    $arguments += $Tags
}
$arguments += "--title"
$arguments += $Title
$arguments += "--output" 
$arguments += $Output

Write-Host "🔄 Starting test execution..." -ForegroundColor Yellow
Write-Host "Command: python scripts\run_tests_with_reports.py $($arguments -join ' ')" -ForegroundColor Gray

# Run tests with Python script
try {
    & python "scripts\run_tests_with_reports.py" @arguments
    $exitCode = $LASTEXITCODE
} catch {
    Write-Host "❌ Error running tests: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""
if ($exitCode -eq 0) {
    Write-Host "✅ Test execution completed successfully!" -ForegroundColor Green
    Write-Host "📊 Check the HTML report in: $Output\" -ForegroundColor Cyan
} else {
    Write-Host "⚠️ Test execution completed with failures." -ForegroundColor Yellow
    Write-Host "📊 Check the HTML report for details: $Output\" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "📁 Generated Reports:" -ForegroundColor Yellow
Write-Host "   - HTML Report: $Output\test_report_*.html" -ForegroundColor White
Write-Host "   - JSON Results: $Output\behave_results_*.json" -ForegroundColor White
Write-Host "   - JUnit XML: output\junit\" -ForegroundColor White
Write-Host "   - Application Logs: logs\test_automation.log" -ForegroundColor White

# Try to open the latest HTML report
$latestReport = Get-ChildItem "$Output\test_report_*.html" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($latestReport) {
    Write-Host ""
    Write-Host "🌐 Opening HTML report..." -ForegroundColor Cyan
    Start-Process $latestReport.FullName
}

exit $exitCode