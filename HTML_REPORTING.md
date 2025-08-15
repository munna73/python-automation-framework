# HTML Reporting Configuration

## Overview

The test automation framework now includes comprehensive HTML reporting that generates beautiful, interactive reports showing all passed and failed tests with detailed information.

## Features

### ✅ **Comprehensive Test Results**
- **Summary Dashboard** - Test counts, pass/fail rates, execution summary
- **Feature-level Results** - Organized by BDD features
- **Scenario Details** - Individual scenario results with timing
- **Step-level Information** - Each test step with status and duration
- **Error Messages** - Detailed error information for failed tests
- **Visual Status Indicators** - Color-coded pass/fail status

### 🎨 **Professional Styling**
- **Modern UI** - Clean, responsive design
- **Color-coded Results** - Green for pass, red for fail
- **Mobile Responsive** - Works on all device sizes
- **Print-friendly** - Clean printing layout

### 📊 **Multiple Report Formats**
- **HTML Report** - Interactive visual report
- **JSON Output** - Machine-readable results
- **JUnit XML** - CI/CD integration
- **Console Output** - Real-time feedback

## Usage

### Quick Start

```bash
# Run all tests with HTML report
./run_tests.sh

# Run specific features  
./run_tests.sh --features features/database/

# Run tests with specific tags
./run_tests.sh --tags "@database"

# Custom report title
./run_tests.sh --title "My Custom Test Report"
```

### Python Script

```bash
# Run with Python script
python3 scripts/run_tests_with_reports.py features/ --title "Database Tests"

# Run specific feature file
python3 scripts/run_tests_with_reports.py features/database/data_comparison.feature
```

### Direct Behave Execution

```bash
# Run behave directly (generates JSON for HTML conversion)
behave features/

# Generate HTML report from existing JSON
python3 utils/html_reporter.py output/reports/behave_results.json output/reports/my_report.html
```

## Configuration

### Behave Configuration (behave.ini)

```ini
[behave]
# Output settings
stdout_capture = false
stderr_capture = false  
log_capture = false

# Default formats for direct behave execution
format = pretty,json
outfiles = ,output/reports/behave_results.json

# JUnit XML reporting
junit = true
junit_directory = output/junit

# Test behavior
show_skipped = true
show_timings = true
summary = true
```

### Environment Variables

```bash
# Customize report generation
export LOG_LEVEL=INFO                    # Logging level
export SINGLE_LOG_FILE=true              # Use single log file
export PYTHONPATH=".:$PYTHONPATH"        # Python path
```

## Output Structure

```
output/
├── reports/
│   ├── test_report_YYYYMMDD_HHMMSS.html    # HTML report
│   ├── behave_results_YYYYMMDD_HHMMSS.json # JSON results
│   └── html/                                # Additional HTML assets
├── junit/                                   # JUnit XML files
└── exports/                                 # Data exports
```

## Report Components

### 1. **Summary Section**
```
┌─────────────────────────────────────┐
│  Test Execution Summary             │
│                                     │
│  Total: 25    Passed: 22    Failed: 3 │
│  Success Rate: 88.0%                │
└─────────────────────────────────────┘
```

### 2. **Feature Results**
```
📁 Feature: Database Comparison Tests
  ✅ Scenario: Basic Oracle to PostgreSQL comparison  (0.245s)
  ❌ Scenario: Complex data validation                (1.123s)
     Step: Given I connect to Oracle database       ✅ (0.015s)  
     Step: When I execute comparison query          ❌ (0.890s)
     Error: Connection timeout after 30 seconds
```

### 3. **Detailed Step Information**
- Individual step execution times
- Step-by-step status tracking
- Error messages and stack traces
- Log output integration

## Customization

### Custom Report Titles

```python
# In scripts/run_tests_with_reports.py
run_tests_with_reports(
    feature_paths=["features/database/"],
    report_title="Database Integration Tests - UAT Environment",
    output_dir="output/uat_reports"
)
```

### Styling Customization

The HTML template in `utils/html_reporter.py` can be customized:

```python
# Modify the _get_html_template() method
def _get_html_template(self) -> str:
    return """<!DOCTYPE html>
    <html>
    <head>
        <title>{{TITLE}}</title>
        <style>
        /* Your custom CSS here */
        .header { background: your-custom-gradient; }
        </style>
    </head>
    <!-- Your custom HTML structure -->
    </html>"""
```

## CI/CD Integration

### Jenkins

```groovy
pipeline {
    stages {
        stage('Test') {
            steps {
                sh './run_tests.sh --title "CI Test Report - Build ${BUILD_NUMBER}"'
            }
            post {
                always {
                    publishHTML([
                        allowMissing: false,
                        alwaysLinkToLastBuild: true,
                        keepAll: true,
                        reportDir: 'output/reports',
                        reportFiles: 'test_report_*.html',
                        reportName: 'Test Report'
                    ])
                    junit 'output/junit/*.xml'
                }
            }
        }
    }
}
```

### GitHub Actions

```yaml
- name: Run Tests with HTML Report
  run: |
    ./run_tests.sh --title "GitHub Actions Test Report"
    
- name: Upload Test Report
  uses: actions/upload-artifact@v3
  if: always()
  with:
    name: test-report
    path: output/reports/test_report_*.html
```

## Benefits

### 🚀 **Developer Experience**
- **Visual Results** - Easy to scan pass/fail status
- **Detailed Debugging** - Click to see exact failure points
- **Historical Tracking** - Compare results over time
- **Team Sharing** - Send HTML reports via email/Slack

### 📈 **Project Management**  
- **Executive Summary** - High-level success metrics
- **Trend Analysis** - Track test stability over time
- **Quality Gates** - Clear pass/fail criteria
- **Documentation** - Test evidence for compliance

### 🔧 **Technical Benefits**
- **No External Dependencies** - Built-in HTML generation
- **Fast Generation** - Reports created in seconds
- **Cross-platform** - Works on all operating systems
- **Lightweight** - Minimal resource usage

## Troubleshooting

### Common Issues

#### 1. **No JSON Output Generated**
```bash
# Check behave.ini format configuration
format = pretty,json
outfiles = ,output/reports/behave_results.json
```

#### 2. **HTML Report Generation Fails**
```bash
# Verify JSON file exists and is valid
python3 -c "import json; print(json.load(open('output/reports/behave_results.json')))"

# Run HTML generator directly
python3 utils/html_reporter.py output/reports/behave_results.json output/test.html
```

#### 3. **Report Doesn't Open in Browser**
```bash
# Open manually
open output/reports/test_report_*.html  # macOS
xdg-open output/reports/test_report_*.html  # Linux
```

### Debug Mode

```bash
# Run with debug output
python3 scripts/run_tests_with_reports.py --help

# Check JSON structure
cat output/reports/behave_results.json | python3 -m json.tool
```

## Examples

### Sample Report Structure

The generated HTML report includes:

1. **Header**: Report title and generation timestamp
2. **Summary**: Aggregate statistics and success rates
3. **Features**: Organized test results by feature
4. **Scenarios**: Individual test scenarios with pass/fail status
5. **Steps**: Detailed step execution with timing
6. **Errors**: Full error messages and stack traces for failed tests

This provides a complete picture of test execution that's easy to understand for both technical and non-technical stakeholders.

## Future Enhancements

- **Trend Charts** - Historical test result trends
- **Test Coverage** - Code coverage integration
- **Performance Metrics** - Response time tracking
- **Email Integration** - Automatic report distribution
- **Slack Integration** - Test result notifications

The HTML reporting system is designed to be extensible and can be customized for specific project needs.