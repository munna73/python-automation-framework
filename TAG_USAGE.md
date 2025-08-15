# Tag-Based Test Execution Guide

## Overview

The framework uses Behave tags to organize and selectively run tests. Tags allow you to categorize tests by functionality, test level, environment, or any custom criteria.

## Available Tags in the Framework

### 🎯 **Test Categories**
- `@smoke` - Quick smoke tests for basic functionality
- `@regression` - Full regression test suite
- `@integration` - Integration tests between components
- `@e2e` - End-to-end business workflow tests
- `@performance` - Performance and load tests
- `@security` - Security-focused tests
- `@contract` - API contract tests

### 🏷️ **Component Tags**
- `@database` / `@sql` - Database-related tests
- `@api` - REST API tests
- `@aws` - AWS service integration tests
- `@kafka` - Kafka messaging tests
- `@mongodb` - NoSQL database tests

### 🔧 **Functionality Tags**
- `@query` - Query execution tests
- `@comparison` - Data comparison tests
- `@export` - Data export tests
- `@validation` - Data validation tests
- `@config` - Configuration-based tests

### ⚡ **Execution Speed**
- `@fast` - Quick execution tests (< 30 seconds)
- `@medium` - Medium duration tests (30s - 2min)
- `@slow` - Long-running tests (> 2 minutes)

### 📊 **Priority Levels**
- `@critical` - Must-pass tests for deployment
- `@high` - High priority tests
- `@medium` - Medium priority tests
- `@low` - Low priority tests

## Running Tests by Tags

### 🚀 **Basic Tag Execution**

```bash
# Run all smoke tests
behave --tags=@smoke

# Run database tests only
behave --tags=@database

# Run API regression tests
behave --tags="@api and @regression"

# Exclude slow tests
behave --tags="not @slow"
```

### 🛠 **Using Framework Scripts**

**Linux/macOS:**
```bash
# Shell script with tags
./run_tests.sh --tags "@smoke"
./run_tests.sh --tags "@database and @regression"
./run_tests.sh --tags "not @performance"

# Python script with tags
python3 scripts/run_tests_with_reports.py --tags "@abctest"
python3 scripts/run_tests_with_reports.py --tags "@api and @smoke"
```

**Windows:**
```cmd
REM Batch script with tags
run_tests.bat --tags "@smoke"
run_tests.bat --tags "@database and @regression"

REM PowerShell with tags
.\run_tests.ps1 -Tags "@abctest"
.\run_tests.ps1 -Tags "@api and @smoke"
```

### 🎯 **Advanced Tag Logic**

```bash
# AND logic - tests must have BOTH tags
behave --tags="@database and @smoke"

# OR logic - tests with EITHER tag
behave --tags="@smoke or @critical"

# NOT logic - exclude specific tests
behave --tags="not @slow"
behave --tags="not (@performance or @load)"

# Complex combinations
behave --tags="(@database or @api) and @smoke and not @slow"
behave --tags="@regression and not (@aws or @kafka)"
```

## Custom Tag Examples

### 📝 **Your Custom @abctest Tag**

Let me create example scenarios with your `@abctest` tag:

```gherkin
Feature: ABC Test Suite
  Custom test scenarios for ABC functionality

  @abctest @smoke
  Scenario: ABC basic functionality test
    Given I have ABC system configured
    When I execute ABC basic operations
    Then ABC should work correctly

  @abctest @regression
  Scenario: ABC comprehensive validation
    Given I have ABC system with full dataset
    When I run comprehensive ABC tests
    Then all ABC validations should pass

  @abctest @performance
  Scenario: ABC performance testing
    Given I have ABC system under load
    When I measure ABC response times
    Then ABC performance should meet SLA
```

**Running @abctest:**
```bash
# Run all ABC tests
behave --tags=@abctest

# Run only ABC smoke tests  
behave --tags="@abctest and @smoke"

# Run ABC tests but exclude performance tests
behave --tags="@abctest and not @performance"
```

### 🏗️ **Environment-Specific Tags**

```gherkin
@dev @database
Scenario: Development database test

@qa @integration
Scenario: QA integration test

@prod @smoke
Scenario: Production smoke test
```

**Usage:**
```bash
# Run development tests only
behave --tags=@dev

# Run QA integration tests
behave --tags="@qa and @integration"

# Run all environments except production
behave --tags="not @prod"
```

## Tag Management Best Practices

### 1. **Consistent Naming**
```gherkin
# Good - consistent naming
@database @smoke
@database @regression
@api @smoke
@api @regression

# Avoid - inconsistent naming
@db @smoke
@database @reg
@apis @testing
```

### 2. **Hierarchical Tagging**
```gherkin
# Component + Test Level + Speed
@database @regression @fast
@api @smoke @medium
@aws @integration @slow
```

### 3. **Meaningful Combinations**
```gherkin
# Feature-specific tags
@user_management @authentication @smoke
@data_migration @database @critical
@payment_processing @api @security
```

## HTML Report Integration

The HTML reports show tag information for each test:

```html
<!-- In the generated report -->
<div class="scenario-tags">
  <span class="tag">@abctest</span>
  <span class="tag">@smoke</span>
  <span class="tag">@critical</span>
</div>
```

## CI/CD Pipeline Integration

### 🔄 **Jenkins Pipeline**
```groovy
pipeline {
    parameters {
        choice(
            name: 'TEST_TAGS',
            choices: ['@smoke', '@regression', '@database', '@api', '@abctest'],
            description: 'Tags to run'
        )
    }
    stages {
        stage('Test') {
            steps {
                bat "run_tests.bat --tags \"${params.TEST_TAGS}\""
            }
        }
    }
}
```

### 🔄 **GitHub Actions**
```yaml
name: Tagged Tests
on:
  workflow_dispatch:
    inputs:
      tags:
        description: 'Test tags to run'
        required: true
        default: '@smoke'

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Run Tagged Tests
        run: |
          ./run_tests.sh --tags "${{ github.event.inputs.tags }}"
```

## Tag Reporting and Analysis

### 📊 **Tag Statistics**
```bash
# Count tests by tag
behave --dry-run --tags=@smoke | grep -c "Scenario"
behave --dry-run --tags=@regression | grep -c "Scenario" 
behave --dry-run --tags=@abctest | grep -c "Scenario"

# List all scenarios with specific tag
behave --dry-run --tags=@abctest --format=plain
```

### 📈 **Tag Performance Tracking**
The HTML reports include tag-based execution time analysis:

```html
<!-- Tag performance summary -->
<div class="tag-performance">
  <h3>Performance by Tag</h3>
  <div class="tag-stats">
    <div class="tag-stat">
      <span class="tag-name">@abctest</span>
      <span class="tag-time">45.2s</span>
      <span class="tag-count">12 scenarios</span>
    </div>
  </div>
</div>
```

## Common Tag Usage Patterns

### 🚀 **Development Workflow**
```bash
# During development - run fast tests frequently
behave --tags="@smoke and @fast"

# Before commit - run component tests
behave --tags="@database and @smoke"

# Before merge - run critical path
behave --tags="@critical or (@smoke and @regression)"
```

### 🧪 **Testing Phases**
```bash
# Phase 1: Smoke tests
behave --tags=@smoke

# Phase 2: Component tests
behave --tags="@database or @api"

# Phase 3: Integration tests  
behave --tags=@integration

# Phase 4: End-to-end tests
behave --tags=@e2e

# Phase 5: Performance tests
behave --tags=@performance
```

### 🎯 **Custom Test Suites**
```bash
# ABC project test suite
behave --tags=@abctest

# Critical path for deployment
behave --tags="@critical and not @slow"

# Full regression minus performance
behave --tags="@regression and not @performance"

# API-only comprehensive testing
behave --tags="@api and (@smoke or @regression)"
```

## Troubleshooting Tags

### ❌ **Common Issues**

1. **Tag not found:**
```bash
# Check if tag exists
behave --dry-run --tags=@abctest

# List all available tags
grep -r "@\w*" features/ | grep -o "@\w*" | sort | uniq
```

2. **Complex tag logic:**
```bash
# Test tag logic without running tests
behave --dry-run --tags="@abctest and @smoke"
behave --dry-run --tags="not @slow"
```

3. **Case sensitivity:**
```bash
# Tags are case-sensitive
@AbcTest ≠ @abctest
@SMOKE ≠ @smoke
```

## Creating Your Own Tags

### ✨ **Custom Tag Categories**

Add tags to your feature files:

```gherkin
Feature: Your Custom Feature

  @abctest @priority_high @module_auth
  Scenario: High priority authentication test
    Given I have authentication configured
    When I test authentication flow
    Then authentication should work correctly

  @abctest @priority_medium @module_reporting  
  Scenario: Medium priority reporting test
    Given I have reporting system ready
    When I generate custom reports
    Then reports should contain correct data
```

**Run your custom tags:**
```bash
# Run all ABC tests
behave --tags=@abctest

# Run high priority ABC tests
behave --tags="@abctest and @priority_high"

# Run ABC authentication module only
behave --tags="@abctest and @module_auth"
```

This gives you complete control over test execution using the tag system! 🎯