# Using Specific Database Sections

## Overview

The framework fully supports using specific Oracle and PostgreSQL section names directly in your config.ini file. You **never need to specify active environments** - just use the exact section names like `[S101_ORACLE]`, `[S102_ORACLE]`, `[P101_POSTGRES]`, `[P102_POSTGRES]`.

## Configuration Setup

### Your config.ini Structure:

```ini
[DEFAULT]
log_level = INFO
export_format = excel
chunk_size = 10000

# Oracle Database Sections - Use specific names directly
[S101_ORACLE]
host = s101-oracle-server.company.com
port = 1521
service_name = S101DB
username = s101_user
password = S101_ORACLE_PWD

[S102_ORACLE]
host = s102-oracle-server.company.com
port = 1521
service_name = S102DB
username = s102_user
password = S102_ORACLE_PWD

[S103_ORACLE]
host = s103-oracle-server.company.com
port = 1521
service_name = S103DB
username = s103_user
password = S103_ORACLE_PWD

# PostgreSQL Database Sections - Use specific names directly
[P101_POSTGRES]
host = p101-postgres-server.company.com
port = 5432
database = p101_database
username = p101_user
password = P101_POSTGRES_PWD

[P102_POSTGRES]
host = p102-postgres-server.company.com
port = 5432
database = p102_database
username = p102_user
password = P102_POSTGRES_PWD

[P103_POSTGRES]
host = p103-postgres-server.company.com
port = 5432
database = p103_database
username = p103_user
password = P103_POSTGRES_PWD

# Query Sections
[QUERIES]
customer_report = SELECT * FROM customers WHERE active = 'Y'
sales_summary = SELECT product_id, SUM(amount) as total FROM sales GROUP BY product_id
inventory_check = SELECT * FROM inventory WHERE quantity < 10

# Table Sections for specific systems
[S101_TABLES]
customer_table = customers
product_table = products
sales_table = sales

[P101_TABLES]
customer_table = customer_data
product_table = product_master
sales_table = transaction_log
```

## Environment Variables

Set passwords as environment variables matching your section names:

```bash
# Oracle passwords
export S101_ORACLE_PWD=your_s101_password
export S102_ORACLE_PWD=your_s102_password
export S103_ORACLE_PWD=your_s103_password

# PostgreSQL passwords
export P101_POSTGRES_PWD=your_p101_password
export P102_POSTGRES_PWD=your_p102_password
export P103_POSTGRES_PWD=your_p103_password
```

**Windows:**
```cmd
REM Oracle passwords
set S101_ORACLE_PWD=your_s101_password
set S102_ORACLE_PWD=your_s102_password
set S103_ORACLE_PWD=your_s103_password

REM PostgreSQL passwords
set P101_POSTGRES_PWD=your_p101_password
set P102_POSTGRES_PWD=your_p102_password
set P103_POSTGRES_PWD=your_p103_password
```

## Feature File Usage

### Basic Database Connections:

```gherkin
Feature: Your Database Tests
  Background:
    Given I load configuration from "config.ini"

  @database @oracle @S101
  Scenario: Test S101 Oracle database
    When I connect to Oracle database using "S101_ORACLE" configuration
    Then Oracle connection should be established successfully

  @database @postgres @P101
  Scenario: Test P101 PostgreSQL database
    When I connect to PostgreSQL database using "P101_POSTGRES" configuration
    Then PostgreSQL connection should be established successfully
```

### Cross-System Data Comparison:

```gherkin
  @database @comparison @cross_system
  Scenario: Compare S101 Oracle with P101 PostgreSQL
    Given I connect to Oracle database using "S101_ORACLE" configuration
    And I connect to PostgreSQL database using "P101_POSTGRES" configuration
    When I read query from config section "QUERIES" key "customer_report"
    And I execute query on Oracle and store as source DataFrame
    And I execute query on PostgreSQL and store as target DataFrame
    Then I perform DataFrame comparison with primary key "id"
    And DataFrames should match within 0% tolerance
```

### Oracle to Oracle Comparison:

```gherkin
  @database @comparison @oracle_to_oracle
  Scenario: Compare S101 Oracle with S102 Oracle
    Given I connect to Oracle database using "S101_ORACLE" configuration as source
    And I connect to Oracle database using "S102_ORACLE" configuration as target
    When I read query from config section "QUERIES" key "sales_summary"
    And I execute query on source database and store as source DataFrame
    And I execute query on target database and store as target DataFrame
    Then I perform DataFrame comparison with primary key "product_id"
    And DataFrames should match within 5% tolerance
```

### PostgreSQL to PostgreSQL Comparison:

```gherkin
  @database @comparison @postgres_to_postgres
  Scenario: Compare P101 PostgreSQL with P102 PostgreSQL
    Given I connect to PostgreSQL database using "P101_POSTGRES" configuration as source
    And I connect to PostgreSQL database using "P102_POSTGRES" configuration as target
    When I read query from config section "QUERIES" key "inventory_check"
    And I execute query on source database and store as source DataFrame
    And I execute query on target database and store as target DataFrame
    Then I perform DataFrame comparison with primary key "id"
    And DataFrames should match exactly
```

### Multi-System Testing:

```gherkin
  @database @multi_system @comprehensive
  Scenario: Multi-system comprehensive test
    Given I connect to Oracle database using "S101_ORACLE" configuration
    And I connect to Oracle database using "S102_ORACLE" configuration as secondary
    And I connect to PostgreSQL database using "P101_POSTGRES" configuration
    And I connect to PostgreSQL database using "P102_POSTGRES" configuration as secondary
    When I execute direct query "SELECT COUNT(*) as record_count FROM dual" on Oracle as source
    And I execute direct query "SELECT COUNT(*) as record_count FROM information_schema.tables" on PostgreSQL as target
    Then source DataFrame should contain data
    And target DataFrame should contain data
    And both databases should be accessible
```

### Table-Based Data Loading:

```gherkin
  @database @table_based @S101
  Scenario: Load data from S101 Oracle using table configuration
    Given I connect to Oracle database using "S101_ORACLE" configuration
    When I load source data using table from config section "S101_TABLES" key "customer_table" on Oracle
    Then source DataFrame should contain data
    And source DataFrame should have more than 0 records

  @database @table_based @P101
  Scenario: Load data from P101 PostgreSQL using table configuration
    Given I connect to PostgreSQL database using "P101_POSTGRES" configuration
    When I load target data using table from config section "P101_TABLES" key "customer_table" on PostgreSQL
    Then target DataFrame should contain data
    And target DataFrame should have more than 0 records
```

## Running Tests with Specific Sections

### Command Line Examples:

```bash
# Run all S101 Oracle tests
behave --tags=@S101

# Run all P101 PostgreSQL tests
behave --tags=@P101

# Run cross-system comparison tests
behave --tags=@cross_system

# Run Oracle to Oracle comparisons
behave --tags=@oracle_to_oracle

# Run PostgreSQL to PostgreSQL comparisons
behave --tags=@postgres_to_postgres

# Run comprehensive multi-system tests
behave --tags=@multi_system

# Combine with other tags
behave --tags="@database and @S101"
behave --tags="@comparison and (@S101 or @P101)"
```

### Framework Scripts:

```bash
# Run with framework scripts
./run_tests.sh --tags "@S101 and @database"
./run_tests.sh --tags "@comparison and @cross_system"

# Windows
run_tests.bat --tags "@P101 and @database"
run_tests.bat --tags "@oracle_to_oracle"

# PowerShell
.\run_tests.ps1 -Tags "@multi_system" -Title "Multi-System Database Tests"
```

## Advanced Usage Patterns

### 1. **System-Specific Test Suites:**

```bash
# Test all S101 Oracle functionality
behave --tags="@S101"

# Test all P101 PostgreSQL functionality  
behave --tags="@P101"

# Test all Oracle systems
behave --tags="@oracle"

# Test all PostgreSQL systems
behave --tags="@postgres"
```

### 2. **Comparison Workflows:**

```bash
# All cross-system comparisons
behave --tags="@comparison and @cross_system"

# Same-type system comparisons
behave --tags="@oracle_to_oracle or @postgres_to_postgres"

# All comparison tests
behave --tags="@comparison"
```

### 3. **Environment-Style Grouping:**

If you want to group systems by environment, use additional tags:

```gherkin
  @database @oracle @S101 @production
  Scenario: S101 Oracle production test
  
  @database @postgres @P101 @staging  
  Scenario: P101 PostgreSQL staging test
```

Then run:
```bash
# All production systems
behave --tags="@production"

# All staging systems
behave --tags="@staging"
```

## HTML Reports

The HTML reports will show:
- **Section names** in test titles (S101_ORACLE, P101_POSTGRES, etc.)
- **System tags** for easy filtering (@S101, @P101, etc.)
- **Connection details** with specific host/database information
- **Query keys** from your config sections

## Configuration Validation

Test your configuration setup:

```bash
# Verify specific sections exist
python3 -c "
from utils.config_helper import get_config_helper
from behave.runner import Context

context = Context()
helper = get_config_helper(context)

# Test S101 Oracle config
try:
    config = helper.load_database_config('S101_ORACLE')
    print(f'✅ S101_ORACLE: {config.host}:{config.port}')
except Exception as e:
    print(f'❌ S101_ORACLE: {e}')

# Test P101 PostgreSQL config
try:
    config = helper.load_database_config('P101_POSTGRES')
    print(f'✅ P101_POSTGRES: {config.host}:{config.port}')
except Exception as e:
    print(f'❌ P101_POSTGRES: {e}')
"
```

## Best Practices

### 1. **Naming Conventions:**
- Use consistent prefixes (S101, S102 for Oracle systems)
- Use consistent suffixes (_ORACLE, _POSTGRES)
- Match environment variable names exactly

### 2. **Configuration Management:**
- Keep passwords in environment variables
- Use descriptive host names
- Document system purposes in comments

### 3. **Test Organization:**
- Tag tests with system identifiers (@S101, @P101)
- Use functional tags (@comparison, @validation)
- Combine tags for flexible execution

### 4. **Security:**
- Never commit passwords to config files
- Use environment variables for all sensitive data
- Rotate passwords regularly

This approach gives you complete control over database connections using your specific section names without any environment variable complexity! 🎯