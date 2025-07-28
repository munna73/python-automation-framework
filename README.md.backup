# Python Automation Framework with Behave BDD

A comprehensive automation framework supporting SQL databases, NoSQL MongoDB, API testing, and MQ messaging with BDD capabilities.

## Features

- 🗄️ **SQL Database Operations**: Oracle and PostgreSQL support with data comparison
- 📊 **NoSQL Database Operations**: MongoDB support with aggregation pipelines
- 🌐 **API Testing**: REST API testing with validation
- 📨 **MQ Integration**: IBM MQ message posting
- 🥒 **BDD Testing**: Behave framework for business-readable tests
- 📊 **Enhanced Exports**: CSV and Excel with special character handling
- 📝 **Centralized Logging**: Component-based logging system
- 🔄 **CI/CD Ready**: Jenkins pipeline integration
- 🏷️ **Tag-Based Testing**: Flexible test execution with tags

## Quick Start

1. **Setup Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure Environment**:
   ```bash
   cp .env.template .env
   # Edit .env with your actual credentials
   ```

3. **Update Configuration**:
   ```bash
   # Edit config/config.ini with your database/API/MQ settings
   ```

4. **Run BDD Tests**:
   ```bash
   behave
   ```

## Project Structure

```
automation-framework/
├── features/           # BDD feature files
│   ├── database/       # SQL database testing features
│   ├── mongodb/        # MongoDB testing features  
│   ├── api/           # API testing features
│   └── mq/            # MQ testing features
├── steps/             # Step definitions
│   ├── database_steps.py    # SQL database steps
│   ├── mongodb_steps.py     # MongoDB steps
│   ├── api_steps.py         # API steps
│   └── mq_steps.py          # MQ steps
├── db/                # Database utilities
│   ├── database_connector.py    # Oracle/PostgreSQL connector
│   ├── mongodb_connector.py     # MongoDB connector
│   └── data_comparator.py       # Data comparison utilities
├── web/               # API testing utilities
├── mq/                # MQ utilities
├── utils/             # Common utilities
├── config/            # Configuration files
├── logs/              # Centralized logging
└── output/            # Reports and exports
```

## Configuration

### Database Configuration (`config/config.ini`)

**Oracle/PostgreSQL:**
```ini
[DEV_ORACLE]
host = your-oracle-host.com
port = 1521
service_name = ORCL
username = your_username

[DEV_POSTGRES]
host = your-postgres-host.com
port = 5432
database = your_database
username = your_username
```

**MongoDB:**
```ini
[DEV_MONGODB]
host = your-mongo-host.com
port = 27017
database = your_database
username = your_username
```

### Environment Variables (`.env`)
```bash
# SQL Database Passwords
DEV_ORACLE_PWD=your_oracle_password
DEV_POSTGRES_PWD=your_postgres_password

# MongoDB Passwords  
DEV_MONGODB_PWD=your_mongodb_password

# API & MQ Authentication
API_TOKEN=your_api_token
MQ_PWD=your_mq_password
```

## 🏷️ Tag-Based Test Execution

### **Available Tags**

#### **By Test Type:**
- `@smoke` - Quick, critical tests for basic functionality
- `@regression` - Comprehensive test suite for full validation
- `@integration` - Integration tests between components
- `@e2e` - End-to-end workflow tests
- `@crud` - Create, Read, Update, Delete operations

#### **By Component:**
- `@database` - SQL database-related tests (Oracle/PostgreSQL)
- `@mongodb` - MongoDB NoSQL database tests
- `@api` - REST API testing scenarios
- `@mq` - Message Queue testing scenarios

#### **By Environment:**
- `@dev` - Development environment tests
- `@qa` - QA environment tests
- `@prod` - Production environment tests

#### **By Priority:**
- `@critical` - Must-pass tests
- `@high` - High priority tests
- `@medium` - Medium priority tests

#### **By Execution Speed:**
- `@fast` - Quick tests (< 1 minute)
- `@slow` - Longer running tests (> 5 minutes)

### **Running Tagged Tests**

#### **Single Tag Execution:**
```bash
# Run all smoke tests
behave --tags=@smoke

# Run all database tests (SQL + MongoDB)
behave --tags=@database

# Run MongoDB-specific tests
behave --tags=@mongodb

# Run API tests only
behave --tags=@api
```

#### **Multiple Tags (AND logic):**
```bash
# Run tests that are BOTH smoke AND database
behave --tags="@smoke and @database"

# Run MongoDB smoke tests
behave --tags="@mongodb and @smoke"

# Run regression tests for databases
behave --tags="@regression and (@database or @mongodb)"
```

#### **Multiple Tags (OR logic):**
```bash
# Run tests that are EITHER smoke OR regression
behave --tags="@smoke or @regression"

# Run either SQL database OR MongoDB tests
behave --tags="@database or @mongodb"
```

#### **Excluding Tags:**
```bash
# Run all tests EXCEPT slow ones
behave --tags="not @slow"

# Run database tests but NOT MongoDB
behave --tags="@database and not @mongodb"
```

#### **Complex Tag Combinations:**
```bash
# Run smoke or regression tests for databases only
behave --tags="(@smoke or @regression) and (@database or @mongodb)"

# Run critical tests for dev or qa environments
behave --tags="@critical and (@dev or @qa)"

# Run fast MongoDB CRUD operations
behave --tags="@mongodb and @crud and @fast"
```

## 🧪 Testing Instructions

### **1. SQL Database Testing**

#### **Basic Database Connectivity:**
```bash
# Test Oracle connection
python scripts/run.py test-db --env DEV --db-type ORACLE

# Test PostgreSQL connection  
python scripts/run.py test-db --env DEV --db-type POSTGRES

# Run SQL database BDD tests
behave features/database/ --tags=@smoke
```

#### **Database Comparison:**
```bash
# Compare data between environments
python scripts/run.py compare --source-env DEV --target-env QA --source-db ORACLE --target-db ORACLE

# Compare with custom settings
behave features/database/data_comparison.feature --tags=@regression
```

### **2. MongoDB Testing**

#### **MongoDB Connectivity:**
```bash
# Test MongoDB connection
python scripts/run.py test-db --env DEV --db-type MONGODB

# Get collection statistics
python scripts/run.py mongodb-stats --env DEV --collection customers
```

#### **MongoDB Queries:**
```bash
# Query MongoDB collection
python scripts/run.py mongodb-query --env DEV --collection users --query '{"status":"active"}' --limit 50

# Run aggregation pipeline
python scripts/run.py mongodb-aggregate --env DEV --collection orders --pipeline '[{"$group":{"_id":"$customer_id","total":{"$sum":"$amount"}}}]'
```

#### **MongoDB BDD Tests:**
```bash
# Run MongoDB-specific tests
behave features/mongodb/ --tags=@smoke

# Run MongoDB CRUD operations
behave features/mongodb/ --tags=@crud

# Run all MongoDB tests
behave --tags=@mongodb
```

### **3. API Testing**

#### **API Connectivity:**
```bash
# Test API connection
python scripts/run.py test-api

# Test specific endpoint
python scripts/run.py api --endpoint /customers --method GET --expected-status 200
```

#### **API BDD Tests:**
```bash
# Run API smoke tests
behave features/api/ --tags=@smoke

# Run API regression tests
behave features/api/ --tags=@regression
```

### **4. MQ Testing**

#### **MQ Operations:**
```bash
# Test MQ connection
python scripts/run.py test-mq

# Post file as single message
python scripts/run.py mq --file test_message.txt --mode single

# Post file line by line
python scripts/run.py mq --file test_data.txt --mode line
```

#### **MQ BDD Tests:**
```bash
# Run MQ smoke tests
behave features/mq/ --tags=@smoke

# Run all MQ tests
behave features/mq/
```

### **5. Comprehensive Test Suites**

#### **Daily Testing:**
```bash
# Quick smoke test across all components
behave --tags="@smoke"

# Critical functionality only
behave --tags="@critical"
```

#### **Weekly Regression:**
```bash
# Full regression test suite
behave --tags="@regression"

# Regression for databases only
behave --tags="@regression and (@database or @mongodb)"
```

#### **Environment-Specific Testing:**
```bash
# Test DEV environment
behave --tags="@dev"

# QA environment validation
behave --tags="@qa and (@smoke or @regression)"

# Production readiness check
behave --tags="@prod and @critical"
```

#### **Component Integration Testing:**
```bash
# Database integration tests
behave --tags="@integration and (@database or @mongodb)"

# API and database integration
behave --tags="@integration and (@api or @database)"

# End-to-end workflows
behave --tags="@e2e"
```

## 🚀 Advanced Usage

### **Custom Test Execution:**

#### **Performance Testing:**
```bash
# Fast tests only (< 1 minute)
behave --tags="@fast"

# Exclude slow tests
behave --tags="not @slow"
```

#### **Data Operation Testing:**
```bash
# All CRUD operations
behave --tags="@crud"

# MongoDB CRUD only
behave --tags="@mongodb and @crud"

# Database comparisons
behave --tags="@database and @comparison"
```

#### **Environment Migration Testing:**
```bash
# Test data sync between environments
behave --tags="@migration"

# Cross-environment validation
behave --tags="(@dev or @qa) and @validation"
```

### **Report Generation:**

#### **HTML Reports:**
```bash
# Generate HTML report
behave --format=html --outfile=output/test-report.html

# Generate with specific tags
behave --tags=@smoke --format=html --outfile=output/smoke-report.html
```

#### **JUnit XML (for CI/CD):**
```bash
# Generate JUnit XML for Jenkins
behave --junit --junit-directory output/junit

# Tagged execution with reports
behave --tags=@regression --junit --junit-directory output/junit
```

#### **Combined Reporting:**
```bash
# Multiple report formats
behave --tags=@smoke --junit --junit-directory output/junit --format=html --outfile=output/smoke-report.html
```

### **Data Export Testing:**
```bash
# Test CSV export functionality
python scripts/run.py mongodb-query --env DEV --collection products --export-format csv

# Test Excel export with CLOB handling
python scripts/run.py compare --source-env DEV --target-env QA --export-format excel
```

## 🔧 CLI Commands Reference

### **Database Commands:**
```bash
# SQL Database testing
python scripts/run.py test-db --env DEV --db-type ORACLE
python scripts/run.py compare --source-env DEV --target-env QA

# MongoDB commands
python scripts/run.py test-db --env DEV --db-type MONGODB
python scripts/run.py mongodb-query --env DEV --collection users
python scripts/run.py mongodb-aggregate --env DEV --collection orders --pipeline '[...]'
python scripts/run.py mongodb-stats --env DEV --collection products
```

### **API Commands:**
```bash
python scripts/run.py test-api
python scripts/run.py api --endpoint /customers --method GET
```

### **MQ Commands:**
```bash
python scripts/run.py test-mq
python scripts/run.py mq --file data.txt --mode line
```

## CI/CD Integration

### **Jenkins Pipeline Usage:**
```groovy
stage('Smoke Tests') {
    steps {
        sh 'behave --tags=@smoke --junit --junit-directory output/junit'
    }
}

stage('Database Tests') {
    steps {  
        sh 'behave --tags="@database or @mongodb" --junit --junit-directory output/junit'
    }
}

stage('Full Regression') {
    when {
        branch 'main'
    }
    steps {
        sh 'behave --tags=@regression --junit --junit-directory output/junit'
    }
}
```

## Troubleshooting

### **Common Issues:**

#### **Database Connection Failures:**
```bash
# Test individual connections
python scripts/run.py test-db --env DEV --db-type ORACLE
python scripts/run.py test-db --env DEV --db-type MONGODB

# Check configuration
python -c "from utils.config_loader import config_loader; print(config_loader.get_database_config('DEV', 'MONGODB'))"
```

#### **Import Errors:**
```bash
# Test all imports
python -c "from db.database_connector import db_connector; print('SQL OK')"
python -c "from db.mongodb_connector import mongodb_connector; print('MongoDB OK')"
python -c "from utils.logger import logger; print('Logger OK')"
```

#### **BDD Test Failures:**
```bash
# Run with verbose output
behave --tags=@smoke -v

# Dry run to check syntax
behave --dry-run
```

## Dependencies

The framework requires:
- Python 3.8+
- Oracle/PostgreSQL databases (for SQL testing)
- MongoDB (for NoSQL testing)
- IBM MQ (for message queue testing)
- API endpoints (for REST API testing)

See `requirements.txt` for complete Python package dependencies.

## Contributing

1. Follow PEP 8 style guidelines
2. Add appropriate tags to new scenarios
3. Add tests for new features
4. Update documentation
5. Use meaningful commit messages

## Tag Best Practices

1. **Use Multiple Tags**: Combine component, priority, and speed tags
2. **Consistent Naming**: Use lowercase with underscores for multi-word tags
3. **Meaningful Groups**: Create logical tag groups for different test strategies
4. **Regular Review**: Periodically review and clean up unused tags
5. **Documentation**: Document custom tags and their purposes

## License

This framework is open source and available under the MIT License.