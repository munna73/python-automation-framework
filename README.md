# Python Automation Framework with Behave BDD

A comprehensive test automation framework supporting SQL databases, NoSQL MongoDB, REST APIs, AWS services, and message queuing with BDD capabilities using Behave.

## 🚀 Features

- 🗄️ **Multi-Database Support**: Oracle, PostgreSQL, and MongoDB with cross-database validation
- 🌐 **REST API Testing**: Comprehensive API testing with schema validation
- ☁️ **AWS Integration**: S3, SQS, and AWS-SQL integration testing
- 📨 **Message Queue**: Support for multiple MQ systems (RabbitMQ, IBM MQ)
- 🥒 **BDD Framework**: Behave-based testing with business-readable scenarios
- 📊 **Advanced Reporting**: HTML, JUnit XML, and Allure reporting
- 🔄 **CI/CD Ready**: Jenkins pipeline with Docker support
- 📝 **Centralized Logging**: Component-based logging system
- 🏷️ **Tag-Based Execution**: Flexible test execution strategies

## 📋 Table of Contents

- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running Tests](#running-tests)
- [Tag-Based Testing](#tag-based-testing)
- [Component Testing](#component-testing)
- [CI/CD Integration](#cicd-integration)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Quick Start

```bash
# Clone the repository
git clone <repository-url>
cd automation-framework

# Setup virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Setup configuration
cp .env.template .env
# Edit .env with your credentials

# Run all tests
behave

# Run smoke tests only
behave --tags=@smoke
```

## Project Structure

```
automation-framework/
├── api/                # API client implementations
│   ├── rest_client.py  # REST API client with retry logic
│   └── json_validator.py # JSON schema validation
├── aws/                # AWS service connectors
│   ├── s3_connector.py # S3 operations
│   ├── sqs_connector.py # SQS messaging
│   └── sql_integration.py # AWS-SQL integration
├── config/             # Configuration files
│   ├── config.ini      # Database/service configurations
│   ├── database_config.py # Database configuration loader
│   └── environments.yaml # Environment-specific settings
├── data/               # Test data
│   ├── input/          # Input test files
│   └── schemas/        # JSON validation schemas
├── db/                 # Database connectors
│   ├── base_connector.py # Base database interface
│   ├── database_connector.py # SQL database operations
│   └── mongodb_connector.py # MongoDB operations
├── features/           # BDD feature files
│   ├── api/           # API test scenarios
│   ├── aws/           # AWS service scenarios
│   ├── database/      # Database test scenarios
│   │   ├── sql/       # SQL-specific tests
│   │   ├── nosql/     # MongoDB tests
│   │   └── cross_database/ # Cross-DB validation
│   ├── mq/            # Message queue scenarios
│   └── steps/         # Step definitions
├── logs/              # Application logs
├── mq/                # Message queue implementations
├── output/            # Test reports and exports
├── scripts/           # Utility scripts
├── tests/             # Unit tests
└── utils/             # Common utilities
```

## Installation

### Prerequisites

- Python 3.8 or higher
- Database access (Oracle/PostgreSQL/MongoDB)
- AWS account (for AWS testing)
- Message Queue server (RabbitMQ/IBM MQ)

### Setup Steps

1. **Install Python dependencies:**
```bash
pip install -r requirements.txt
```

2. **Install additional tools (optional):**
```bash
# For code quality
pip install pylint black flake8

# For performance testing
pip install locust memory-profiler
```

3. **Setup pre-commit hooks:**
```bash
pip install pre-commit
pre-commit install
```

## Configuration

### Environment Variables (.env)

```bash
# Database Credentials
DEV_ORACLE_PWD=your_oracle_password
DEV_POSTGRES_PWD=your_postgres_password
DEV_MONGODB_PWD=your_mongodb_password

# AWS Credentials
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# API Configuration
API_BASE_URL=https://api.example.com
API_TOKEN=your_api_token

# Message Queue
MQ_HOST=localhost
MQ_PORT=5672
MQ_USERNAME=guest
MQ_PASSWORD=guest
```

### Database Configuration (config/config.ini)

```ini
[DEV_ORACLE]
host = oracle-dev.example.com
port = 1521
service_name = ORCL
username = dev_user

[DEV_POSTGRES]
host = postgres-dev.example.com
port = 5432
database = dev_db
username = dev_user

[DEV_MONGODB]
host = mongodb-dev.example.com
port = 27017
database = dev_db
username = dev_user
auth_source = admin
```

## Running Tests

### Using Behave

```bash
# Run all tests
behave

# Run with specific format
behave -f pretty
behave -f json -o output/results.json

# Run specific feature
behave features/api/rest_api.feature

# Dry run (syntax check)
behave --dry-run
```

### Using Makefile

```bash
# Setup environment
make setup

# Run all tests
make test

# Run specific components
make test-api
make test-db
make test-aws

# Clean up
make clean

# Generate reports
make report
```

### Using pytest (for unit tests)

```bash
# Run unit tests
pytest tests/

# With coverage
pytest --cov=utils --cov=db --cov=api tests/
```

## Tag-Based Testing

### Available Tags

#### Test Types
- `@smoke` - Quick validation tests
- `@regression` - Comprehensive test suite
- `@integration` - Integration tests
- `@e2e` - End-to-end workflows
- `@performance` - Performance tests

#### Components
- `@api` - REST API tests
- `@database` - SQL database tests
- `@mongodb` - MongoDB tests
- `@aws` - AWS service tests
- `@mq` - Message queue tests

#### Environments
- `@dev` - Development environment
- `@qa` - QA environment
- `@staging` - Staging environment
- `@prod` - Production environment

#### Operations
- `@crud` - Create, Read, Update, Delete
- `@query` - Query operations
- `@validation` - Data validation
- `@auth` - Authentication tests

### Tag Usage Examples

```bash
# Single tag
behave --tags=@smoke

# AND logic
behave --tags="@api and @smoke"

# OR logic
behave --tags="@database or @mongodb"

# NOT logic
behave --tags="not @slow"

# Complex combinations
behave --tags="(@smoke or @regression) and @api and not @prod"
```

## Component Testing

### API Testing

```bash
# Test REST API endpoints
behave features/api/ --tags=@api

# Test specific endpoint
python scripts/run.py test-api --endpoint /customers --method GET

# API performance test
behave features/api/ --tags="@api and @performance"
```

### Database Testing

```bash
# SQL Database tests
behave features/database/sql/ --tags=@database

# MongoDB tests
behave features/database/nosql/ --tags=@mongodb

# Cross-database validation
behave features/database/cross_database/ --tags=@validation
```

### AWS Testing

```bash
# S3 operations
behave features/aws/ --tags="@aws and @s3"

# SQS messaging
behave features/aws/ --tags="@aws and @sqs"

# Full AWS integration
behave features/aws/ --tags=@aws
```

### Message Queue Testing

```bash
# MQ connectivity
behave features/mq/ --tags="@mq and @smoke"

# Message processing
behave features/mq/ --tags="@mq and @integration"
```

## Advanced Features

### Data Comparison

```bash
# Compare data between environments
python scripts/run.py compare \
  --source-env DEV \
  --target-env QA \
  --table customers

# MongoDB comparison
python scripts/run.py compare-mongo \
  --source-env DEV \
  --target-env QA \
  --collection users
```

### Parallel Execution

```bash
# Run tests in parallel (requires pytest-xdist)
pytest -n 4 tests/

# Parallel behave execution
behave --parallel 2 --parallel-element scenario
```

### Custom Reports

```bash
# Generate Allure report
behave -f allure_behave.formatter:AllureFormatter -o output/allure-results
allure generate output/allure-results -o output/allure-report

# Generate HTML report
behave -f html -o output/report.html
```

## CI/CD Integration

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    
    stages {
        stage('Setup') {
            steps {
                sh 'python -m venv venv'
                sh '. venv/bin/activate && pip install -r requirements.txt'
            }
        }
        
        stage('Lint') {
            steps {
                sh '. venv/bin/activate && pylint api/ db/ utils/'
            }
        }
        
        stage('Unit Tests') {
            steps {
                sh '. venv/bin/activate && pytest tests/ --junit-xml=output/junit/unit-tests.xml'
            }
        }
        
        stage('Smoke Tests') {
            steps {
                sh '. venv/bin/activate && behave --tags=@smoke --junit --junit-directory=output/junit'
            }
        }
        
        stage('Regression Tests') {
            when {
                branch 'main'
            }
            steps {
                sh '. venv/bin/activate && behave --tags=@regression --junit --junit-directory=output/junit'
            }
        }
    }
    
    post {
        always {
            junit 'output/junit/*.xml'
            publishHTML([
                reportDir: 'output/reports',
                reportFiles: '*.html',
                reportName: 'BDD Test Report'
            ])
        }
    }
}
```

### Docker Support

```dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run tests
CMD ["behave", "--tags=@smoke"]
```

## Troubleshooting

### Common Issues

#### Database Connection Issues
```bash
# Test connectivity
python -c "from db.database_connector import db_connector; print(db_connector.test_connection('DEV', 'ORACLE'))"

# Check configuration
python -c "from utils.config_loader import config_loader; print(config_loader.get_database_config('DEV', 'ORACLE'))"
```

#### Import Errors
```bash
# Verify Python path
python -c "import sys; print('\n'.join(sys.path))"

# Test imports
python -c "import api.rest_client; print('API OK')"
python -c "import db.mongodb_connector; print('MongoDB OK')"
```

#### AWS Authentication
```bash
# Check AWS credentials
aws sts get-caller-identity

# Test AWS connection
python -c "from aws.s3_connector import s3_connector; print(s3_connector.test_connection())"
```

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
behave --tags=@smoke

# Verbose output
behave -v --tags=@api

# Stop on first failure
behave -x --tags=@regression
```

## Performance Optimization

### Test Execution Speed

```bash
# Run only fast tests
behave --tags="@fast"

# Skip slow tests
behave --tags="not @slow"

# Parallel execution
behave --parallel 2 --parallel-element feature
```

### Database Query Optimization

```python
# Use connection pooling
db_connector.enable_pooling(min_size=2, max_size=10)

# Batch operations
db_connector.batch_insert(data, batch_size=1000)
```

## Best Practices

1. **Tag Strategy**
   - Use multiple tags for flexibility
   - Keep tags consistent and documented
   - Review and clean up unused tags regularly

2. **Test Data Management**
   - Use fixtures for test data setup/teardown
   - Keep test data in version control
   - Use data factories for dynamic data

3. **Code Quality**
   - Run linters before committing
   - Maintain test coverage above 80%
   - Follow PEP 8 style guide

4. **Performance**
   - Use connection pooling
   - Implement proper cleanup in teardown
   - Optimize database queries

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Coding Standards

- Follow PEP 8
- Write descriptive commit messages
- Add tests for new features
- Update documentation
- Add appropriate tags to scenarios

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the repository
- Check the [Wiki](wiki-link) for detailed documentation
- Contact the team at automation-team@example.com

## Acknowledgments

- Behave BDD Framework
- AWS SDK for Python (Boto3)
- MongoDB Python Driver
- PostgreSQL psycopg2
- Oracle cx_Oracle