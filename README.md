Python Automation Framework with Behave BDD
A comprehensive test automation framework supporting SQL databases, NoSQL MongoDB, REST APIs, AWS services, and message queuing with BDD capabilities using Behave.

📋 Table of Contents
Quick Start

Project Structure

Git Ignore

Features

Prerequisites

Installation

Configuration

Running Tests

Tag-Based Testing

Component Testing

CI/CD Integration

Troubleshooting

Contributing

License

🚀 Quick Start
Get up and running with the framework in a few simple steps.

# Clone the repository
git clone <repository-url>
cd automation-framework

# Setup virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies from the requirements.txt file
pip install -r requirements.txt

# Setup configuration
cp .env.template .env
# Edit .env with your credentials

# Run all tests
behave

# Run smoke tests only
behave --tags=@smoke

tree -I '__pycache__|venv|*.pyc|*.log' -N --dirsfirst -L 5 > project_structure.txt 

🗂️.
.
├── api
│   ├── __init__.py
│   ├── json_validator.py
│   └── rest_client.py
├── aws
│   ├── __init__.py
│   ├── s3_connector.py
│   ├── sql_integration.py
│   └── sqs_connector.py
├── config
│   ├── config.ini
│   └── environments.yaml
├── data
│   ├── input
│   │   ├── bulk_test_data.json
│   │   ├── customer.json
│   │   ├── multi_line_test.txt
│   │   ├── test_data.txt
│   │   └── test_message.txt
│   └── schemas
│       ├── api_schema.json
│       └── customer_schema.json
├── db
│   ├── __init__.py
│   ├── base_connector.py
│   ├── database_connector.py
│   ├── database_manager.py
│   └── mongodb_connector.py
├── features
│   ├── api
│   │   └── rest_api.feature
│   ├── aws
│   │   └── aws_integration.feature
│   ├── database
│   │   ├── cross_database
│   │   │   └── data_sync_validation.feature
│   │   ├── nosql
│   │   │   ├── mongodb_connection.feature
│   │   │   ├── mongodb_crud.feature
│   │   │   └── mongodb_operations.feature
│   │   ├── sql
│   │   │   ├── basic_validation.feature
│   │   │   ├── config_basedquery.feature
│   │   │   └── data_validation.feature
│   │   └── data_comparison.feature
│   ├── kafka
│   │   └── kafka_integration.feature
│   ├── mq
│   │   └── message_queue.feature
│   ├── steps
│   │   ├── database
│   │   │   ├── base_database_steps.py
│   │   │   ├── cross_database_steps.py
│   │   │   ├── enhanced_data_compare_steps.py.py
│   │   │   ├── mongodb_steps.py
│   │   │   ├── query_database_steps.py
│   │   │   └── sql_database_steps.py
│   │   ├── __init__.py
│   │   ├── api_steps.py
│   │   ├── aws_steps.py
│   │   ├── database_steps.py
│   │   ├── kafka_steps.py
│   │   └── mq_steps.py
│   └── environment.py
├── logs
│   ├── api
│   ├── application
│   ├── database
│   ├── mq
│   └── test_execution
├── mq
│   ├── __init__.py
│   ├── kafka_connector.py
│   └── mq_producer.py
├── output
│   ├── exports
│   ├── junit
│   └── reports
├── path
│   └── to
├── scripts
│   ├── __init__.py
│   └── run.py
├── tests
│   ├── unit
│   │   ├── test_data_comparator.py
│   │   ├── test_data_validator.py
│   │   ├── test_export_utils.py
│   │   ├── test_json_validator.py
│   │   └── test_query_loader.py
│   └── __init__.py
├── utils
│   ├── __init__.py
│   ├── config_loader.py
│   ├── custom_exceptions.py
│   ├── data_cleaner.py
│   ├── data_comparator.py
│   ├── data_loader.py
│   ├── data_validator.py
│   ├── export_utils.py
│   ├── logger.py
│   └── query_loader.py
├── Dockerfile
├── Jenkinsfile
├── README.md
├── behave.ini
├── generate_requirements.py
├── project_structure.txt
├── requirements.txt
└── tox.ini

36 directories, 75 files



📝 Git Ignore (.gitignore)
To keep the repository clean and avoid committing unnecessary files, the project uses the following .gitignore configuration.

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
env/
ENV/

# Environment variables
.env

# Logs
logs/**/*.log

# Output files
output/**/*
!output/.gitkeep

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Pytest
.pytest_cache/
.coverage
htmlcov/

# Behave
reports/

setup_framework.py
generate_implementation.py
MY_NEXT_STEPS.md
mynotes.txt

🌟 Features
Multi-Database Support: Oracle, PostgreSQL, and MongoDB with cross-database validation.

REST API Testing: Comprehensive API testing with schema validation.

AWS Integration: S3, SQS, and AWS-SQL integration testing.

Message Queue: Support for multiple MQ systems (RabbitMQ, IBM MQ).

BDD Framework: Behave-based testing with business-readable scenarios.

Advanced Reporting: HTML, JUnit XML, and Allure reporting.

CI/CD Ready: Jenkins pipeline with Docker support.

Centralized Logging: Component-based logging system.

Tag-Based Execution: Flexible test execution strategies.

⚙️ Prerequisites
Before you begin, ensure you have the following installed:

Python 3.8 or higher

Database access (Oracle/PostgreSQL/MongoDB)

AWS account (for AWS testing)

Message Queue server (RabbitMQ/IBM MQ)

📦 Installation
Clone the repository.

Set up a virtual environment.

Install Python dependencies using the requirements.txt file in this project. The required packages are:
pip install behave jsonschema pandas requests boto3 SQLAlchemy pymongo kafka-python PyYAML
behave
jsonschema
pandas
requests
pymqi
boto3
SQLAlchemy
pymongo
PyYAML


Optionally, install additional tools for code quality and performance testing.

# For code quality
pip install pylint black flake8
# For performance testing
pip install locust memory-profiler


Set up pre-commit hooks.

pip install pre-commit
pre-commit install


🛠️ Configuration
The framework uses environment variables and .ini files for configuration.

Environment Variables (.env)
Copy the .env.template file to .env and fill in your credentials.

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


Database Configuration (config/config.ini)
This file holds specific connection details for each database environment.

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


▶️ Running Tests
Using Behave

# Run all tests
behave

# Run with specific format
behave -f pretty
behave -f json -o output/results.json

# Run specific feature
behave features/api/rest_api.feature

# Dry run (syntax check)
behave --dry-run


Using Makefile

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


Using pytest (for unit tests)

# Run unit tests
pytest tests/

# With coverage
pytest --cov=utils --cov=db --cov=api tests/


🏷️ Tag-Based Testing
Available Tags

Test Types: @smoke, @regression, @integration, @e2e, @performance

Components: @api, @database, @mongodb, @aws, @mq

Environments: @dev, @qa, @staging, @prod

Operations: @crud, @query, @validation, @auth

Tag Usage Examples

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


🧩 Component Testing
API Testing

# Test REST API endpoints
behave features/api/ --tags=@api

# Test specific endpoint
python scripts/run.py test-api --endpoint /customers --method GET


Database Testing

# SQL Database tests
behave features/database/sql/ --tags=@database

# MongoDB tests
behave features/database/nosql/ --tags=@mongodb


AWS Testing

# S3 operations
behave features/aws/ --tags="@aws and @s3"


Message Queue Testing

# MQ connectivity
behave features/mq/ --tags="@mq and @smoke"


🔄 CI/CD Integration
Jenkins Pipeline

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


Docker Support

FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Run tests
CMD ["behave", "--tags=@smoke"]


⚠️ Troubleshooting
Common Issues

Database Connection Issues: Use the provided commands to test connectivity and check your configuration.

Import Errors: Verify your Python path.

AWS Authentication: Use the aws sts get-caller-identity command to confirm your credentials.

🤝 Contributing
For information on how to contribute to this project, including coding standards, commit messages, and the pull request process, please refer to the provided details.

📜 License
This project is licensed under the MIT License - see the LICENSE file for details.

📞 Support
For issues and questions, please:

Create an issue in the repository.

Check the Wiki for detailed documentation.

Contact the team at automation-team@example.com.