# Makefile for BDD Test Automation Framework

# Variables
PYTHON = python3
BEHAVE = behave
PIP = pip3

# Default target (runs when you just type 'make')
.DEFAULT_GOAL := help

# Help command
help:
	@echo "Available commands:"
	@echo "  make setup      - Install all dependencies"
	@echo "  make test       - Run all tests"
	@echo "  make test-api   - Run only API tests"
	@echo "  make test-db    - Run only database tests"
	@echo "  make test-aws   - Run only AWS tests"
	@echo "  make clean      - Clean up temporary files"
	@echo "  make report     - Generate test report"

# Install dependencies
setup:
	$(PIP) install -r requirements.txt
	@echo "✅ Setup complete!"

# Run all tests
test:
	$(BEHAVE) features/

# Run specific test suites
test-api:
	$(BEHAVE) features/api/

test-db:
	$(BEHAVE) features/database/

test-aws:
	$(BEHAVE) features/aws/

test-mq:
	$(BEHAVE) features/mq/

# Run tests with specific tags
test-smoke:
	$(BEHAVE) features/ --tags=@smoke

test-regression:
	$(BEHAVE) features/ --tags=@regression

# Clean up project
clean:
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +
	rm -rf output/reports/*
	rm -rf logs/*/*.log
	@echo "🧹 Cleanup complete!"

# Generate HTML report
report:
	$(BEHAVE) features/ -f html -o output/reports/test_report.html

# Run tests in different environments
test-dev:
	$(BEHAVE) features/ -D env=dev

test-qa:
	$(BEHAVE) features/ -D env=qa

test-staging:
	$(BEHAVE) features/ -D env=staging

# Check code quality (if you have linters installed)
lint:
	pylint api/ aws/ db/ mq/ utils/

# Run tests with coverage
test-coverage:
	coverage run -m behave features/
	coverage report
	coverage html -d output/reports/coverage

# Show test statistics
stats:
	@echo "📊 Test Statistics:"
	@echo "Total features: $$(find features -name "*.feature" | wc -l)"
	@echo "Total scenarios: $$(grep -r "Scenario:" features | wc -l)"
	@echo "Total step files: $$(find features/steps -name "*.py" | wc -l)"

# One command to rule them all (clean, setup, and test)
all: clean setup test report
	@echo "🎉 All tasks completed!"

# Mark targets as not representing files
.PHONY: help setup test test-api test-db test-aws test-mq clean report test-dev test-qa test-staging lint test-coverage stats all test-smoke test-regression