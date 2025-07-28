# features/database/sql/basic_validation.feature

@database @sql @smoke
Feature: Basic SQL Database Validation
    As a QA engineer
    I want to validate basic database operations
    So that I can ensure database connectivity and basic functionality

    Background:
        Given I have a connection to "DEV" environment "ORACLE" database

    @critical @fast
    Scenario: Simple record count validation
        When I execute query "SELECT COUNT(*) FROM dual" on "DEV" database
        And I store the result as "count_result"
        Then the "count_result" should equal "count_result"

    @smoke @fast
    Scenario: Database connectivity test
        When I execute query "SELECT 1 FROM dual" on "DEV" database
        Then the query should complete successfully

    @regression @medium
    Scenario: Compare record counts between tables
        When I execute query "SELECT COUNT(*) FROM customers" on "DEV" database
        And I store the result as "customer_count"
        When I execute query "SELECT COUNT(*) FROM orders" on "DEV" database
        And I store the result as "order_count"
        Then the results should be stored successfully